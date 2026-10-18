#include "common/network.hpp"
#include "common/protocol.hpp"
#include "src/peer/dht_storage.hpp"
#include "src/peer/ring_state.hpp"
#include "src/peer/storm_record.hpp"

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

using dht::peer::DhtStorage;
using dht::peer::PeerAddress;
using dht::peer::StormRecord;

constexpr std::size_t kManagerBufferSize = 1024;
constexpr std::size_t kPeerBufferSize = 65536;

struct DhtContext {
  dht::peer::RingState ring;
  DhtStorage storage;

  // The leader uses these fields to decide when setup is actually complete.
  std::size_t total_records = 0;
  std::unordered_set<int> expected_event_ids;
  std::unordered_set<int> acked_event_ids;
  std::vector<std::size_t> records_per_peer;
  bool setup_active = false;
  bool completion_sent = false;
};

std::string build_store_message(int destination_id, int position,
                                int hash_table_size,
                                const StormRecord &record) {
  std::vector<std::string> fields{
      "store", std::to_string(destination_id), std::to_string(position),
      std::to_string(hash_table_size)};
  fields.insert(fields.end(), record.fields.begin(), record.fields.end());
  return dht::protocol::join_message(fields);
}

bool send_peer_message(int socket_fd, const PeerAddress &peer,
                       const std::string &message) {
  return dht::network::send_udp_message(
      socket_fd, {peer.ip_address, peer.peer_port}, message);
}

bool send_to_right_neighbor(int socket_fd, const DhtContext &dht_state,
                            const std::string &message) {
  const PeerAddress *right_neighbor = dht_state.ring.right_neighbor();
  return right_neighbor &&
         send_peer_message(socket_fd, *right_neighbor, message);
}

bool recreate_manager_socket(int &manager_socket) {
  if (manager_socket >= 0)
    close(manager_socket);
  manager_socket = dht::network::create_udp_socket();
  return manager_socket >= 0;
}

bool handle_store_message(const std::string &message, DhtContext &dht_state,
                          int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  constexpr std::size_t record_start = 4;
  if (tokens.size() != record_start + dht::peer::kStormFieldCount ||
      tokens[0] != "store" ||
      !dht_state.ring.configured()) {
    return false;
  }

  std::optional<int> destination_id =
      dht::protocol::parse_integer(tokens[1]);
  std::optional<int> position = dht::protocol::parse_integer(tokens[2]);
  std::optional<int> hash_table_size =
      dht::protocol::parse_integer(tokens[3]);
  std::optional<int> event_id =
      dht::protocol::parse_integer(tokens[record_start]);
  if (!destination_id || !position || !hash_table_size || !event_id ||
      *event_id < 0 || *destination_id < 0 ||
      *destination_id >= dht_state.ring.size() || *position < 0 ||
      *hash_table_size <= 0) {
    return false;
  }

  if (!dht_state.storage.initialized()) {
    if (!dht_state.storage.initialize(*hash_table_size))
      return false;
  } else if (dht_state.storage.table_size() != *hash_table_size) {
    return false;
  }

  std::optional<int> expected_position =
      dht_state.storage.position_for(*event_id);
  std::optional<int> expected_owner =
      dht_state.storage.owner_for(*event_id, dht_state.ring.size());
  if (!expected_position || !expected_owner ||
      *position != *expected_position || *destination_id != *expected_owner) {
    return false;
  }

  StormRecord record;
  record.fields.assign(tokens.begin() + record_start, tokens.end());
  record.event_id = *event_id;

  if (*destination_id == dht_state.ring.local_id()) {
    if (dht_state.ring.local_id() == 0 &&
        dht_state.expected_event_ids.find(record.event_id) ==
            dht_state.expected_event_ids.end()) {
      return false;
    }

    DhtStorage::StoreResult result =
        dht_state.storage.store(*position, record);
    if (result == DhtStorage::StoreResult::InvalidPosition)
      return false;

    if (dht_state.ring.local_id() == 0) {
      if (dht_state.acked_event_ids.insert(record.event_id).second) {
        if (dht_state.records_per_peer.empty())
          return false;
        ++dht_state.records_per_peer[0];
      }
    } else {
      std::vector<std::string> acknowledgement_fields{
          "store-ack", std::to_string(*destination_id),
          std::to_string(record.event_id), std::to_string(*position)};
      if (!send_to_right_neighbor(
              peer_socket, dht_state,
              dht::protocol::join_message(acknowledgement_fields))) {
        return false;
      }
    }

    if (result == DhtStorage::StoreResult::Inserted) {
      std::cout << "Stored event " << record.event_id << " at position "
                << *position << " on peer id " << dht_state.ring.local_id()
                << ".\n";
    }
    return true;
  }

  if (!send_to_right_neighbor(peer_socket, dht_state, message)) {
    std::cerr << "Failed to forward store for event " << record.event_id
              << ".\n";
    return false;
  }

  const PeerAddress *right_neighbor = dht_state.ring.right_neighbor();
  std::cout << "Forwarded store for event " << record.event_id
            << " from peer " << dht_state.ring.local_id() << " to "
            << right_neighbor->name << ".\n";
  return true;
}

bool handle_store_ack(const std::string &message, DhtContext &dht_state,
                      int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() != 4 || tokens[0] != "store-ack" ||
      !dht_state.ring.configured()) {
    return false;
  }

  std::optional<int> destination_id =
      dht::protocol::parse_integer(tokens[1]);
  std::optional<int> event_id = dht::protocol::parse_integer(tokens[2]);
  std::optional<int> position = dht::protocol::parse_integer(tokens[3]);
  if (!destination_id || !event_id || !position || *event_id < 0 ||
      *position < 0 || *destination_id < 0 ||
      *destination_id >= dht_state.ring.size()) {
    return false;
  }

  if (dht_state.ring.local_id() != 0)
    return send_to_right_neighbor(peer_socket, dht_state, message);

  std::optional<int> expected_position =
      dht_state.storage.position_for(*event_id);
  std::optional<int> expected_owner =
      dht_state.storage.owner_for(*event_id, dht_state.ring.size());
  if (!expected_position || !expected_owner ||
      *position != *expected_position || *destination_id != *expected_owner ||
      dht_state.expected_event_ids.find(*event_id) ==
          dht_state.expected_event_ids.end() ||
      dht_state.records_per_peer.size() !=
          static_cast<std::size_t>(dht_state.ring.size())) {
    return false;
  }

  if (dht_state.acked_event_ids.insert(*event_id).second)
    ++dht_state.records_per_peer[*destination_id];
  return true;
}

bool distribute_records(const std::string &year, DhtContext &dht_state,
                        int peer_socket) {
  std::vector<StormRecord> records;
  std::string path = "details-" + year + ".csv";
  if (!dht::peer::load_storm_records(path, records)) {
    std::cerr << "Unable to load dataset " << path
              << " (expected 14 valid fields per record).\n";
    return false;
  }

  std::unordered_set<int> event_ids;
  for (const StormRecord &record : records) {
    if (!event_ids.insert(record.event_id).second) {
      std::cerr << "Dataset contains duplicate event id " << record.event_id
                << ".\n";
      return false;
    }
  }

  if (!dht_state.storage.initialize_for_record_count(records.size())) {
    std::cerr << "Dataset is too large to construct the local hash table.\n";
    return false;
  }

  dht_state.total_records = records.size();
  dht_state.expected_event_ids = std::move(event_ids);
  dht_state.acked_event_ids.clear();
  dht_state.records_per_peer.assign(dht_state.ring.size(), 0);
  dht_state.setup_active = true;
  dht_state.completion_sent = false;

  std::cout << "Loaded " << records.size() << " records from " << path
            << "; local hash table size is "
            << dht_state.storage.table_size() << ".\n";

  for (const StormRecord &record : records) {
    std::optional<int> position =
        dht_state.storage.position_for(record.event_id);
    std::optional<int> destination_id =
        dht_state.storage.owner_for(record.event_id, dht_state.ring.size());
    if (!position || !destination_id)
      return false;

    std::string message = build_store_message(
        *destination_id, *position, dht_state.storage.table_size(), record);

    if (*destination_id == dht_state.ring.local_id()) {
      if (!handle_store_message(message, dht_state, peer_socket))
        return false;
    } else if (!send_to_right_neighbor(peer_socket, dht_state, message)) {
      std::cerr << "Failed to send store for event " << record.event_id
                << " to the right neighbor.\n";
      return false;
    }
  }
  return true;
}

bool parse_ring_members(const std::vector<std::string> &tokens,
                        std::size_t first_member, int ring_size,
                        std::vector<PeerAddress> &members) {
  if (ring_size < 1 ||
      tokens.size() != first_member + static_cast<std::size_t>(3 * ring_size)) {
    return false;
  }

  std::vector<PeerAddress> parsed_members;
  parsed_members.reserve(ring_size);
  for (int peer_id = 0; peer_id < ring_size; ++peer_id) {
    std::size_t base_index = first_member + (peer_id * 3);
    std::optional<int> peer_port =
        dht::protocol::parse_integer(tokens[base_index + 2]);
    if (!peer_port)
      return false;
    parsed_members.push_back(
        {tokens[base_index], tokens[base_index + 1], *peer_port});
  }

  members = std::move(parsed_members);
  return true;
}

void handle_manager_response(const std::string &request_message,
                             const std::string &response_message,
                             int peer_socket, DhtContext &dht_state) {
  std::vector<std::string> request_tokens =
      dht::protocol::split_message(request_message);
  std::vector<std::string> response_tokens =
      dht::protocol::split_message(response_message);

  if (request_tokens.empty() || response_tokens.empty())
    return;

  if (request_tokens[0] != "setup-dht" || response_tokens[0] != "SUCCESS")
    return;

  if (request_tokens.size() != 4) {
    std::cerr << "Invalid setup-dht request state.\n";
    return;
  }

  std::optional<int> ring_size =
      dht::protocol::parse_integer(request_tokens[2]);
  if (!ring_size || *ring_size < 3) {
    std::cerr << "Invalid ring size in setup-dht request.\n";
    return;
  }

  std::vector<PeerAddress> members;
  if (!parse_ring_members(response_tokens, 1, *ring_size, members) ||
      members[0].name != request_tokens[1] ||
      !dht_state.ring.configure(0, std::move(members))) {
    std::cerr << "Invalid setup-dht response: expected " << *ring_size
              << " valid peer tuples beginning with the leader.\n";
    return;
  }

  dht_state.storage.clear();
  std::cout << "Assigned Leader status. Building logical ring of size "
            << *ring_size << "...\n";

  for (int peer_id = 1; peer_id < *ring_size; ++peer_id) {
    std::vector<std::string> set_id_fields{"set-id", std::to_string(peer_id),
                                           std::to_string(*ring_size)};
    set_id_fields.insert(set_id_fields.end(), response_tokens.begin() + 1,
                         response_tokens.end());

    const PeerAddress *target = dht_state.ring.peer_by_id(peer_id);
    if (!target || !send_peer_message(
                       peer_socket, *target,
                       dht::protocol::join_message(set_id_fields))) {
      std::cerr << "Failed to send set-id data to peer id " << peer_id
                << ".\n";
      return;
    }

    std::cout << "Sent set-id data to " << target->name << " at "
              << target->ip_address << ":" << target->peer_port << "\n";
  }

  if (!distribute_records(request_tokens[3], dht_state, peer_socket)) {
    std::cerr << "DHT setup stopped before record distribution completed.\n";
    return;
  }
  std::cout << "Leader stored " << dht_state.storage.record_count()
            << " records locally.\n";
}

bool handle_set_id(const std::string &message, DhtContext &dht_state) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() < 3 || tokens[0] != "set-id")
    return false;

  std::optional<int> peer_id = dht::protocol::parse_integer(tokens[1]);
  std::optional<int> ring_size = dht::protocol::parse_integer(tokens[2]);
  if (!peer_id || !ring_size || *ring_size < 3 || *peer_id < 1 ||
      *peer_id >= *ring_size) {
    return false;
  }

  std::vector<PeerAddress> members;
  if (!parse_ring_members(tokens, 3, *ring_size, members) ||
      !dht_state.ring.configure(*peer_id, std::move(members))) {
    return false;
  }

  dht_state.storage.clear();
  const PeerAddress *right_neighbor = dht_state.ring.right_neighbor();

  std::cout << "Configured DHT id=" << dht_state.ring.local_id()
            << ", ring size=" << dht_state.ring.size()
            << ", right neighbor=" << right_neighbor->name << " at "
            << right_neighbor->ip_address << ":" << right_neighbor->peer_port
            << "\n";
  return true;
}

void maybe_report_setup_complete(int manager_socket,
                                 const dht::network::UdpEndpoint &manager,
                                 DhtContext &dht_state) {
  if (!dht_state.ring.configured() || dht_state.ring.local_id() != 0 ||
      !dht_state.setup_active || dht_state.completion_sent ||
      dht_state.acked_event_ids.size() != dht_state.total_records) {
    return;
  }

  const PeerAddress *local_peer = dht_state.ring.local_peer();
  if (!local_peer)
    return;

  std::string completion_message = "dht-complete|" + local_peer->name;
  if (!dht::network::send_udp_message(manager_socket, manager,
                                      completion_message)) {
    std::cerr << "Unable to send dht-complete to the manager.\n";
    return;
  }

  char completion_buffer[kManagerBufferSize];
  sockaddr_in completion_address;
  socklen_t completion_length = sizeof(completion_address);
  ssize_t completion_bytes =
      recvfrom(manager_socket, completion_buffer, sizeof(completion_buffer) - 1,
               MSG_WAITALL,
               reinterpret_cast<sockaddr *>(&completion_address),
               &completion_length);
  if (completion_bytes <= 0) {
    std::cerr << "Unable to receive the dht-complete response.\n";
    return;
  }

  completion_buffer[completion_bytes] = '\0';
  std::string completion_response(completion_buffer);
  std::cout << "Manager replied: " << completion_response << "\n";
  if (completion_response != "SUCCESS")
    return;

  dht_state.completion_sent = true;
  dht_state.setup_active = false;
  for (std::size_t peer_id = 0; peer_id < dht_state.records_per_peer.size();
       ++peer_id) {
    std::cout << "Peer id " << peer_id << " stored "
              << dht_state.records_per_peer[peer_id] << " records.\n";
  }
}

} // namespace

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0]
              << " <manager_ipv4> <manager_port>\n";
    return EXIT_FAILURE;
  }

  std::string manager_ip = argv[1];
  std::optional<int> manager_port = dht::protocol::parse_integer(argv[2]);
  if (!dht::network::is_valid_ipv4(manager_ip) || !manager_port ||
      !dht::network::is_valid_port(*manager_port)) {
    std::cerr << "Error: Invalid manager IPv4 address or port.\n";
    return EXIT_FAILURE;
  }

  int manager_socket = dht::network::create_udp_socket();
  if (manager_socket < 0) {
    std::cerr << "Error: Unable to create manager socket.\n";
    return EXIT_FAILURE;
  }
  dht::network::UdpEndpoint manager_endpoint{manager_ip, *manager_port};

  int peer_socket = -1;
  bool manager_socket_bound = false;
  DhtContext dht_state;

  std::cout << "Peer started. Manager at " << manager_ip << ":"
            << *manager_port << "\n";
  std::cout
      << "Enter commands (e.g., register <name> <ip> <m-port> <p-port>)\n> ";

  while (true) {
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(STDIN_FILENO, &read_fds);
    int max_fd = STDIN_FILENO;

    if (peer_socket != -1) {
      FD_SET(peer_socket, &read_fds);
      if (peer_socket > max_fd)
        max_fd = peer_socket;
    }

    if (select(max_fd + 1, &read_fds, nullptr, nullptr, nullptr) < 0) {
      std::cerr << "Error in select multiplexing.\n";
      break;
    }

    if (FD_ISSET(STDIN_FILENO, &read_fds)) {
      std::string input_line;
      if (!std::getline(std::cin, input_line))
        break;

      if (input_line.empty()) {
        std::cout << "> ";
        continue;
      }

      std::istringstream input(input_line);
      std::vector<std::string> command_fields;
      std::string field;
      while (input >> field)
        command_fields.push_back(field);

      std::string wire_message =
          dht::protocol::join_message(command_fields);
      std::vector<std::string> request_tokens =
          dht::protocol::split_message(wire_message);

      int pending_peer_socket = -1;
      bool pending_registration = false;
      if (!request_tokens.empty() && request_tokens[0] == "register") {
        if (manager_socket_bound || peer_socket != -1) {
          std::cerr << "This peer is already registered.\n> ";
          continue;
        }
        if (request_tokens.size() != 5) {
          std::cerr << "Usage: register <name> <ip> <m-port> <p-port>\n> ";
          continue;
        }

        std::optional<int> local_manager_port =
            dht::protocol::parse_integer(request_tokens[3]);
        std::optional<int> local_peer_port =
            dht::protocol::parse_integer(request_tokens[4]);
        if (!local_manager_port || !local_peer_port ||
            !dht::network::is_valid_port(*local_manager_port) ||
            !dht::network::is_valid_port(*local_peer_port) ||
            *local_manager_port == *local_peer_port ||
            !dht::network::is_valid_ipv4(request_tokens[2])) {
          std::cerr << "Invalid address or ports in register command.\n> ";
          continue;
        }

        pending_peer_socket =
            dht::network::create_bound_udp_socket(*local_peer_port);
        if (pending_peer_socket < 0) {
          std::cerr << "Unable to bind P2P socket to port " << *local_peer_port
                    << ".\n> ";
          continue;
        }

        if (!dht::network::bind_udp_socket(manager_socket,
                                           *local_manager_port)) {
          close(pending_peer_socket);
          std::cerr << "Unable to bind manager socket to port "
                    << *local_manager_port << ".\n> ";
          continue;
        }
        manager_socket_bound = true;
        pending_registration = true;
        std::cout << "Manager socket bound to port " << *local_manager_port
                  << ".\n";
      } else if (!manager_socket_bound) {
        std::cerr << "Register this peer before sending other commands.\n> ";
        continue;
      }

      if (!request_tokens.empty() && request_tokens[0] == "setup-dht") {
        if (request_tokens.size() != 4) {
          std::cerr << "Usage: setup-dht <leader> <ring-size> <year>\n> ";
          continue;
        }
        std::vector<StormRecord> records;
        std::string dataset_path = "details-" + request_tokens[3] + ".csv";
        if (!dht::peer::load_storm_records(dataset_path, records)) {
          std::cerr << "Unable to load dataset " << dataset_path
                    << "; setup-dht was not sent.\n> ";
          continue;
        }
      }

      if (!dht::network::send_udp_message(manager_socket, manager_endpoint,
                                          wire_message)) {
        if (pending_peer_socket >= 0) {
          close(pending_peer_socket);
          manager_socket_bound = false;
          if (!recreate_manager_socket(manager_socket)) {
            std::cerr << "Unable to recreate manager socket.\n";
            break;
          }
        }
        std::cerr << "Unable to send command to the manager.\n> ";
        continue;
      }

      char receive_buffer[kManagerBufferSize];
      sockaddr_in from_address;
      socklen_t from_length = sizeof(from_address);
      ssize_t bytes_received =
          recvfrom(manager_socket, receive_buffer, sizeof(receive_buffer) - 1,
                   MSG_WAITALL, reinterpret_cast<sockaddr *>(&from_address),
                   &from_length);

      if (bytes_received > 0) {
        receive_buffer[bytes_received] = '\0';
        std::string manager_response(receive_buffer);
        std::cout << "Manager replied: " << manager_response << "\n";

        if (pending_registration) {
          if (manager_response == "SUCCESS") {
            peer_socket = pending_peer_socket;
            pending_peer_socket = -1;
            std::cout << "Local P2P socket successfully bound to port "
                      << request_tokens[4] << ".\n";
          } else {
            close(pending_peer_socket);
            pending_peer_socket = -1;
            manager_socket_bound = false;
            if (!recreate_manager_socket(manager_socket)) {
              std::cerr << "Unable to recreate manager socket.\n";
              break;
            }
          }
        }

        handle_manager_response(wire_message, manager_response, peer_socket,
                                dht_state);
        maybe_report_setup_complete(manager_socket, manager_endpoint,
                                    dht_state);
      } else if (pending_peer_socket >= 0) {
        close(pending_peer_socket);
        manager_socket_bound = false;
        if (!recreate_manager_socket(manager_socket)) {
          std::cerr << "Unable to recreate manager socket.\n";
          break;
        }
      }
      std::cout << "> ";
    }

    if (peer_socket != -1 && FD_ISSET(peer_socket, &read_fds)) {
      char peer_buffer[kPeerBufferSize];
      sockaddr_in from_peer_address;
      socklen_t from_peer_length = sizeof(from_peer_address);
      ssize_t peer_bytes =
          recvfrom(peer_socket, peer_buffer, sizeof(peer_buffer) - 1,
                   MSG_WAITALL,
                   reinterpret_cast<sockaddr *>(&from_peer_address),
                   &from_peer_length);
      if (peer_bytes > 0) {
        peer_buffer[peer_bytes] = '\0';
        std::string peer_message(peer_buffer);
        std::cout << "\n[P2P Message Received]: " << peer_message << "\n> ";

        bool handled = false;
        if (peer_message.rfind("set-id|", 0) == 0) {
          handled = handle_set_id(peer_message, dht_state);
        } else if (peer_message.rfind("store|", 0) == 0) {
          handled =
              handle_store_message(peer_message, dht_state, peer_socket);
        } else if (peer_message.rfind("store-ack|", 0) == 0) {
          handled = handle_store_ack(peer_message, dht_state, peer_socket);
        }

        if (!handled)
          std::cerr << "Invalid or unsupported P2P message.\n";
        maybe_report_setup_complete(manager_socket, manager_endpoint,
                                    dht_state);
      }
    }
  }

  close(manager_socket);
  if (peer_socket != -1)
    close(peer_socket);
  return EXIT_SUCCESS;
}
