#include "common/network.hpp"
#include "common/protocol.hpp"
#include "src/peer/dht_storage.hpp"
#include "src/peer/ring_state.hpp"
#include "src/peer/storm_record.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <random>
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

constexpr std::size_t kManagerBufferSize = 65536;
constexpr std::size_t kPeerBufferSize = 65536;

enum class LocalOperation {
  None,
  Leaving,
  Joining,
  JoinLeader,
  TearingDown
};

enum class DistributionCompletion { None, InitialSetup, Rebuild };

struct DhtContext {
  dht::peer::RingState ring;
  DhtStorage storage;

  std::string local_name;
  std::string local_ip_address;
  int local_peer_port = 0;
  std::string dataset_year;
  std::optional<PeerAddress> query_target;
  LocalOperation operation = LocalOperation::None;
  DistributionCompletion distribution_completion =
      DistributionCompletion::None;
  std::vector<PeerAddress> pending_members;
  std::optional<PeerAddress> rebuild_coordinator;
  bool deregistered = false;

  // The leader uses these fields to decide when setup is actually complete.
  std::size_t total_records = 0;
  std::unordered_set<int> expected_event_ids;
  std::unordered_set<int> acked_event_ids;
  std::vector<std::size_t> records_per_peer;
  bool setup_active = false;
  bool completion_sent = false;
};

constexpr std::array<const char *, dht::peer::kStormFieldCount>
    kStormFieldLabels{"event id",         "state",          "year",
                      "month name",       "event type",     "cz type",
                      "cz name",          "injuries direct", "injuries indirect",
                      "deaths direct",    "deaths indirect", "damage property",
                      "damage crops",     "tor f scale"};

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

std::string join_ids(const std::vector<int> &ids) {
  std::vector<std::string> fields;
  fields.reserve(ids.size());
  for (int id : ids)
    fields.push_back(std::to_string(id));
  return dht::protocol::join_message(fields, ',');
}

bool parse_ids(const std::string &value, int ring_size,
               std::vector<int> &ids) {
  std::vector<int> parsed;
  std::unordered_set<int> unique;
  if (!value.empty()) {
    for (const std::string &field :
         dht::protocol::split_message(value, ',')) {
      std::optional<int> id = dht::protocol::parse_integer(field);
      if (!id || *id < 0 || *id >= ring_size || !unique.insert(*id).second)
        return false;
      parsed.push_back(*id);
    }
  }
  ids = std::move(parsed);
  return true;
}

bool send_find_result(int peer_socket, const std::string &requester_ip,
                      int requester_port, int event_id,
                      const std::vector<int> &visited,
                      const StormRecord *record) {
  std::vector<std::string> fields{"find-result",
                                  record ? "SUCCESS" : "FAILURE",
                                  std::to_string(event_id), join_ids(visited)};
  if (record)
    fields.insert(fields.end(), record->fields.begin(), record->fields.end());
  return dht::network::send_udp_message(
      peer_socket, {requester_ip, requester_port},
      dht::protocol::join_message(fields));
}

bool handle_find_event(const std::string &message, DhtContext &dht_state,
                       int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() != 6 || tokens[0] != "find-event" ||
      !dht_state.ring.configured() || !dht_state.storage.initialized()) {
    return false;
  }

  std::optional<int> event_id = dht::protocol::parse_integer(tokens[1]);
  std::optional<int> requester_port =
      dht::protocol::parse_integer(tokens[4]);
  std::vector<int> visited;
  if (!event_id || *event_id < 0 || !requester_port ||
      !dht::network::is_valid_port(*requester_port) ||
      !dht::network::is_valid_ipv4(tokens[3]) ||
      !parse_ids(tokens[5], dht_state.ring.size(), visited)) {
    return false;
  }

  const int local_id = dht_state.ring.local_id();
  if (std::find(visited.begin(), visited.end(), local_id) == visited.end())
    visited.push_back(local_id);

  const StormRecord *record = dht_state.storage.find(*event_id);
  if (record) {
    std::cout << "Found event " << *event_id << " on peer id " << local_id
              << "; returning it to " << tokens[2] << ".\n";
    return send_find_result(peer_socket, tokens[3], *requester_port,
                            *event_id, visited, record);
  }

  std::vector<int> candidates;
  for (int peer_id = 0; peer_id < dht_state.ring.size(); ++peer_id) {
    if (std::find(visited.begin(), visited.end(), peer_id) == visited.end())
      candidates.push_back(peer_id);
  }

  if (candidates.empty()) {
    std::cout << "Event " << *event_id
              << " was not found after probing every peer.\n";
    return send_find_result(peer_socket, tokens[3], *requester_port,
                            *event_id, visited, nullptr);
  }

  std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<std::size_t> distribution(
      0, candidates.size() - 1);
  int next_id = candidates[distribution(generator)];
  const PeerAddress *next_peer = dht_state.ring.peer_by_id(next_id);
  if (!next_peer)
    return false;

  tokens[5] = join_ids(visited);
  std::cout << "Hot-potato query for event " << *event_id << " moved from id "
            << local_id << " to id " << next_id << ".\n";
  return send_peer_message(peer_socket, *next_peer,
                           dht::protocol::join_message(tokens));
}

bool handle_find_result(const std::string &message) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() < 4 || tokens[0] != "find-result")
    return false;

  std::optional<int> event_id = dht::protocol::parse_integer(tokens[2]);
  if (!event_id || *event_id < 0)
    return false;

  if (tokens[1] == "FAILURE" && tokens.size() == 4) {
    std::cout << "Storm event " << *event_id << " not found in the DHT.\n";
    std::cout << "id-seq: " << tokens[3] << "\n";
    return true;
  }

  if (tokens[1] != "SUCCESS" ||
      tokens.size() != 4 + dht::peer::kStormFieldCount ||
      tokens[4] != tokens[2]) {
    return false;
  }

  std::cout << "Storm event " << *event_id << " found:\n";
  for (std::size_t index = 0; index < dht::peer::kStormFieldCount; ++index)
    std::cout << "  " << kStormFieldLabels[index] << ": " << tokens[4 + index]
              << "\n";
  std::cout << "id-seq: " << tokens[3] << "\n";
  return true;
}

bool recreate_manager_socket(int &manager_socket) {
  if (manager_socket >= 0)
    close(manager_socket);
  manager_socket = dht::network::create_udp_socket();
  return manager_socket >= 0;
}

std::optional<std::string>
exchange_with_manager(int manager_socket,
                      const dht::network::UdpEndpoint &manager,
                      const std::string &message) {
  if (!dht::network::send_udp_message(manager_socket, manager, message))
    return std::nullopt;

  char buffer[kManagerBufferSize];
  sockaddr_in response_address;
  socklen_t response_length = sizeof(response_address);
  ssize_t bytes_received =
      recvfrom(manager_socket, buffer, sizeof(buffer) - 1, MSG_WAITALL,
               reinterpret_cast<sockaddr *>(&response_address),
               &response_length);
  if (bytes_received <= 0)
    return std::nullopt;

  buffer[bytes_received] = '\0';
  return std::string(buffer);
}

std::vector<std::string>
ring_member_fields(const std::vector<PeerAddress> &members) {
  std::vector<std::string> fields;
  fields.reserve(members.size() * 3);
  for (const PeerAddress &member : members) {
    fields.push_back(member.name);
    fields.push_back(member.ip_address);
    fields.push_back(std::to_string(member.peer_port));
  }
  return fields;
}

bool same_members(const std::vector<PeerAddress> &left,
                  const std::vector<PeerAddress> &right) {
  if (left.size() != right.size())
    return false;
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (left[index].name != right[index].name ||
        left[index].ip_address != right[index].ip_address ||
        left[index].peer_port != right[index].peer_port) {
      return false;
    }
  }
  return true;
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

bool handle_init_storage(const std::string &message, DhtContext &dht_state,
                         int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() != 3 || tokens[0] != "init-storage" ||
      !dht_state.ring.configured()) {
    return false;
  }

  std::optional<int> table_size = dht::protocol::parse_integer(tokens[1]);
  if (!table_size || *table_size <= 0)
    return false;

  if (dht_state.local_name == tokens[2])
    return dht_state.ring.local_id() == 0;

  if (!dht_state.storage.initialized()) {
    if (!dht_state.storage.initialize(*table_size))
      return false;
  } else if (dht_state.storage.table_size() != *table_size) {
    return false;
  }
  return send_to_right_neighbor(peer_socket, dht_state, message);
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

  if (dht_state.ring.size() > 1) {
    std::string initialize_message = dht::protocol::join_message(
        {"init-storage", std::to_string(dht_state.storage.table_size()),
         dht_state.local_name});
    if (!send_to_right_neighbor(peer_socket, dht_state,
                                initialize_message)) {
      std::cerr << "Failed to initialize storage around the ring.\n";
      return false;
    }
  }

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

std::string build_reset_message(const std::string &mode,
                                const PeerAddress &coordinator,
                                const std::string &year, int next_id,
                                const std::vector<PeerAddress> &members) {
  std::vector<std::string> fields{
      "reset-id", mode, coordinator.name, coordinator.ip_address,
      std::to_string(coordinator.peer_port), year, std::to_string(next_id),
      std::to_string(members.size())};
  std::vector<std::string> member_fields = ring_member_fields(members);
  fields.insert(fields.end(), member_fields.begin(), member_fields.end());
  return dht::protocol::join_message(fields);
}

bool start_rebuild(const std::string &year, const PeerAddress &coordinator,
                   const std::vector<PeerAddress> &members,
                   DhtContext &dht_state, int peer_socket) {
  if (!dht_state.ring.configured() || dht_state.ring.local_id() != 0 ||
      !same_members(dht_state.ring.members(), members)) {
    return false;
  }

  dht_state.dataset_year = year;
  dht_state.rebuild_coordinator = coordinator;
  dht_state.distribution_completion = DistributionCompletion::Rebuild;
  dht_state.operation = LocalOperation::None;
  if (!distribute_records(year, dht_state, peer_socket)) {
    dht_state.distribution_completion = DistributionCompletion::None;
    return false;
  }
  std::cout << "Rebuilding the DHT as leader " << dht_state.local_name
            << ".\n";
  return true;
}

bool handle_reset_id(const std::string &message, DhtContext &dht_state,
                     int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  constexpr std::size_t member_start = 8;
  if (tokens.size() < member_start || tokens[0] != "reset-id" ||
      (tokens[1] != "leave" && tokens[1] != "join")) {
    return false;
  }

  std::optional<int> coordinator_port =
      dht::protocol::parse_integer(tokens[4]);
  std::optional<int> next_id = dht::protocol::parse_integer(tokens[6]);
  std::optional<int> ring_size = dht::protocol::parse_integer(tokens[7]);
  if (!coordinator_port ||
      !dht::network::is_valid_port(*coordinator_port) ||
      !dht::network::is_valid_ipv4(tokens[3]) || !next_id || !ring_size ||
      *ring_size < 1 || *next_id < 0 || *next_id > *ring_size) {
    return false;
  }

  PeerAddress coordinator{tokens[2], tokens[3], *coordinator_port};
  std::vector<PeerAddress> members;
  if (!parse_ring_members(tokens, member_start, *ring_size, members))
    return false;

  dht_state.pending_members = members;
  if (*next_id < *ring_size) {
    if (members[*next_id].name != dht_state.local_name ||
        !dht_state.ring.configure(*next_id, members)) {
      return false;
    }
    dht_state.storage.clear();
    std::cout << "Reset DHT id to " << *next_id << " of " << *ring_size
              << ".\n";

    int following_id = *next_id + 1;
    tokens[6] = std::to_string(following_id);
    const PeerAddress &destination =
        following_id < *ring_size
            ? members[following_id]
            : (tokens[1] == "leave" ? coordinator : members[0]);
    return send_peer_message(peer_socket, destination,
                             dht::protocol::join_message(tokens));
  }

  if (tokens[1] == "leave") {
    if (dht_state.operation != LocalOperation::Leaving ||
        dht_state.local_name != coordinator.name) {
      return false;
    }
    dht_state.ring.clear();
    std::vector<std::string> rebuild_fields{
        "rebuild-dht", tokens[5], coordinator.name, coordinator.ip_address,
        std::to_string(coordinator.peer_port), std::to_string(*ring_size)};
    std::vector<std::string> member_fields = ring_member_fields(members);
    rebuild_fields.insert(rebuild_fields.end(), member_fields.begin(),
                          member_fields.end());
    std::cout << "Ring identifiers reset; asking " << members[0].name
              << " to rebuild the DHT.\n";
    return send_peer_message(peer_socket, members[0],
                             dht::protocol::join_message(rebuild_fields));
  }

  if (dht_state.local_name != members[0].name)
    return false;
  return start_rebuild(tokens[5], coordinator, members, dht_state,
                       peer_socket);
}

bool handle_rebuild_dht(const std::string &message, DhtContext &dht_state,
                        int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  constexpr std::size_t member_start = 6;
  if (tokens.size() < member_start || tokens[0] != "rebuild-dht")
    return false;

  std::optional<int> coordinator_port =
      dht::protocol::parse_integer(tokens[4]);
  std::optional<int> ring_size = dht::protocol::parse_integer(tokens[5]);
  if (!coordinator_port ||
      !dht::network::is_valid_port(*coordinator_port) ||
      !dht::network::is_valid_ipv4(tokens[3]) || !ring_size ||
      *ring_size < 1) {
    return false;
  }

  std::vector<PeerAddress> members;
  if (!parse_ring_members(tokens, member_start, *ring_size, members) ||
      members[0].name != dht_state.local_name) {
    return false;
  }
  return start_rebuild(tokens[1],
                       PeerAddress{tokens[2], tokens[3], *coordinator_port},
                       members, dht_state, peer_socket);
}

bool handle_rebuild_complete(
    const std::string &message, DhtContext &dht_state, int manager_socket,
    const dht::network::UdpEndpoint &manager_endpoint) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() != 2 || tokens[0] != "rebuild-complete" ||
      (dht_state.operation != LocalOperation::Leaving &&
       dht_state.operation != LocalOperation::Joining) ||
      dht_state.pending_members.empty() ||
      tokens[1] != dht_state.pending_members[0].name) {
    return false;
  }

  std::string command = dht::protocol::join_message(
      {"dht-rebuilt", dht_state.local_name, tokens[1]});
  std::optional<std::string> response = exchange_with_manager(
      manager_socket, manager_endpoint, command);
  if (!response) {
    std::cerr << "Unable to report dht-rebuilt to the manager.\n";
    return false;
  }
  std::cout << "Manager replied: " << *response << "\n";
  if (*response != "SUCCESS")
    return false;

  if (dht_state.operation == LocalOperation::Leaving)
    std::cout << dht_state.local_name << " is now Free.\n";
  else
    std::cout << dht_state.local_name << " joined the rebuilt DHT.\n";
  dht_state.operation = LocalOperation::None;
  dht_state.rebuild_coordinator.reset();
  return true;
}

bool handle_teardown(const std::string &message, DhtContext &dht_state,
                     int peer_socket, int manager_socket,
                     const dht::network::UdpEndpoint &manager_endpoint) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() != 3 || tokens[0] != "teardown" ||
      (tokens[1] != "leave" && tokens[1] != "join" &&
       tokens[1] != "delete") ||
      !dht_state.ring.configured()) {
    return false;
  }

  if (tokens[2] != dht_state.local_name) {
    dht_state.storage.clear();
    std::cout << "Cleared local DHT for " << tokens[1] << " operation.\n";
    bool forwarded = send_to_right_neighbor(peer_socket, dht_state, message);
    if (forwarded)
      dht_state.ring.clear();
    return forwarded;
  }

  dht_state.storage.clear();
  if (tokens[1] == "delete") {
    if (dht_state.operation != LocalOperation::TearingDown)
      return false;
    dht_state.ring.clear();
    std::optional<std::string> response = exchange_with_manager(
        manager_socket, manager_endpoint,
        dht::protocol::join_message(
            {"teardown-complete", dht_state.local_name}));
    if (!response) {
      std::cerr << "Unable to report teardown-complete to the manager.\n";
      return false;
    }
    std::cout << "Manager replied: " << *response << "\n";
    if (*response != "SUCCESS")
      return false;
    dht_state.operation = LocalOperation::None;
    dht_state.dataset_year.clear();
    std::cout << "DHT teardown completed.\n";
    return true;
  }

  if (tokens[1] == "leave") {
    if (dht_state.operation != LocalOperation::Leaving ||
        dht_state.pending_members.empty()) {
      return false;
    }
    PeerAddress coordinator{dht_state.local_name,
                            dht_state.local_ip_address,
                            dht_state.local_peer_port};
    return send_peer_message(
        peer_socket, dht_state.pending_members[0],
        build_reset_message("leave", coordinator, dht_state.dataset_year, 0,
                            dht_state.pending_members));
  }

  if (dht_state.operation != LocalOperation::JoinLeader ||
      dht_state.pending_members.empty() || !dht_state.rebuild_coordinator ||
      dht_state.pending_members[0].name != dht_state.local_name ||
      !dht_state.ring.configure(0, dht_state.pending_members)) {
    return false;
  }

  if (dht_state.pending_members.size() == 1) {
    return start_rebuild(dht_state.dataset_year,
                         *dht_state.rebuild_coordinator,
                         dht_state.pending_members, dht_state, peer_socket);
  }
  return send_peer_message(
      peer_socket, dht_state.pending_members[1],
      build_reset_message("join", *dht_state.rebuild_coordinator,
                          dht_state.dataset_year, 1,
                          dht_state.pending_members));
}

bool handle_join_request(const std::string &message, DhtContext &dht_state,
                         int peer_socket) {
  std::vector<std::string> tokens = dht::protocol::split_message(message);
  if (tokens.size() != 5 || tokens[0] != "join-request" ||
      !dht_state.ring.configured() || dht_state.ring.local_id() != 0 ||
      dht_state.operation != LocalOperation::None) {
    return false;
  }

  std::optional<int> peer_port = dht::protocol::parse_integer(tokens[3]);
  if (!peer_port || !dht::network::is_valid_port(*peer_port) ||
      !dht::network::is_valid_ipv4(tokens[2])) {
    return false;
  }
  for (const PeerAddress &member : dht_state.ring.members()) {
    if (member.name == tokens[1])
      return false;
  }

  PeerAddress joining_peer{tokens[1], tokens[2], *peer_port};
  dht_state.pending_members = dht_state.ring.members();
  dht_state.pending_members.push_back(joining_peer);
  dht_state.rebuild_coordinator = joining_peer;
  dht_state.dataset_year = tokens[4];
  dht_state.operation = LocalOperation::JoinLeader;
  std::cout << "Starting ring rebuild to add " << joining_peer.name << ".\n";
  return send_to_right_neighbor(
      peer_socket, dht_state,
      dht::protocol::join_message(
          {"teardown", "join", dht_state.local_name}));
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

  if (request_tokens[0] == "register" && request_tokens.size() == 5 &&
      response_tokens.size() == 1 && response_tokens[0] == "SUCCESS") {
    std::optional<int> peer_port =
        dht::protocol::parse_integer(request_tokens[4]);
    if (peer_port) {
      dht_state.local_name = request_tokens[1];
      dht_state.local_ip_address = request_tokens[2];
      dht_state.local_peer_port = *peer_port;
    }
    return;
  }

  if (request_tokens[0] == "deregister" && request_tokens.size() == 2 &&
      response_tokens.size() == 1 && response_tokens[0] == "SUCCESS") {
    dht_state.deregistered = true;
    std::cout << "Deregistered successfully; peer is exiting.\n";
    return;
  }

  if (request_tokens[0] == "query-dht" && request_tokens.size() == 2 &&
      response_tokens.size() == 4 && response_tokens[0] == "SUCCESS") {
    std::optional<int> peer_port =
        dht::protocol::parse_integer(response_tokens[3]);
    if (!peer_port || !dht::network::is_valid_port(*peer_port) ||
        !dht::network::is_valid_ipv4(response_tokens[2])) {
      std::cerr << "Manager returned an invalid DHT query target.\n";
      return;
    }
    dht_state.query_target =
        PeerAddress{response_tokens[1], response_tokens[2], *peer_port};
    std::cout << "Query entry point: " << response_tokens[1] << " at "
              << response_tokens[2] << ":" << *peer_port
              << ". Enter find-event <event-id>.\n";
    return;
  }

  if (request_tokens[0] == "leave-dht" && request_tokens.size() == 2 &&
      response_tokens.size() == 2 && response_tokens[0] == "SUCCESS") {
    if (!dht_state.ring.configured() ||
        request_tokens[1] != dht_state.local_name ||
        dht_state.ring.size() <= 1) {
      std::cerr << "Cannot begin leave: local ring state is invalid.\n";
      return;
    }

    dht_state.dataset_year = response_tokens[1];
    dht_state.pending_members.clear();
    for (int offset = 1; offset < dht_state.ring.size(); ++offset) {
      int member_id =
          (dht_state.ring.local_id() + offset) % dht_state.ring.size();
      const PeerAddress *member = dht_state.ring.peer_by_id(member_id);
      if (!member)
        return;
      dht_state.pending_members.push_back(*member);
    }
    dht_state.operation = LocalOperation::Leaving;
    if (!send_to_right_neighbor(
            peer_socket, dht_state,
            dht::protocol::join_message(
                {"teardown", "leave", dht_state.local_name}))) {
      std::cerr << "Unable to start leave teardown around the ring.\n";
      return;
    }
    std::cout << "Leave accepted; tearing down the old DHT.\n";
    return;
  }

  if (request_tokens[0] == "join-dht" && request_tokens.size() == 2 &&
      response_tokens.size() >= 3 && response_tokens[0] == "SUCCESS") {
    std::optional<int> ring_size =
        dht::protocol::parse_integer(response_tokens[2]);
    std::vector<PeerAddress> current_members;
    if (!ring_size || *ring_size < 1 ||
        !parse_ring_members(response_tokens, 3, *ring_size,
                            current_members) ||
        request_tokens[1] != dht_state.local_name) {
      std::cerr << "Manager returned invalid join configuration.\n";
      return;
    }

    PeerAddress local_peer{dht_state.local_name,
                           dht_state.local_ip_address,
                           dht_state.local_peer_port};
    dht_state.dataset_year = response_tokens[1];
    dht_state.pending_members = current_members;
    dht_state.pending_members.push_back(local_peer);
    dht_state.operation = LocalOperation::Joining;
    std::string join_request = dht::protocol::join_message(
        {"join-request", local_peer.name, local_peer.ip_address,
         std::to_string(local_peer.peer_port), dht_state.dataset_year});
    if (!send_peer_message(peer_socket, current_members[0], join_request)) {
      std::cerr << "Unable to send join request to the DHT leader.\n";
      return;
    }
    std::cout << "Join accepted; asked leader " << current_members[0].name
              << " to rebuild the ring.\n";
    return;
  }

  if (request_tokens[0] == "teardown-dht" && request_tokens.size() == 2 &&
      response_tokens.size() == 1 && response_tokens[0] == "SUCCESS") {
    if (!dht_state.ring.configured() ||
        dht_state.ring.local_id() != 0 ||
        request_tokens[1] != dht_state.local_name) {
      std::cerr << "Cannot begin teardown: this peer is not the leader.\n";
      return;
    }
    dht_state.operation = LocalOperation::TearingDown;
    if (!send_to_right_neighbor(
            peer_socket, dht_state,
            dht::protocol::join_message(
                {"teardown", "delete", dht_state.local_name}))) {
      std::cerr << "Unable to start DHT teardown around the ring.\n";
      return;
    }
    std::cout << "Teardown accepted; deleting the DHT around the ring.\n";
    return;
  }

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
  dht_state.dataset_year = request_tokens[3];
  dht_state.distribution_completion = DistributionCompletion::InitialSetup;
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

void maybe_report_setup_complete(int manager_socket, int peer_socket,
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
  for (std::size_t peer_id = 0; peer_id < dht_state.records_per_peer.size();
       ++peer_id) {
    std::cout << "Peer id " << peer_id << " stored "
              << dht_state.records_per_peer[peer_id] << " records.\n";
  }

  if (dht_state.distribution_completion ==
      DistributionCompletion::InitialSetup) {
    std::optional<std::string> response = exchange_with_manager(
        manager_socket, manager,
        dht::protocol::join_message({"dht-complete", local_peer->name}));
    if (!response) {
      std::cerr << "Unable to exchange dht-complete with the manager.\n";
      return;
    }
    std::cout << "Manager replied: " << *response << "\n";
    if (*response != "SUCCESS")
      return;
  } else if (dht_state.distribution_completion ==
             DistributionCompletion::Rebuild) {
    if (!dht_state.rebuild_coordinator ||
        !send_peer_message(
            peer_socket, *dht_state.rebuild_coordinator,
            dht::protocol::join_message(
                {"rebuild-complete", local_peer->name}))) {
      std::cerr << "Unable to signal rebuild completion to the coordinator.\n";
      return;
    }
    std::cout << "Signalled rebuilt DHT completion to "
              << dht_state.rebuild_coordinator->name << ".\n";
  } else {
    return;
  }

  dht_state.completion_sent = true;
  dht_state.setup_active = false;
  dht_state.distribution_completion = DistributionCompletion::None;
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

  while (!dht_state.deregistered) {
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

      if (!request_tokens.empty() && request_tokens[0] == "find-event") {
        if (request_tokens.size() != 2) {
          std::cerr << "Usage: find-event <event-id>\n> ";
          continue;
        }
        std::optional<int> event_id =
            dht::protocol::parse_integer(request_tokens[1]);
        if (!event_id || *event_id < 0 || dht_state.local_name.empty() ||
            !dht_state.query_target) {
          std::cerr << "Run a successful query-dht before find-event.\n> ";
          continue;
        }

        std::vector<std::string> query_fields{
            "find-event", request_tokens[1], dht_state.local_name,
            dht_state.local_ip_address,
            std::to_string(dht_state.local_peer_port), ""};
        if (!send_peer_message(peer_socket, *dht_state.query_target,
                               dht::protocol::join_message(query_fields))) {
          std::cerr << "Unable to send find-event to the DHT entry point.\n> ";
          continue;
        }
        std::cout << "Sent find-event for " << *event_id << " to "
                  << dht_state.query_target->name << ".\n> ";
        dht_state.query_target.reset();
        continue;
      }

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
        maybe_report_setup_complete(manager_socket, peer_socket,
                                    manager_endpoint,
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
        } else if (peer_message.rfind("init-storage|", 0) == 0) {
          handled =
              handle_init_storage(peer_message, dht_state, peer_socket);
        } else if (peer_message.rfind("find-event|", 0) == 0) {
          handled = handle_find_event(peer_message, dht_state, peer_socket);
        } else if (peer_message.rfind("find-result|", 0) == 0) {
          handled = handle_find_result(peer_message);
        } else if (peer_message.rfind("join-request|", 0) == 0) {
          handled =
              handle_join_request(peer_message, dht_state, peer_socket);
        } else if (peer_message.rfind("teardown|", 0) == 0) {
          handled = handle_teardown(peer_message, dht_state, peer_socket,
                                    manager_socket, manager_endpoint);
        } else if (peer_message.rfind("reset-id|", 0) == 0) {
          handled = handle_reset_id(peer_message, dht_state, peer_socket);
        } else if (peer_message.rfind("rebuild-dht|", 0) == 0) {
          handled =
              handle_rebuild_dht(peer_message, dht_state, peer_socket);
        } else if (peer_message.rfind("rebuild-complete|", 0) == 0) {
          handled = handle_rebuild_complete(
              peer_message, dht_state, manager_socket, manager_endpoint);
        }

        if (!handled)
          std::cerr << "Invalid or unsupported P2P message.\n";
        maybe_report_setup_complete(manager_socket, peer_socket,
                                    manager_endpoint,
                                    dht_state);
      }
    }
  }

  close(manager_socket);
  if (peer_socket != -1)
    close(peer_socket);
  return EXIT_SUCCESS;
}
