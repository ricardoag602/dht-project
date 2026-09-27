#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <unordered_map>
#include <unistd.h>
#include <vector>

// Decode a pipe-delimited network message while preserving empty fields.
std::vector<std::string> split_message(const std::string &msg, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream tokenStream(msg);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
  if (!msg.empty() && msg.back() == delimiter)
    tokens.push_back("");
  return tokens;
}

struct PeerAddress {
  std::string name;
  std::string ip_address;
  int peer_port;
};

struct StormRecord {
  std::vector<std::string> fields;
  int event_id;
};

// Maintain the peer's DHT state and local hash table.
struct DhtContext {
  bool configured = false;
  int peer_id = -1;
  int ring_size = 0;
  std::string local_peer_name;
  std::vector<PeerAddress> ring_peers;
  PeerAddress right_peer;
  int hash_table_size = 0;
  std::unordered_map<int, StormRecord> local_hash_table;
  size_t total_records = 0;
  size_t acknowledged_records = 0;
  std::vector<size_t> record_counts;
  bool setup_in_progress = false;
  bool completion_reported = false;
};

// Parse a CSV line into fields, handling quoted commas and escaped quotes.
std::vector<std::string> parse_csv_line(const std::string &line) {
  std::vector<std::string> fields;
  std::string field;
  bool quoted = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char current = line[i];
    if (current == '"') {
      if (quoted && i + 1 < line.size() && line[i + 1] == '"') {
        field += '"';
        ++i;
      } else {
        quoted = !quoted;
      }
    } else if (current == ',' && !quoted) {
      fields.push_back(field);
      field.clear();
    } else {
      field += current;
    }
  }
  fields.push_back(field);
  return fields;
}

// Test whether a candidate can be used as the local hash-table size.
bool is_prime(int value) {
  if (value < 2)
    return false;
  for (int divisor = 2; divisor * divisor <= value; ++divisor) {
    if (value % divisor == 0)
      return false;
  }
  return true;
}

int first_prime_larger_than(int value) {
  int candidate = value + 1;
  while (!is_prime(candidate))
    ++candidate;
  return candidate;
}

// Read the selected NWS file and convert each row into a storm record.
bool load_storm_records(const std::string &path,
                        std::vector<StormRecord> &records) {
  std::ifstream input(path);
  if (!input)
    return false;

  records.clear();
  std::string line;
  std::getline(input, line); // Skip the CSV header.
  while (std::getline(input, line)) {
    if (line.empty())
      continue;

    std::vector<std::string> fields = parse_csv_line(line);
    if (fields.size() != 14)
      return false;

    try {
      records.push_back({fields, std::stoi(fields[0])});
    } catch (const std::exception &) {
      return false;
    }
  }
  return true;
}

// Build the wire format used to move one record around the logical ring.
std::string build_store_message(int destination_id, int position,
           int hash_table_size,
                               const StormRecord &record) {
  std::string message = "store|" + std::to_string(destination_id) + "|" +
         std::to_string(position) + "|" +
         std::to_string(hash_table_size);
  for (const std::string &field : record.fields)
    message += "|" + field;
  return message;
}

bool send_peer_message(int socket, const PeerAddress &peer,
                  const std::string &message) {
  struct sockaddr_in destination;
  std::memset(&destination, 0, sizeof(destination));
  destination.sin_family = AF_INET;
  destination.sin_port = htons(peer.peer_port);
  if (inet_pton(AF_INET, peer.ip_address.c_str(), &destination.sin_addr) != 1)
    return false;

  ssize_t sent = sendto(socket, message.c_str(), message.length(), 0,
                        (const struct sockaddr *)&destination,
                        sizeof(destination));
  return sent == static_cast<ssize_t>(message.length());
}

// Store a record locally when this peer owns it, or forward it to the right.
bool handle_store_message(const std::string &message, DhtContext &dht_state,
                  int p_port_socket) {
  std::vector<std::string> tokens = split_message(message, '|');
  const size_t record_start = 4;
  if (tokens.size() != record_start + 14 || tokens[0] != "store")
    return false;

  int destination_id;
  int position;
  int hash_table_size;
  try {
    destination_id = std::stoi(tokens[1]);
    position = std::stoi(tokens[2]);
    hash_table_size = std::stoi(tokens[3]);
  } catch (const std::exception &) {
    return false;
  }

  if (!dht_state.configured || hash_table_size <= 0 || destination_id < 0 ||
      destination_id >= dht_state.ring_size || position < 0 ||
      (dht_state.hash_table_size != 0 &&
       dht_state.hash_table_size != hash_table_size) ||
      position >= hash_table_size)
    return false;

  dht_state.hash_table_size = hash_table_size;

  StormRecord record;
  record.fields.assign(tokens.begin() + record_start, tokens.end());
  try {
    record.event_id = std::stoi(record.fields[0]);
  } catch (const std::exception &) {
    return false;
  }

  if (destination_id == dht_state.peer_id) {
    dht_state.local_hash_table[position] = record;
    if (dht_state.peer_id == 0) {
      ++dht_state.acknowledged_records;
      ++dht_state.record_counts[0];
    } else {
      std::string acknowledgement =
          "store-ack|" + std::to_string(destination_id) + "|" +
          std::to_string(record.event_id) + "|" + std::to_string(position);
      if (!send_peer_message(p_port_socket, dht_state.right_peer,
                        acknowledgement)) {
        return false;
      }
    }
    std::cout << "Stored event " << record.event_id << " at position "
              << position << " on peer id " << dht_state.peer_id << ".\n";
    return true;
  }

  if (!send_peer_message(p_port_socket, dht_state.right_peer, message)) {
    std::cerr << "Failed to forward store for event " << record.event_id
              << ".\n";
    return false;
  }
  std::cout << "Forwarded store for event " << record.event_id << " from peer "
            << dht_state.peer_id << " to " << dht_state.right_peer.name
            << ".\n";
  return true;
}

// Return store acknowledgements to the leader through the same ring direction.
bool handle_store_ack(const std::string &message, DhtContext &dht_state,
                      int p_port_socket) {
  std::vector<std::string> tokens = split_message(message, '|');
  if (tokens.size() != 4 || tokens[0] != "store-ack")
    return false;

  int destination_id;
  try {
    destination_id = std::stoi(tokens[1]);
  } catch (const std::exception &) {
    return false;
  }

  if (destination_id < 0 || destination_id >= dht_state.ring_size)
    return false;

  if (dht_state.peer_id == 0) {
    ++dht_state.acknowledged_records;
    ++dht_state.record_counts[destination_id];
    return true;
  }
  return send_peer_message(p_port_socket, dht_state.right_peer, message);
}

// Load the selected year, hash each record, and start ring-based distribution.
bool distribute_records(const std::string &year, DhtContext &dht_state,
                              int p_port_socket) {
  std::vector<StormRecord> records;
  std::string path = "details-" + year + ".csv";
  if (!load_storm_records(path, records)) {
    std::cerr << "Unable to load dataset " << path
              << " (expected 14 fields per record).\n";
    return false;
  }

  dht_state.hash_table_size =
      first_prime_larger_than(static_cast<int>(2 * records.size()));
  dht_state.local_hash_table.clear();
  dht_state.total_records = records.size();
  dht_state.acknowledged_records = 0;
  dht_state.record_counts.assign(dht_state.ring_size, 0);
  dht_state.setup_in_progress = true;
  dht_state.completion_reported = false;

  std::cout << "Loaded " << records.size() << " records from " << path
            << "; local hash table size is " << dht_state.hash_table_size
            << ".\n";

  for (const StormRecord &record : records) {
    int position = record.event_id % dht_state.hash_table_size;
    int destination_id = position % dht_state.ring_size;
    std::string message =
        build_store_message(destination_id, position, dht_state.hash_table_size,
               record);

    if (destination_id == dht_state.peer_id) {
      if (!handle_store_message(message, dht_state, p_port_socket))
        return false;
    } else if (!send_peer_message(p_port_socket, dht_state.right_peer, message)) {
      std::cerr << "Failed to send store for event " << record.event_id
                << " to the right neighbor.\n";
      return false;
    }
  }
  return true;
}

// Create the UDP socket used for peer-to-peer ring traffic.
int create_peer_socket(int port) {
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (sock < 0)
    return -1;

  struct sockaddr_in p_addr;
  std::memset(&p_addr, 0, sizeof(p_addr));
  p_addr.sin_family = AF_INET;
  p_addr.sin_addr.s_addr = INADDR_ANY;
  p_addr.sin_port = htons(port);

  if (bind(sock, (const struct sockaddr *)&p_addr, sizeof(p_addr)) < 0) {
    std::cerr << "Error: Bind failed on P2P port " << port
              << ". Port may be in use.\n";
    close(sock);
    return -1;
  }
  return sock;
}

bool bind_manager_socket(int socket, int port) {
  struct sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);
  return bind(socket, (const struct sockaddr *)&address, sizeof(address)) == 0;
}

// Apply a manager response and trigger the peer-side work it starts.
void handle_manager_response(const std::string &req_msg,
                             const std::string &resp_msg, int &p_port_socket,
                             DhtContext &dht_state) {
  std::vector<std::string> req_tokens = split_message(req_msg, '|');
  std::vector<std::string> resp_tokens = split_message(resp_msg, '|');

  if (req_tokens.empty() || resp_tokens.empty())
    return;

  // Handle successful registration: Bind the local P2P socket
  if (req_tokens[0] == "register" && resp_tokens[0] == "SUCCESS") {
    int peer_port = std::stoi(req_tokens[4]);
    p_port_socket = create_peer_socket(peer_port);

    if (p_port_socket != -1) {
      std::cout << "Local P2P socket successfully bound to port " << peer_port
                << ".\n";
    }
  }

  // Handle successful DHT setup: Act as Leader and route set-id messages[cite:
  // 1]
  if (req_tokens[0] == "setup-dht" && resp_tokens[0] == "SUCCESS") {
    int n = std::stoi(req_tokens[2]);
    if (resp_tokens.size() != static_cast<size_t>(1 + (3 * n))) {
      std::cerr << "Invalid setup-dht response: expected " << n
                << " peer tuples.\n";
      return;
    }

    dht_state.configured = true;
    dht_state.local_peer_name = req_tokens[1];
    dht_state.peer_id = 0;
    dht_state.ring_size = n;
    dht_state.ring_peers.clear();

    for (int i = 0; i < n; ++i) {
      int base_idx = 1 + (i * 3);
      dht_state.ring_peers.push_back(
          {resp_tokens[base_idx], resp_tokens[base_idx + 1],
           std::stoi(resp_tokens[base_idx + 2])});
    }
    dht_state.right_peer = dht_state.ring_peers[1 % n];

    std::cout << "Assigned Leader status. Building logical ring of size " << n
              << "...\n";

    std::string all_tuples = "";
    for (size_t i = 1; i < resp_tokens.size(); ++i) {
      all_tuples += "|" + resp_tokens[i];
    }

    for (int i = 1; i < n; ++i) {
      int base_idx = 1 + (i * 3);
      std::string target_name = resp_tokens[base_idx];
      std::string target_ip = resp_tokens[base_idx + 1];
      int target_port = std::stoi(resp_tokens[base_idx + 2]);

      std::string set_id_msg =
          "set-id|" + std::to_string(i) + "|" + std::to_string(n) + all_tuples;

      struct sockaddr_in peer_dest;
      std::memset(&peer_dest, 0, sizeof(peer_dest));
      peer_dest.sin_family = AF_INET;
      peer_dest.sin_port = htons(target_port);
      inet_pton(AF_INET, target_ip.c_str(), &peer_dest.sin_addr);

      sendto(p_port_socket, set_id_msg.c_str(), set_id_msg.length(), 0,
             (const struct sockaddr *)&peer_dest, sizeof(peer_dest));

      std::cout << "Sent set-id data to " << target_name << " at " << target_ip
                << ":" << target_port << "\n";
    }

    if (!distribute_records(req_tokens[3], dht_state, p_port_socket)) {
      std::cerr << "DHT setup stopped before record distribution.\n";
      return;
    }
    std::cout << "Leader stored " << dht_state.local_hash_table.size()
              << " records locally.\n";
  }
}

bool handle_set_id(const std::string &msg, DhtContext &dht_state) {
  std::vector<std::string> tokens = split_message(msg, '|');
  if (tokens.size() < 3 || tokens[0] != "set-id")
    return false;

  int peer_id = std::stoi(tokens[1]);
  int ring_size = std::stoi(tokens[2]);
  if (ring_size < 3 || peer_id < 1 || peer_id >= ring_size ||
      tokens.size() != static_cast<size_t>(3 + (3 * ring_size))) {
    return false;
  }

  dht_state.configured = true;
  dht_state.peer_id = peer_id;
  dht_state.ring_size = ring_size;
  dht_state.ring_peers.clear();

  for (int i = 0; i < ring_size; ++i) {
    int base_idx = 3 + (i * 3);
    dht_state.ring_peers.push_back(
        {tokens[base_idx], tokens[base_idx + 1],
         std::stoi(tokens[base_idx + 2])});
  }
  dht_state.right_peer = dht_state.ring_peers[(peer_id + 1) % ring_size];

  std::cout << "Configured DHT id=" << dht_state.peer_id
            << ", ring size=" << dht_state.ring_size << ", right neighbor="
            << dht_state.right_peer.name << " at "
            << dht_state.right_peer.ip_address << ":"
            << dht_state.right_peer.peer_port << "\n";
  return true;
}

// Start the peer, then service terminal commands and incoming P2P messages.
int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <manager_ipv4> <manager_port>\n";
    return EXIT_FAILURE;
  }

  std::string manager_ip = argv[1];
  int manager_port;
  try {
    manager_port = std::stoi(argv[2]);
  } catch (...) {
    std::cerr << "Error: Invalid port.\n";
    return EXIT_FAILURE;
  }

  int manager_socket = socket(AF_INET, SOCK_DGRAM, 0);
  struct sockaddr_in dest_addr;
  std::memset(&dest_addr, 0, sizeof(dest_addr));
  dest_addr.sin_family = AF_INET;
  dest_addr.sin_port = htons(manager_port);
  inet_pton(AF_INET, manager_ip.c_str(), &dest_addr.sin_addr);

  int peer_socket = -1;
  bool manager_socket_bound = false;
  DhtContext dht_state;
  std::string wire_msg = "";

  std::cout << "Peer started. Manager at " << manager_ip << ":" << manager_port
            << "\n";
  std::cout
      << "Enter commands (e.g., register <name> <ip> <m-port> <p-port>)\n> ";

  // infinite loop to handle terminal input and incoming P2P messages
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

    // --- Handle Terminal Input ---
    if (FD_ISSET(STDIN_FILENO, &read_fds)) {
      std::string input_line;
      std::getline(std::cin, input_line);

      if (input_line.empty()) {
        std::cout << "> ";
        continue;
      }

      std::istringstream iss(input_line);
      std::string token;
      wire_msg = "";
      bool first = true;
      while (iss >> token) {
        if (!first)
          wire_msg += "|";
        wire_msg += token;
        first = false;
      }

      // Send the command to the manager and wait for a response.
      std::vector<std::string> request_tokens = split_message(wire_msg, '|');
      if (request_tokens.size() == 5 && request_tokens[0] == "register" &&
          !manager_socket_bound) {
        int manager_peer_port;
        try {
          manager_peer_port = std::stoi(request_tokens[3]);
        } catch (const std::exception &) {
          std::cerr << "Invalid manager port in register command.\n";
          std::cout << "> ";
          continue;
        }

        if (!bind_manager_socket(manager_socket, manager_peer_port)) {
          std::cerr << "Unable to bind manager socket to port "
                    << manager_peer_port << ".\n";
          std::cout << "> ";
          continue;
        }
        manager_socket_bound = true;
        std::cout << "Manager socket bound to port " << manager_peer_port
                  << ".\n";
      }

      sendto(manager_socket, wire_msg.c_str(), wire_msg.length(), 0,
             (const struct sockaddr *)&dest_addr, sizeof(dest_addr));

      char recv_buffer[1024];
      struct sockaddr_in from_addr;
      socklen_t from_len = sizeof(from_addr);
      ssize_t bytes_received =
          recvfrom(manager_socket, recv_buffer, sizeof(recv_buffer) - 1,
                   MSG_WAITALL, (struct sockaddr *)&from_addr, &from_len);

      if (bytes_received > 0) {
        recv_buffer[bytes_received] = '\0';
        std::string manager_response(recv_buffer);
        std::cout << "Manager replied: " << manager_response << "\n";

        handle_manager_response(wire_msg, manager_response, peer_socket,
               dht_state);
      }
      std::cout << "> ";
    }

    // --- Handle Incoming P2P Messages ---
    if (peer_socket != -1 && FD_ISSET(peer_socket, &read_fds)) {
      char p2p_buffer[2048];
      struct sockaddr_in from_peer_addr;
      socklen_t from_peer_len = sizeof(from_peer_addr);

      ssize_t p2p_bytes = recvfrom(
          peer_socket, p2p_buffer, sizeof(p2p_buffer) - 1, MSG_WAITALL,
          (struct sockaddr *)&from_peer_addr, &from_peer_len);
      if (p2p_bytes > 0) {
        p2p_buffer[p2p_bytes] = '\0';
        std::cout << "\n[P2P Message Received]: " << p2p_buffer << "\n> ";
        bool handled = false;
        std::string p2p_message(p2p_buffer);
        if (p2p_message.rfind("set-id|", 0) == 0) {
          handled = handle_set_id(p2p_message, dht_state);
        } else if (p2p_message.rfind("store|", 0) == 0) {
          handled = handle_store_message(p2p_message, dht_state, peer_socket);
        } else if (p2p_message.rfind("store-ack|", 0) == 0) {
          handled = handle_store_ack(p2p_message, dht_state, peer_socket);
        }
        if (!handled) {
          std::cerr << "Invalid or unsupported P2P message.\n";
        }

        if (dht_state.peer_id == 0 && dht_state.setup_in_progress &&
            !dht_state.completion_reported &&
            dht_state.acknowledged_records == dht_state.total_records) {
          wire_msg = "dht-complete|" + dht_state.local_peer_name;
          sendto(manager_socket, wire_msg.c_str(), wire_msg.length(), 0,
                 (const struct sockaddr *)&dest_addr, sizeof(dest_addr));

          char completion_buffer[1024];
          struct sockaddr_in completion_addr;
          socklen_t completion_len = sizeof(completion_addr);
          ssize_t completion_bytes = recvfrom(
              manager_socket, completion_buffer, sizeof(completion_buffer) - 1,
              MSG_WAITALL, (struct sockaddr *)&completion_addr,
              &completion_len);
          if (completion_bytes > 0) {
            completion_buffer[completion_bytes] = '\0';
            std::string completion_response(completion_buffer);
            std::cout << "Manager replied: " << completion_response << "\n";
            if (completion_response == "SUCCESS") {
                  dht_state.completion_reported = true;
                  dht_state.setup_in_progress = false;
              for (size_t peer_id = 0;
                    peer_id < dht_state.record_counts.size(); ++peer_id) {
                std::cout << "Peer id " << peer_id << " stored "
                     << dht_state.record_counts[peer_id]
                          << " records.\n";
              }
            }
          }
        }
      }
    }
  }

  close(manager_socket);
  if (peer_socket != -1)
    close(peer_socket);
  return EXIT_SUCCESS;
}
