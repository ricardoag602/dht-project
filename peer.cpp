#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

// Helper: Split incoming delimited strings
std::vector<std::string> splitMessage(const std::string &msg, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream tokenStream(msg);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}

struct PeerEndpoint {
  std::string name;
  std::string ip;
  int p_port;
};

struct DhtState {
  bool configured = false;
  int id = -1;
  int ring_size = 0;
  std::vector<PeerEndpoint> peers;
  PeerEndpoint right_neighbor;
};

// Helper: Safely bind the P2P socket with error handling
int setup_p2p_socket(int port) {
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

// Helper: Process the manager's response and execute subsequent P2P actions
void handle_manager_response(const std::string &req_msg,
                             const std::string &resp_msg, int &p_port_socket,
                             DhtState &dht_state) {
  std::vector<std::string> req_tokens = splitMessage(req_msg, '|');
  std::vector<std::string> resp_tokens = splitMessage(resp_msg, '|');

  if (req_tokens.empty() || resp_tokens.empty())
    return;

  // Handle successful registration: Bind the local P2P socket
  if (req_tokens[0] == "register" && resp_tokens[0] == "SUCCESS") {
    int p_port = std::stoi(req_tokens[4]);
    p_port_socket = setup_p2p_socket(p_port);

    if (p_port_socket != -1) {
      std::cout << "Local P2P socket successfully bound to port " << p_port
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
    dht_state.id = 0;
    dht_state.ring_size = n;
    dht_state.peers.clear();

    for (int i = 0; i < n; ++i) {
      int base_idx = 1 + (i * 3);
      dht_state.peers.push_back(
          {resp_tokens[base_idx], resp_tokens[base_idx + 1],
           std::stoi(resp_tokens[base_idx + 2])});
    }
    dht_state.right_neighbor = dht_state.peers[1 % n];

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
  }
}

bool handle_set_id(const std::string &msg, DhtState &dht_state) {
  std::vector<std::string> tokens = splitMessage(msg, '|');
  if (tokens.size() < 3 || tokens[0] != "set-id")
    return false;

  int peer_id = std::stoi(tokens[1]);
  int ring_size = std::stoi(tokens[2]);
  if (ring_size < 3 || peer_id < 1 || peer_id >= ring_size ||
      tokens.size() != static_cast<size_t>(3 + (3 * ring_size))) {
    return false;
  }

  dht_state.configured = true;
  dht_state.id = peer_id;
  dht_state.ring_size = ring_size;
  dht_state.peers.clear();

  for (int i = 0; i < ring_size; ++i) {
    int base_idx = 3 + (i * 3);
    dht_state.peers.push_back(
        {tokens[base_idx], tokens[base_idx + 1],
         std::stoi(tokens[base_idx + 2])});
  }
  dht_state.right_neighbor = dht_state.peers[(peer_id + 1) % ring_size];

  std::cout << "Configured DHT id=" << dht_state.id
            << ", ring size=" << dht_state.ring_size << ", right neighbor="
            << dht_state.right_neighbor.name << " at "
            << dht_state.right_neighbor.ip << ":"
            << dht_state.right_neighbor.p_port << "\n";
  return true;
}

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

  int p_port_socket = -1;
  DhtState dht_state;
  std::string wire_msg = "";

  std::cout << "Peer started. Manager at " << manager_ip << ":" << manager_port
            << "\n";
  std::cout
      << "Enter commands (e.g., register <name> <ip> <m-port> <p-port>)\n> ";

  while (true) {
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(STDIN_FILENO, &read_fds);
    int max_fd = STDIN_FILENO;

    if (p_port_socket != -1) {
      FD_SET(p_port_socket, &read_fds);
      if (p_port_socket > max_fd)
        max_fd = p_port_socket;
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

        handle_manager_response(wire_msg, manager_response, p_port_socket,
               dht_state);
      }
      std::cout << "> ";
    }

    // --- Handle Incoming P2P Messages ---
    if (p_port_socket != -1 && FD_ISSET(p_port_socket, &read_fds)) {
      char p2p_buffer[2048];
      struct sockaddr_in from_peer_addr;
      socklen_t from_peer_len = sizeof(from_peer_addr);

      ssize_t p2p_bytes = recvfrom(
          p_port_socket, p2p_buffer, sizeof(p2p_buffer) - 1, MSG_WAITALL,
          (struct sockaddr *)&from_peer_addr, &from_peer_len);
      if (p2p_bytes > 0) {
        p2p_buffer[p2p_bytes] = '\0';
        std::cout << "\n[P2P Message Received]: " << p2p_buffer << "\n> ";
        if (!handle_set_id(p2p_buffer, dht_state)) {
          std::cerr << "Invalid or unsupported P2P message.\n";
        }
      }
    }
  }

  close(manager_socket);
  if (p_port_socket != -1)
    close(p_port_socket);
  return EXIT_SUCCESS;
}
