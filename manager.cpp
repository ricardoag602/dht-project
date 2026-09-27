#include <arpa/inet.h> // IP address conversion function
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netinet/in.h> 
#include <sstream>
#include <string>
#include <sys/socket.h> // Core socket functions
#include <unistd.h>     // POSIX API access (close function)
#include <unordered_map>
#include <random>
#include <vector>

enum class PeerState { Free, Leader, InDHT };

struct PeerInfo {
  std::string ip_address;
  int manager_port;
  int peer_port;
  PeerState state;
};

bool dht_exists = false;
bool dht_setup_in_progress = false;

// The manager's state table, indexed by the name supplied during registration.
std::unordered_map<std::string, PeerInfo> registered_peers;

// Decode one pipe-delimited command received over UDP.
std::vector<std::string> split_message(const std::string &msg, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream tokenStream(msg);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}

// Validate a command, update manager state, and build its response message.
std::string process_manager_command(const std::string &incoming_msg) {
  std::vector<std::string> tokens = split_message(incoming_msg, '|');
  if (tokens.empty())
    return "FAILURE";

  std::string command = tokens[0];

  if (dht_setup_in_progress && command != "dht-complete") {
    std::cout << "Command rejected while DHT setup is in progress: "
              << command << "\n";
    return "FAILURE";
  }

  // --- COMMAND: register ---
  if (command == "register" && tokens.size() == 5) {
    std::string peer_name = tokens[1];
    std::string peer_ip = tokens[2];
    int m_port = std::stoi(tokens[3]);
    int p_port = std::stoi(tokens[4]);

    if (registered_peers.find(peer_name) == registered_peers.end()) {
      registered_peers[peer_name] = {peer_ip, m_port, p_port, PeerState::Free};
      std::cout << "Registered: " << peer_name << " as Free.\n";
      return "SUCCESS";
    }
    std::cout << "Registration failed: Duplicate name " << peer_name << "\n";
    return "FAILURE";
  }

  // --- COMMAND: setup-dht ---
  if (command == "setup-dht" && tokens.size() == 4) {
    std::string peer_name = tokens[1];
    int n = std::stoi(tokens[2]);
    std::string year = tokens[3];

    bool is_registered =
        registered_peers.find(peer_name) != registered_peers.end();

    if (!is_registered || n < 3 ||
      registered_peers.size() < static_cast<size_t>(n) || dht_exists) {
      std::cout << "Setup-DHT failed for " << peer_name << ".\n";
      return "FAILURE";
    }

    registered_peers[peer_name].state = PeerState::Leader;
    dht_exists = true;
    dht_setup_in_progress = true;

    std::string response = "SUCCESS|" + peer_name + "|" +
                           registered_peers[peer_name].ip_address + "|" +
                           std::to_string(registered_peers[peer_name].peer_port);

    std::vector<std::string> free_peer_names;
    for (const auto &pair : registered_peers) {
      if (pair.first != peer_name && pair.second.state == PeerState::Free)
        free_peer_names.push_back(pair.first);
    }
    std::shuffle(free_peer_names.begin(), free_peer_names.end(),
                 std::mt19937(std::random_device{}()));

    for (int i = 0; i < n - 1; ++i) {
      const std::string &selected_name = free_peer_names[i];
      PeerInfo &selected_peer = registered_peers[selected_name];
      selected_peer.state = PeerState::InDHT;
      response += "|" + selected_name + "|" + selected_peer.ip_address + "|" +
                  std::to_string(selected_peer.peer_port);
    }
    std::cout << "DHT Setup complete. Leader: " << peer_name << "\n";
    return response;
  }

  // --- COMMAND: dht-complete ---
  if (command == "dht-complete" && tokens.size() == 2) {
    std::string peer_name = tokens[1];

    auto it = registered_peers.find(peer_name);

    // Validate that the peer exists and is currently the Leader[cite: 1]
    if (dht_setup_in_progress && it != registered_peers.end() &&
      it->second.state == PeerState::Leader) {
      std::cout << "DHT Setup completed by Leader: " << peer_name << ".\n";
      dht_setup_in_progress = false;
      return "SUCCESS";
    }

    std::cout << "DHT Complete failed: " << peer_name
              << " is not the leader.\n";
    return "FAILURE";
  }

  return "FAILURE";
}

// Create and bind the UDP socket on which the manager listens.
int create_manager_socket(int manager_port) {
  int manager_socket = socket(AF_INET, SOCK_DGRAM, 0);
  if (manager_socket < 0)
    return -1;

  struct sockaddr_in manager_addr;
  std::memset(&manager_addr, 0, sizeof(manager_addr));
  manager_addr.sin_family = AF_INET;
  manager_addr.sin_addr.s_addr = INADDR_ANY;
  manager_addr.sin_port = htons(manager_port);

  if (bind(manager_socket, (const struct sockaddr *)&manager_addr,
           sizeof(manager_addr)) < 0) {
    close(manager_socket);
    return -1;
  }
  return manager_socket;
}

int main(int argCount, char *argValue[]) {
  if (argCount != 2) {
    std::cerr << "Usage: " << argValue[0] << " <manager_port>\n ";
    return EXIT_FAILURE;
  }

  try {
    int manager_port = std::stoi(argValue[1]);
    std::cout << "Manager starting on port: " << manager_port << "\n";

    int manager_socket = create_manager_socket(manager_port);
    if (manager_socket < 0) {
      std::cerr << "Error: Socket creation or bind failed.\n";
      return EXIT_FAILURE;
    }
    std::cout << "Manager successfully bound to port " << manager_port << ".\n";

    // manager runs an infinite loop listening for messages
    while (true) {
      char buffer[1024];
      struct sockaddr_in peer_addr;
      socklen_t peer_len = sizeof(peer_addr);

      // Block and wait for an incoming UDP datagram
      ssize_t bytes_received =
          recvfrom(manager_socket, (char *)buffer, sizeof(buffer) - 1,
                   MSG_WAITALL, (struct sockaddr *)&peer_addr, &peer_len);

      if (bytes_received < 0) {
        std::cerr << "Error: Failed to receive message.\n";
        continue;
      }

      buffer[bytes_received] = '\0';
      std::string incoming_msg(buffer);
      std::cout << "Received raw message: " << incoming_msg << "\n";

      // Process the command and get the response string
      std::string response = process_manager_command(incoming_msg);

      // Send SUCCESS or FAILURE back to the peer
      sendto(manager_socket, response.c_str(), response.length(), 0,
             (struct sockaddr *)&peer_addr, peer_len);
    }

  } catch (const std::exception &e) {
    std::cerr << "Error: Port must be a valid integer.\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
