#include <arpa/inet.h> // IP address conversion function
#include <cstdlib>
#include <cstring> // For memset
#include <iostream>
#include <netinet/in.h> // Internet address structures
#include <sstream>
#include <string>
#include <sys/socket.h> // Core socket functions
#include <unistd.h>     // POSIX API access (close function)
#include <unordered_map>
#include <vector>

enum class PeerState { Free, Leader, InDHT };

struct PeerInfo {
  std::string ip;
  int m_port;
  int p_port;
  PeerState state;
};

bool dht_exists = false;

// Global map to track peers by their name
std::unordered_map<std::string, PeerInfo> registered_peers;

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

// Helper: Process commands and generate the manager's response string
std::string process_command(const std::string &incoming_msg) {
  std::vector<std::string> tokens = splitMessage(incoming_msg, '|');
  if (tokens.empty())
    return "FAILURE";

  std::string command = tokens[0];

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

    if (!is_registered || n < 3 || registered_peers.size() < n || dht_exists) {
      std::cout << "Setup-DHT failed for " << peer_name << ".\n";
      return "FAILURE";
    }

    registered_peers[peer_name].state = PeerState::Leader;
    dht_exists = true;

    std::string response = "SUCCESS|" + peer_name + "|" +
                           registered_peers[peer_name].ip + "|" +
                           std::to_string(registered_peers[peer_name].p_port);

    int peers_added = 1;
    for (auto &pair : registered_peers) {
      if (peers_added == n)
        break; // Stop once we have n peers

      if (pair.first != peer_name && pair.second.state == PeerState::Free) {
        pair.second.state = PeerState::InDHT;
        response += "|" + pair.first + "|" + pair.second.ip + "|" +
                    std::to_string(pair.second.p_port);
        peers_added++;
      }
    }
    std::cout << "DHT Setup complete. Leader: " << peer_name << "\n";
    return response;
  }

  // --- COMMAND: dht-complete ---
  if (command == "dht-complete" && tokens.size() == 2) {
    std::string peer_name = tokens[1];

    auto it = registered_peers.find(peer_name);

    // Validate that the peer exists and is currently the Leader[cite: 1]
    if (it != registered_peers.end() && it->second.state == PeerState::Leader) {
      std::cout << "DHT Setup completed by Leader: " << peer_name << ".\n";
      return "SUCCESS";
    }

    std::cout << "DHT Complete failed: " << peer_name
              << " is not the leader.\n";
    return "FAILURE";
  }

  return "FAILURE";
}

// Helper: Configure and bind the UDP socket
int setup_udp_socket(int manager_port) {
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

    int manager_socket = setup_udp_socket(manager_port);
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
      std::string response = process_command(incoming_msg);

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
