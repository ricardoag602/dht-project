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

// Global or main-scoped map to track peers by their name
std::unordered_map<std::string, PeerInfo> registered_peers;

std::vector<std::string> splitMessage(const std::string &msg, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream tokenStream(msg);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}

int main(int argCount, char *argValue[]) {

  if (argCount != 2) {
    std::cerr << "Usage: " << argValue[0] << " <manager_port>\n ";
    return EXIT_FAILURE;
  }

  try {
    int manager_port = std::stoi(argValue[1]);
    std::cout << "Manager starting on port: " << manager_port << "\n";

    // 1. Create socket file descriptor
    int manager_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (manager_socket < 0) {
      std::cerr << "Error: Socket creation failed.\n";
      return EXIT_FAILURE;
    }

    // 2. Configure the server address structure
    struct sockaddr_in manager_addr;
    std::memset(&manager_addr, 0,
                sizeof(manager_addr)); // Zero out the structure
    manager_addr.sin_family = AF_INET;
    manager_addr.sin_addr.s_addr = INADDR_ANY;
    manager_addr.sin_port = htons(manager_port);

    // 3. Bind the socket to the port
    if (bind(manager_socket, (const struct sockaddr *)&manager_addr,
             sizeof(manager_addr)) < 0) {
      std::cerr << "Error: Bind failed on port " << manager_port << ".\n";
      close(manager_socket);
      return EXIT_FAILURE;
    }

    std::cout << "Manager successfully bound to port " << manager_port << ".\n";

    // manager runs an infinite loop listening for messages
    while (true) {

      char buffer[1024];
      struct sockaddr_in peer_addr;
      socklen_t peer_len = sizeof(peer_addr);

      // 2. Block and wait for an incoming UDP datagram
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

      // Parse the incoming_msg based on your delimiter
      std::vector<std::string> tokens = splitMessage(incoming_msg, '|');
      std::string response = "FAILURE";

      if (!tokens.empty()) {
        std::string command = tokens[0];

        if (command == "register" && tokens.size() == 5) {
          std::string peer_name = tokens[1];
          std::string peer_ip = tokens[2];
          int m_port = std::stoi(tokens[3]);
          int p_port = std::stoi(tokens[4]);

          // Check if peer name is unique (max 15 chars assumed valid for now)
          if (registered_peers.find(peer_name) == registered_peers.end()) {
            // Save to state information base
            registered_peers[peer_name] = {peer_ip, m_port, p_port,
                                           PeerState::Free};
            response = "SUCCESS";
            std::cout << "Registered: " << peer_name << " as Free.\n";
          } else {
            std::cout << "Registration failed: Duplicate name " << peer_name
                      << "\n";
          }
        }
        // Other commands like setup-dht will go here as else-if blocks
      }

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
