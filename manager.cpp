#include <arpa/inet.h> // IP address conversion function
#include <cstdlib>
#include <cstring> // For memset
#include <iostream>
#include <netinet/in.h> // Internet address structures
#include <string>
#include <sys/socket.h> // Core socket functions
#include <unistd.h>     // POSIX API access (close function)

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

      // TODO: Parse the incoming_msg based on your delimiter
      // TODO: Execute the requested comman (register, setup-dht, etc.)
      // TODO: Send SUCCESS or FAILURE back to the peer using sendto()
    }

  } catch (const std::exception &e) {
    std::cerr << "Error: Port must be a valid integer.\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
