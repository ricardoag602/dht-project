#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

int main(int argCount, char *argValue[]) {

  // 1. Validate command-line arguments
  if (argCount != 3) {
    std::cerr << "Usage: " << argValue[0] << " <manager_ipv4> <manager_port>\n";
    return EXIT_FAILURE;
  }

  try {
    std::string manager_ip = argValue[1];
    int manager_port = std::stoi(argValue[2]);

    // 2. Create the UDP socket (same as the manager)
    int peer_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (peer_socket < 0) {
      std::cerr << "Error: Socket creation failed.\n";
      return EXIT_FAILURE;
    }

    // 3. Configure the manager's destination address structure
    struct sockaddr_in dest_addr;
    std::memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(manager_port);

    // Convert the IPv4 string into a binary network address
    if (inet_pton(AF_INET, manager_ip.c_str(), &dest_addr.sin_addr) <= 0) {
      std::cerr << "Error: Invalid manager IP address.\n";
      close(peer_socket);
      return EXIT_FAILURE;
    }

    std::cout << "Peer started. Manager at " << manager_ip << ":"
              << manager_port << "\n";
    std::cout << "Enter commands (e.g., <name> <ip> <m-port> <p-port>\n> ";

    std::string input_line;

    // 4. Interactive loop reading from stdin
    while (std::getline(std::cin, input_line)) {
      if (input_line.empty()) {
        std::cout << "> ";
        continue;
      }

      // Convert space-separated user input into the pipe-delimited wire
      // protocol
      std::istringstream iss(input_line);
      std::string token;
      std::string wire_msg = "";
      bool first = true;

      while (iss >> token) {
        if (!first)
          wire_msg += "|";
        wire_msg += token;
        first = false;
      }

      // 5. Send the formatted message to the manager
      ssize_t bytes_sent =
          sendto(peer_socket, wire_msg.c_str(), wire_msg.length(), 0,
                 (const struct sockaddr *)&dest_addr, sizeof(dest_addr));

      if (bytes_sent < 0) {
        std::cerr << "error: Failed to send message.\n";
      } else {
        // 6. Block & wait for the manager's pair response
        char recv_buffer[1024];
        struct sockaddr_in from_addr;
        socklen_t from_len = sizeof(from_addr);

        ssize_t bytes_received =
            recvfrom(peer_socket, (char *)recv_buffer, sizeof(recv_buffer) - 1,
                     MSG_WAITALL, (struct sockaddr *)&from_addr, &from_len);

        if (bytes_received > 0) {
          recv_buffer[bytes_received] = '\0';
          std::cout << "Manager replied: " << recv_buffer << "\n";
        } else {
          std::cerr << "Error: Failed to receive response.\n";
        }
        std::cout << "> ";
      }
    }

    close(peer_socket);
  } catch (const std::exception &e) {
    std::cerr << "Error: Invalid arguments.\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
