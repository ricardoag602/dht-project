#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

int main(int argCount, char *argValue[]) {

  if (argCount != 3) {
    std::cerr << "Usage: " << argValue[0] << " <manager_ipv4> <manager_port>\n";
    return EXIT_FAILURE;
  }

  try {
    std::string manager_ip = argValue[1];
    int manager_port = std::stoi(argValue[2]);

    // 1. Create the UDP socket (same as the manager)
    int peer_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (peer_socket < 0) {
      std::cerr << "Error: Socket creation failed.\n";
      return EXIT_FAILURE;
    }

    // 2. Configure the manager's destination address structure
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

    // 3. Send a test message mimicking our delimited format
    std::string test_msg = "register|Ricardo|127.0.0.1|5001|5002";
    ssize_t bytes_sent =
        sendto(peer_socket, test_msg.c_str(), test_msg.length(), 0,
               (const struct sockaddr *)&dest_addr, sizeof(dest_addr));

    if (bytes_sent < 0) {
      std::cerr << "Error: Failed to send message.\n";
    } else {
      std::cout << "Test message sent to manager!\n";
    }

    close(peer_socket);

  } catch (const std::exception &e) {
    std::cerr << "Error: Invalid arguments.\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
