#include "common/network.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>

namespace dht::network {

bool is_valid_ipv4(const std::string &ip_address) {
  in_addr address;
  return inet_pton(AF_INET, ip_address.c_str(), &address) == 1;
}

bool is_valid_port(int port) { return port >= 1 && port <= 65535; }

int create_udp_socket() { return socket(AF_INET, SOCK_DGRAM, 0); }

bool bind_udp_socket(int socket_fd, int port) {
  if (socket_fd < 0 || !is_valid_port(port))
    return false;

  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  return bind(socket_fd, reinterpret_cast<const sockaddr *>(&address),
              sizeof(address)) == 0;
}

int create_bound_udp_socket(int port) {
  int socket_fd = create_udp_socket();
  if (socket_fd < 0)
    return -1;

  if (!bind_udp_socket(socket_fd, port)) {
    close(socket_fd);
    return -1;
  }

  return socket_fd;
}

bool send_udp_message(int socket_fd, const UdpEndpoint &destination,
                      const std::string &message) {
  if (socket_fd < 0 || !is_valid_port(destination.port))
    return false;

  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(destination.port);
  if (inet_pton(AF_INET, destination.ip_address.c_str(), &address.sin_addr) !=
      1) {
    return false;
  }

  return send_udp_message(socket_fd, address, sizeof(address), message);
}

bool send_udp_message(int socket_fd, const sockaddr_in &destination,
                      socklen_t destination_length,
                      const std::string &message) {
  if (socket_fd < 0)
    return false;

  ssize_t bytes_sent =
      sendto(socket_fd, message.data(), message.size(), 0,
             reinterpret_cast<const sockaddr *>(&destination),
             destination_length);
  return bytes_sent == static_cast<ssize_t>(message.size());
}

} // namespace dht::network
