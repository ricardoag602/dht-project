#ifndef DHT_COMMON_NETWORK_HPP
#define DHT_COMMON_NETWORK_HPP

#include <netinet/in.h>
#include <string>
#include <sys/socket.h>

namespace dht::network {

struct UdpEndpoint {
  std::string ip_address;
  int port;
};

bool is_valid_ipv4(const std::string &ip_address);
bool is_valid_port(int port);

int create_udp_socket();
bool bind_udp_socket(int socket_fd, int port);
int create_bound_udp_socket(int port);

bool send_udp_message(int socket_fd, const UdpEndpoint &destination,
                      const std::string &message);

bool send_udp_message(int socket_fd, const sockaddr_in &destination,
                      socklen_t destination_length,
                      const std::string &message);

} // namespace dht::network

#endif
