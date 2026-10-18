#ifndef DHT_MANAGER_MANAGER_HPP
#define DHT_MANAGER_MANAGER_HPP

#include <string>
#include <unordered_map>
#include <vector>

namespace dht::manager {

class Manager {
public:
  explicit Manager(int port);
  ~Manager();

  Manager(const Manager &) = delete;
  Manager &operator=(const Manager &) = delete;

  int run();

private:
  enum class PeerState { Free, Leader, InDHT };
  enum class DhtStatus { Absent, Building, Ready };

  struct PeerInfo {
    std::string ip_address;
    int manager_port;
    int peer_port;
    PeerState state;
  };

  std::string process_command(const std::string &message);
  std::string handle_register(const std::vector<std::string> &fields);
  std::string handle_setup_dht(const std::vector<std::string> &fields);
  std::string handle_dht_complete(const std::vector<std::string> &fields);

  int port_;
  int socket_fd_ = -1;
  DhtStatus dht_status_ = DhtStatus::Absent;
  std::unordered_map<std::string, PeerInfo> registered_peers_;
};

} // namespace dht::manager

#endif
