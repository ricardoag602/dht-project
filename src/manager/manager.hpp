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
  enum class DhtStatus { Absent, Building, Ready, Rebuilding, TearingDown };
  enum class RebuildKind { None, Leave, Join };

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
  std::string handle_query_dht(const std::vector<std::string> &fields);
  std::string handle_leave_dht(const std::vector<std::string> &fields);
  std::string handle_join_dht(const std::vector<std::string> &fields);
  std::string handle_dht_rebuilt(const std::vector<std::string> &fields);
  std::string handle_deregister(const std::vector<std::string> &fields);
  std::string handle_teardown_dht(const std::vector<std::string> &fields);
  std::string
  handle_teardown_complete(const std::vector<std::string> &fields);

  std::vector<std::string> prospective_members() const;
  void assign_dht_states(const std::vector<std::string> &members,
                         const std::string &leader_name);

  int port_;
  int socket_fd_ = -1;
  DhtStatus dht_status_ = DhtStatus::Absent;
  RebuildKind rebuild_kind_ = RebuildKind::None;
  std::string pending_peer_name_;
  std::string leader_name_;
  std::string dataset_year_;
  std::vector<std::string> dht_members_;
  std::unordered_map<std::string, PeerInfo> registered_peers_;
};

} // namespace dht::manager

#endif
