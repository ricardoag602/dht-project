#ifndef DHT_PEER_RING_STATE_HPP
#define DHT_PEER_RING_STATE_HPP

#include <string>
#include <vector>

namespace dht::peer {

struct PeerAddress {
  std::string name;
  std::string ip_address;
  int peer_port;
};

class RingState {
public:
  bool configure(int local_id, std::vector<PeerAddress> members);
  void clear();

  bool configured() const;
  int local_id() const;
  int size() const;
  const PeerAddress *local_peer() const;
  const PeerAddress *right_neighbor() const;
  const PeerAddress *peer_by_id(int peer_id) const;
  const std::vector<PeerAddress> &members() const;

private:
  int local_id_ = -1;
  std::vector<PeerAddress> members_;
};

} // namespace dht::peer

#endif
