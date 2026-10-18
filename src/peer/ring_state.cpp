#include "src/peer/ring_state.hpp"

#include "common/network.hpp"

#include <utility>

namespace dht::peer {

bool RingState::configure(int local_id, std::vector<PeerAddress> members) {
  if (members.empty() || local_id < 0 ||
      local_id >= static_cast<int>(members.size())) {
    return false;
  }

  for (const PeerAddress &member : members) {
    if (member.name.empty() || !network::is_valid_ipv4(member.ip_address) ||
        !network::is_valid_port(member.peer_port)) {
      return false;
    }
  }

  local_id_ = local_id;
  members_ = std::move(members);
  return true;
}

void RingState::clear() {
  local_id_ = -1;
  members_.clear();
}

bool RingState::configured() const { return local_id_ >= 0; }

int RingState::local_id() const { return local_id_; }

int RingState::size() const { return static_cast<int>(members_.size()); }

const PeerAddress *RingState::local_peer() const {
  return peer_by_id(local_id_);
}

const PeerAddress *RingState::right_neighbor() const {
  if (!configured())
    return nullptr;
  return peer_by_id((local_id_ + 1) % size());
}

const PeerAddress *RingState::peer_by_id(int peer_id) const {
  if (peer_id < 0 || peer_id >= size())
    return nullptr;
  return &members_[peer_id];
}

const std::vector<PeerAddress> &RingState::members() const { return members_; }

} // namespace dht::peer
