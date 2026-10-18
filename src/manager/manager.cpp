#include "src/manager/manager.hpp"

#include "common/network.hpp"
#include "common/protocol.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <random>
#include <sys/socket.h>
#include <unistd.h>

namespace dht::manager {

namespace {

constexpr std::size_t kManagerBufferSize = 65536;

bool is_valid_peer_name(const std::string &name) {
  return !name.empty() && name.size() <= 15 &&
         std::all_of(name.begin(), name.end(), [](unsigned char character) {
           return std::isalpha(character) != 0;
         });
}

} // namespace

Manager::Manager(int port) : port_(port) {}

Manager::~Manager() {
  if (socket_fd_ >= 0)
    close(socket_fd_);
}

int Manager::run() {
  std::cout << "Manager starting on port: " << port_ << "\n";

  socket_fd_ = network::create_bound_udp_socket(port_);
  if (socket_fd_ < 0) {
    std::cerr << "Error: Socket creation or bind failed.\n";
    return EXIT_FAILURE;
  }

  std::cout << "Manager successfully bound to port " << port_ << ".\n";

  while (true) {
    char buffer[kManagerBufferSize];
    sockaddr_in peer_address;
    socklen_t peer_address_length = sizeof(peer_address);

    ssize_t bytes_received =
        recvfrom(socket_fd_, buffer, sizeof(buffer) - 1, MSG_WAITALL,
                 reinterpret_cast<sockaddr *>(&peer_address),
                 &peer_address_length);

    if (bytes_received < 0) {
      std::cerr << "Error: Failed to receive message.\n";
      continue;
    }

    buffer[bytes_received] = '\0';
    std::string incoming_message(buffer);
    std::cout << "Received raw message: " << incoming_message << "\n";

    std::string response = process_command(incoming_message);
    if (!network::send_udp_message(socket_fd_, peer_address,
                                   peer_address_length, response)) {
      std::cerr << "Error: Failed to send response.\n";
    }
  }
}

std::string Manager::process_command(const std::string &message) {
  std::vector<std::string> fields = protocol::split_message(message);
  if (fields.empty())
    return "FAILURE";

  const std::string &command = fields[0];

  const bool command_allowed_during_transition =
      (dht_status_ == DhtStatus::Building && command == "dht-complete") ||
      (dht_status_ == DhtStatus::Rebuilding && command == "dht-rebuilt") ||
      (dht_status_ == DhtStatus::TearingDown &&
       command == "teardown-complete");
  if ((dht_status_ == DhtStatus::Building ||
       dht_status_ == DhtStatus::Rebuilding ||
       dht_status_ == DhtStatus::TearingDown) &&
      !command_allowed_during_transition) {
    std::cout << "Command rejected while a DHT transition is in progress: "
              << command << "\n";
    return "FAILURE";
  }

  if (command == "register")
    return handle_register(fields);
  if (command == "setup-dht")
    return handle_setup_dht(fields);
  if (command == "dht-complete")
    return handle_dht_complete(fields);
  if (command == "query-dht")
    return handle_query_dht(fields);
  if (command == "leave-dht")
    return handle_leave_dht(fields);
  if (command == "join-dht")
    return handle_join_dht(fields);
  if (command == "dht-rebuilt")
    return handle_dht_rebuilt(fields);
  if (command == "deregister")
    return handle_deregister(fields);
  if (command == "teardown-dht")
    return handle_teardown_dht(fields);
  if (command == "teardown-complete")
    return handle_teardown_complete(fields);

  return "FAILURE";
}

std::string Manager::handle_register(const std::vector<std::string> &fields) {
  if (fields.size() != 5)
    return "FAILURE";

  std::optional<int> manager_port = protocol::parse_integer(fields[3]);
  std::optional<int> peer_port = protocol::parse_integer(fields[4]);
  if (!manager_port || !peer_port ||
      !network::is_valid_port(*manager_port) ||
      !network::is_valid_port(*peer_port) || *manager_port == *peer_port ||
      !network::is_valid_ipv4(fields[2]) ||
      !is_valid_peer_name(fields[1])) {
    return "FAILURE";
  }

  const std::string &peer_name = fields[1];
  if (registered_peers_.find(peer_name) != registered_peers_.end()) {
    std::cout << "Registration failed: Duplicate name " << peer_name << "\n";
    return "FAILURE";
  }

  for (const auto &entry : registered_peers_) {
    const PeerInfo &registered_peer = entry.second;
    if (*manager_port == registered_peer.manager_port ||
        *manager_port == registered_peer.peer_port ||
        *peer_port == registered_peer.manager_port ||
        *peer_port == registered_peer.peer_port) {
      std::cout << "Registration failed: Port already registered.\n";
      return "FAILURE";
    }
  }

  registered_peers_[peer_name] = {
      fields[2], *manager_port, *peer_port, PeerState::Free};
  std::cout << "Registered: " << peer_name << " as Free.\n";
  return "SUCCESS";
}

std::string
Manager::handle_setup_dht(const std::vector<std::string> &fields) {
  if (fields.size() != 4)
    return "FAILURE";

  const std::string &leader_name = fields[1];
  std::optional<int> ring_size = protocol::parse_integer(fields[2]);
  std::optional<int> year = protocol::parse_integer(fields[3]);
  auto leader = registered_peers_.find(leader_name);

  if (!ring_size || !year || *ring_size < 3 || *year < 1950 || *year > 2019 ||
      leader == registered_peers_.end() ||
      leader->second.state != PeerState::Free ||
      registered_peers_.size() < static_cast<std::size_t>(*ring_size) ||
      dht_status_ != DhtStatus::Absent) {
    std::cout << "Setup-DHT failed for " << leader_name << ".\n";
    return "FAILURE";
  }

  std::vector<std::string> free_peer_names;
  for (const auto &[name, peer] : registered_peers_) {
    if (name != leader_name && peer.state == PeerState::Free)
      free_peer_names.push_back(name);
  }

  if (free_peer_names.size() < static_cast<std::size_t>(*ring_size - 1)) {
    std::cout << "Setup-DHT failed for " << leader_name << ".\n";
    return "FAILURE";
  }

  std::shuffle(free_peer_names.begin(), free_peer_names.end(),
               std::mt19937(std::random_device{}()));

  leader->second.state = PeerState::Leader;
  dht_status_ = DhtStatus::Building;
  leader_name_ = leader_name;
  dataset_year_ = fields[3];
  dht_members_.clear();
  dht_members_.push_back(leader_name);

  std::vector<std::string> response_fields{
      "SUCCESS", leader_name, leader->second.ip_address,
      std::to_string(leader->second.peer_port)};

  for (int index = 0; index < *ring_size - 1; ++index) {
    const std::string &selected_name = free_peer_names[index];
    PeerInfo &selected_peer = registered_peers_.at(selected_name);
    selected_peer.state = PeerState::InDHT;
    dht_members_.push_back(selected_name);
    response_fields.push_back(selected_name);
    response_fields.push_back(selected_peer.ip_address);
    response_fields.push_back(std::to_string(selected_peer.peer_port));
  }

  std::cout << "DHT construction started. Leader: " << leader_name << "\n";
  return protocol::join_message(response_fields);
}

std::string
Manager::handle_dht_complete(const std::vector<std::string> &fields) {
  if (fields.size() != 2)
    return "FAILURE";

  const std::string &peer_name = fields[1];
  auto peer = registered_peers_.find(peer_name);

  if (dht_status_ == DhtStatus::Building &&
      peer != registered_peers_.end() &&
      peer->second.state == PeerState::Leader) {
    dht_status_ = DhtStatus::Ready;
    std::cout << "DHT Setup completed by Leader: " << peer_name << ".\n";
    return "SUCCESS";
  }

  std::cout << "DHT Complete failed: " << peer_name
            << " is not the leader.\n";
  return "FAILURE";
}

std::string
Manager::handle_query_dht(const std::vector<std::string> &fields) {
  if (fields.size() != 2 || dht_status_ != DhtStatus::Ready ||
      dht_members_.empty()) {
    return "FAILURE";
  }

  auto requester = registered_peers_.find(fields[1]);
  if (requester == registered_peers_.end() ||
      requester->second.state != PeerState::Free) {
    return "FAILURE";
  }

  std::uniform_int_distribution<std::size_t> distribution(
      0, dht_members_.size() - 1);
  std::mt19937 generator(std::random_device{}());
  const std::string &selected_name = dht_members_[distribution(generator)];
  const PeerInfo &selected = registered_peers_.at(selected_name);
  return protocol::join_message({"SUCCESS", selected_name,
                                 selected.ip_address,
                                 std::to_string(selected.peer_port)});
}

std::string
Manager::handle_leave_dht(const std::vector<std::string> &fields) {
  if (fields.size() != 2 || dht_status_ != DhtStatus::Ready ||
      dht_members_.size() <= 1) {
    return "FAILURE";
  }

  auto peer = registered_peers_.find(fields[1]);
  if (peer == registered_peers_.end() ||
      (peer->second.state != PeerState::Leader &&
       peer->second.state != PeerState::InDHT)) {
    return "FAILURE";
  }

  pending_peer_name_ = fields[1];
  rebuild_kind_ = RebuildKind::Leave;
  dht_status_ = DhtStatus::Rebuilding;
  std::cout << "DHT leave started by " << pending_peer_name_ << ".\n";
  return protocol::join_message({"SUCCESS", dataset_year_});
}

std::string Manager::handle_join_dht(const std::vector<std::string> &fields) {
  if (fields.size() != 2 || dht_status_ != DhtStatus::Ready ||
      dht_members_.empty()) {
    return "FAILURE";
  }

  auto peer = registered_peers_.find(fields[1]);
  if (peer == registered_peers_.end() ||
      peer->second.state != PeerState::Free) {
    return "FAILURE";
  }

  pending_peer_name_ = fields[1];
  rebuild_kind_ = RebuildKind::Join;
  dht_status_ = DhtStatus::Rebuilding;

  std::vector<std::string> response{"SUCCESS", dataset_year_,
                                    std::to_string(dht_members_.size())};
  for (const std::string &member_name : dht_members_) {
    const PeerInfo &member = registered_peers_.at(member_name);
    response.push_back(member_name);
    response.push_back(member.ip_address);
    response.push_back(std::to_string(member.peer_port));
  }

  std::cout << "DHT join started by " << pending_peer_name_ << ".\n";
  return protocol::join_message(response);
}

std::vector<std::string> Manager::prospective_members() const {
  std::vector<std::string> members = dht_members_;
  if (rebuild_kind_ == RebuildKind::Leave) {
    members.erase(std::remove(members.begin(), members.end(),
                              pending_peer_name_),
                  members.end());
  } else if (rebuild_kind_ == RebuildKind::Join) {
    members.push_back(pending_peer_name_);
  }
  return members;
}

void Manager::assign_dht_states(const std::vector<std::string> &members,
                                const std::string &leader_name) {
  for (auto &[name, peer] : registered_peers_) {
    if (std::find(members.begin(), members.end(), name) == members.end()) {
      if (peer.state == PeerState::Leader || peer.state == PeerState::InDHT)
        peer.state = PeerState::Free;
      continue;
    }
    peer.state = name == leader_name ? PeerState::Leader : PeerState::InDHT;
  }
}

std::string
Manager::handle_dht_rebuilt(const std::vector<std::string> &fields) {
  if (fields.size() != 3 || dht_status_ != DhtStatus::Rebuilding ||
      rebuild_kind_ == RebuildKind::None || fields[1] != pending_peer_name_) {
    return "FAILURE";
  }

  std::vector<std::string> members = prospective_members();
  if (members.empty() ||
      std::find(members.begin(), members.end(), fields[2]) == members.end()) {
    return "FAILURE";
  }

  if (rebuild_kind_ == RebuildKind::Leave) {
    auto leaving =
        std::find(dht_members_.begin(), dht_members_.end(), pending_peer_name_);
    if (leaving == dht_members_.end())
      return "FAILURE";
    std::size_t leaving_index =
        static_cast<std::size_t>(leaving - dht_members_.begin());
    const std::string &expected_leader =
        dht_members_[(leaving_index + 1) % dht_members_.size()];
    if (fields[2] != expected_leader)
      return "FAILURE";
  } else if (fields[2] != leader_name_) {
    return "FAILURE";
  }

  auto leader_position = std::find(members.begin(), members.end(), fields[2]);
  std::rotate(members.begin(), leader_position, members.end());
  assign_dht_states(members, fields[2]);
  dht_members_ = std::move(members);
  leader_name_ = fields[2];
  pending_peer_name_.clear();
  rebuild_kind_ = RebuildKind::None;
  dht_status_ = DhtStatus::Ready;
  std::cout << "DHT rebuild completed. Leader: " << leader_name_ << ".\n";
  return "SUCCESS";
}

std::string
Manager::handle_deregister(const std::vector<std::string> &fields) {
  if (fields.size() != 2)
    return "FAILURE";

  auto peer = registered_peers_.find(fields[1]);
  if (peer == registered_peers_.end() ||
      peer->second.state != PeerState::Free) {
    return "FAILURE";
  }

  registered_peers_.erase(peer);
  std::cout << "Deregistered peer " << fields[1] << ".\n";
  return "SUCCESS";
}

std::string
Manager::handle_teardown_dht(const std::vector<std::string> &fields) {
  if (fields.size() != 2 || dht_status_ != DhtStatus::Ready ||
      fields[1] != leader_name_) {
    return "FAILURE";
  }

  dht_status_ = DhtStatus::TearingDown;
  std::cout << "DHT teardown started by " << leader_name_ << ".\n";
  return "SUCCESS";
}

std::string
Manager::handle_teardown_complete(const std::vector<std::string> &fields) {
  if (fields.size() != 2 || dht_status_ != DhtStatus::TearingDown ||
      fields[1] != leader_name_) {
    return "FAILURE";
  }

  assign_dht_states({}, "");
  dht_members_.clear();
  leader_name_.clear();
  dataset_year_.clear();
  pending_peer_name_.clear();
  rebuild_kind_ = RebuildKind::None;
  dht_status_ = DhtStatus::Absent;
  std::cout << "DHT teardown completed.\n";
  return "SUCCESS";
}

} // namespace dht::manager
