#include "src/manager/manager.hpp"

#include "common/network.hpp"
#include "common/protocol.hpp"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <random>
#include <sys/socket.h>
#include <unistd.h>

namespace dht::manager {

namespace {

constexpr std::size_t kManagerBufferSize = 1024;

std::optional<int> parse_integer(const std::string &value) {
  try {
    std::size_t parsed_characters = 0;
    int parsed_value = std::stoi(value, &parsed_characters);
    if (parsed_characters != value.size())
      return std::nullopt;
    return parsed_value;
  } catch (const std::exception &) {
    return std::nullopt;
  }
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

  if (dht_status_ == DhtStatus::Building && command != "dht-complete") {
    std::cout << "Command rejected while DHT setup is in progress: " << command
              << "\n";
    return "FAILURE";
  }

  if (command == "register")
    return handle_register(fields);
  if (command == "setup-dht")
    return handle_setup_dht(fields);
  if (command == "dht-complete")
    return handle_dht_complete(fields);

  return "FAILURE";
}

std::string Manager::handle_register(const std::vector<std::string> &fields) {
  if (fields.size() != 5)
    return "FAILURE";

  std::optional<int> manager_port = parse_integer(fields[3]);
  std::optional<int> peer_port = parse_integer(fields[4]);
  if (!manager_port || !peer_port)
    return "FAILURE";

  const std::string &peer_name = fields[1];
  if (registered_peers_.find(peer_name) != registered_peers_.end()) {
    std::cout << "Registration failed: Duplicate name " << peer_name << "\n";
    return "FAILURE";
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
  std::optional<int> ring_size = parse_integer(fields[2]);
  auto leader = registered_peers_.find(leader_name);

  if (!ring_size || *ring_size < 3 || leader == registered_peers_.end() ||
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

  std::vector<std::string> response_fields{
      "SUCCESS", leader_name, leader->second.ip_address,
      std::to_string(leader->second.peer_port)};

  for (int index = 0; index < *ring_size - 1; ++index) {
    const std::string &selected_name = free_peer_names[index];
    PeerInfo &selected_peer = registered_peers_.at(selected_name);
    selected_peer.state = PeerState::InDHT;
    response_fields.push_back(selected_name);
    response_fields.push_back(selected_peer.ip_address);
    response_fields.push_back(std::to_string(selected_peer.peer_port));
  }

  std::cout << "DHT Setup complete. Leader: " << leader_name << "\n";
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

} // namespace dht::manager
