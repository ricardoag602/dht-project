#include "src/manager/manager.hpp"

#include "common/network.hpp"
#include "common/protocol.hpp"

#include <cstdlib>
#include <iostream>

int main(int argCount, char *argValue[]) {
  if (argCount != 2) {
    std::cerr << "Usage: " << argValue[0] << " <manager_port>\n ";
    return EXIT_FAILURE;
  }

  std::optional<int> manager_port = dht::protocol::parse_integer(argValue[1]);
  if (!manager_port || !dht::network::is_valid_port(*manager_port)) {
    std::cerr << "Error: Port must be an integer from 1 through 65535.\n";
    return EXIT_FAILURE;
  }

  dht::manager::Manager manager(*manager_port);
  return manager.run();
}
