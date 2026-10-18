#include "src/manager/manager.hpp"

#include <cstdlib>
#include <iostream>

int main(int argCount, char *argValue[]) {
  if (argCount != 2) {
    std::cerr << "Usage: " << argValue[0] << " <manager_port>\n ";
    return EXIT_FAILURE;
  }

  try {
    int manager_port = std::stoi(argValue[1]);
    dht::manager::Manager manager(manager_port);
    return manager.run();
  } catch (const std::exception &) {
    std::cerr << "Error: Port must be a valid integer.\n";
    return EXIT_FAILURE;
  }
}
