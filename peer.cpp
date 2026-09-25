#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
int main(int argCount, char *argValue[]) {

  if (argCount != 3) {
    std::cerr << "Usage: " << argValue[0] << " <manager_ipv4> <manager_port>\n";
    return EXIT_FAILURE;
  }

  try {
    std::string manager_ip = argValue[1];
    int manager_port = std::stoi(argValue[2]);

    std::cout << "Peer started. Manager at " << manager_ip << ":"
              << manager_port << "\n";

    // TODO: Peer UDP socket setup and stdin command loop will go here
  } catch (const std::exception &e) {
    std::cerr << "Error: Manager port must be a valid integer.\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
