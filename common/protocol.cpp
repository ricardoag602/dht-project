#include "common/protocol.hpp"

#include <sstream>

namespace dht::protocol {

std::vector<std::string> split_message(const std::string &message,
                                       char delimiter) {
  std::vector<std::string> fields;
  std::string field;
  std::istringstream stream(message);

  while (std::getline(stream, field, delimiter))
    fields.push_back(field);

  if (!message.empty() && message.back() == delimiter)
    fields.push_back("");

  return fields;
}

std::string join_message(const std::vector<std::string> &fields,
                         char delimiter) {
  std::string message;

  for (std::size_t index = 0; index < fields.size(); ++index) {
    if (index != 0)
      message += delimiter;
    message += fields[index];
  }

  return message;
}

} // namespace dht::protocol
