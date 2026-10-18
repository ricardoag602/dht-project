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

} // namespace dht::protocol
