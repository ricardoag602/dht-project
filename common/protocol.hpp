#ifndef DHT_COMMON_PROTOCOL_HPP
#define DHT_COMMON_PROTOCOL_HPP

#include <optional>
#include <string>
#include <vector>

namespace dht::protocol {

std::vector<std::string> split_message(const std::string &message,
                                       char delimiter = '|');

std::string join_message(const std::vector<std::string> &fields,
                         char delimiter = '|');

std::optional<int> parse_integer(const std::string &value);

} // namespace dht::protocol

#endif
