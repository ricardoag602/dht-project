#ifndef DHT_COMMON_PROTOCOL_HPP
#define DHT_COMMON_PROTOCOL_HPP

#include <string>
#include <vector>

namespace dht::protocol {

std::vector<std::string> split_message(const std::string &message,
                                       char delimiter = '|');

std::string join_message(const std::vector<std::string> &fields,
                         char delimiter = '|');

} // namespace dht::protocol

#endif
