#ifndef DHT_PEER_STORM_RECORD_HPP
#define DHT_PEER_STORM_RECORD_HPP

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

namespace dht::peer {

inline constexpr std::size_t kStormFieldCount = 14;

struct StormRecord {
  std::vector<std::string> fields;
  int event_id;
};

std::vector<std::string> parse_csv_line(const std::string &line);
std::optional<StormRecord> parse_storm_record(const std::string &line);
bool load_storm_records(const std::string &path,
                        std::vector<StormRecord> &records);

} // namespace dht::peer

#endif
