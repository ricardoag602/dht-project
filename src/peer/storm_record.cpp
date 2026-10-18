#include "src/peer/storm_record.hpp"

#include "common/protocol.hpp"

#include <fstream>
#include <utility>

namespace dht::peer {

std::vector<std::string> parse_csv_line(const std::string &line) {
  std::vector<std::string> fields;
  std::string field;
  bool quoted = false;

  for (std::size_t index = 0; index < line.size(); ++index) {
    char current = line[index];
    if (current == '"') {
      if (quoted && index + 1 < line.size() && line[index + 1] == '"') {
        field += '"';
        ++index;
      } else {
        quoted = !quoted;
      }
    } else if (current == ',' && !quoted) {
      fields.push_back(field);
      field.clear();
    } else {
      field += current;
    }
  }

  fields.push_back(field);
  return fields;
}

std::optional<StormRecord> parse_storm_record(const std::string &line) {
  std::vector<std::string> fields = parse_csv_line(line);
  if (fields.size() != kStormFieldCount)
    return std::nullopt;

  std::optional<int> event_id = protocol::parse_integer(fields[0]);
  if (!event_id || *event_id < 0)
    return std::nullopt;

  return StormRecord{std::move(fields), *event_id};
}

bool load_storm_records(const std::string &path,
                        std::vector<StormRecord> &records) {
  std::ifstream input(path);
  if (!input)
    return false;

  std::vector<StormRecord> loaded_records;
  std::string line;
  if (!std::getline(input, line))
    return false;

  while (std::getline(input, line)) {
    if (line.empty())
      continue;

    std::optional<StormRecord> record = parse_storm_record(line);
    if (!record)
      return false;
    loaded_records.push_back(std::move(*record));
  }

  records = std::move(loaded_records);
  return true;
}

} // namespace dht::peer
