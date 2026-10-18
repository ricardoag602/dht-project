#ifndef DHT_PEER_DHT_STORAGE_HPP
#define DHT_PEER_DHT_STORAGE_HPP

#include "src/peer/storm_record.hpp"

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <vector>

namespace dht::peer {

class DhtStorage {
public:
  enum class StoreResult { Inserted, AlreadyPresent, InvalidPosition };

  bool initialize_for_record_count(std::size_t record_count);
  bool initialize(int table_size);
  void clear();

  bool initialized() const;
  int table_size() const;
  std::size_t record_count() const;
  std::optional<int> position_for(int event_id) const;
  std::optional<int> owner_for(int event_id, int ring_size) const;
  StoreResult store(int position, const StormRecord &record);
  const StormRecord *find(int event_id) const;

private:
  int table_size_ = 0;
  std::size_t record_count_ = 0;

  // Separate chaining preserves records when event IDs hash to the same slot.
  std::unordered_map<int, std::vector<StormRecord>> buckets_;
};

} // namespace dht::peer

#endif
