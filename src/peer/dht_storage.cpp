#include "src/peer/dht_storage.hpp"

#include <limits>

namespace dht::peer {

namespace {

bool is_prime(int value) {
  if (value < 2)
    return false;
  for (int divisor = 2; divisor <= value / divisor; ++divisor) {
    if (value % divisor == 0)
      return false;
  }
  return true;
}

std::optional<int> first_prime_larger_than(std::size_t value) {
  if (value >= static_cast<std::size_t>(std::numeric_limits<int>::max()))
    return std::nullopt;

  int candidate = static_cast<int>(value) + 1;
  while (!is_prime(candidate)) {
    if (candidate == std::numeric_limits<int>::max())
      return std::nullopt;
    ++candidate;
  }
  return candidate;
}

} // namespace

bool DhtStorage::initialize_for_record_count(std::size_t record_count) {
  if (record_count >
      static_cast<std::size_t>(std::numeric_limits<int>::max() / 2)) {
    return false;
  }

  std::optional<int> new_table_size =
      first_prime_larger_than(record_count * 2);
  return new_table_size && initialize(*new_table_size);
}

bool DhtStorage::initialize(int table_size) {
  if (table_size <= 0)
    return false;

  table_size_ = table_size;
  record_count_ = 0;
  buckets_.clear();
  return true;
}

void DhtStorage::clear() {
  table_size_ = 0;
  record_count_ = 0;
  buckets_.clear();
}

bool DhtStorage::initialized() const { return table_size_ > 0; }

int DhtStorage::table_size() const { return table_size_; }

std::size_t DhtStorage::record_count() const { return record_count_; }

std::optional<int> DhtStorage::position_for(int event_id) const {
  if (!initialized() || event_id < 0)
    return std::nullopt;
  return event_id % table_size_;
}

std::optional<int> DhtStorage::owner_for(int event_id, int ring_size) const {
  std::optional<int> position = position_for(event_id);
  if (!position || ring_size <= 0)
    return std::nullopt;
  return *position % ring_size;
}

DhtStorage::StoreResult DhtStorage::store(int position,
                                          const StormRecord &record) {
  std::optional<int> expected_position = position_for(record.event_id);
  if (!expected_position || position != *expected_position)
    return StoreResult::InvalidPosition;

  std::vector<StormRecord> &bucket = buckets_[position];
  for (const StormRecord &stored_record : bucket) {
    if (stored_record.event_id == record.event_id)
      return StoreResult::AlreadyPresent;
  }

  bucket.push_back(record);
  ++record_count_;
  return StoreResult::Inserted;
}

const StormRecord *DhtStorage::find(int event_id) const {
  std::optional<int> position = position_for(event_id);
  if (!position)
    return nullptr;

  auto bucket = buckets_.find(*position);
  if (bucket == buckets_.end())
    return nullptr;

  for (const StormRecord &record : bucket->second) {
    if (record.event_id == event_id)
      return &record;
  }
  return nullptr;
}

} // namespace dht::peer
