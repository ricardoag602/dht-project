#include "common/protocol.hpp"
#include "src/peer/dht_storage.hpp"
#include "src/peer/ring_state.hpp"
#include "src/peer/storm_record.hpp"

#include <cassert>
#include <iostream>
#include <vector>

namespace {

dht::peer::StormRecord make_record(int event_id) {
  return {{std::to_string(event_id), "a", "b", "c", "d", "e", "f",
           "g", "h", "i", "j", "k", "l", "m"},
          event_id};
}

void test_integer_parsing() {
  assert(dht::protocol::parse_integer("27000") == 27000);
  assert(!dht::protocol::parse_integer("27000x"));
  assert(!dht::protocol::parse_integer(""));
}

void test_csv_parsing() {
  std::optional<dht::peer::StormRecord> record =
      dht::peer::parse_storm_record(
          "42,\"alpha,beta\",c,d,e,f,g,h,i,j,k,l,m,n");
  assert(record);
  assert(record->event_id == 42);
  assert(record->fields.size() == 14);
  assert(record->fields[1] == "alpha,beta");
  assert(!dht::peer::parse_storm_record("not-an-id,a,b,c,d,e,f,g,h,i,j,k,l,m"));
}

void test_ring_state() {
  dht::peer::RingState ring;
  std::vector<dht::peer::PeerAddress> members{
      {"alpha", "127.0.0.1", 27001},
      {"beta", "127.0.0.1", 27003},
      {"gamma", "127.0.0.1", 27005}};

  assert(ring.configure(2, members));
  assert(ring.configured());
  assert(ring.local_peer()->name == "gamma");
  assert(ring.right_neighbor()->name == "alpha");
  assert(ring.peer_by_id(1)->name == "beta");
  assert(ring.peer_by_id(3) == nullptr);
  assert(ring.members().size() == 3);
  assert(!ring.configure(3, members));
  ring.clear();
  assert(!ring.configured());
  assert(ring.size() == 0);
  assert(ring.right_neighbor() == nullptr);
}

void test_collision_safe_storage() {
  dht::peer::DhtStorage storage;
  assert(storage.initialize_for_record_count(0));
  assert(storage.table_size() == 2);
  assert(storage.initialize_for_record_count(223));
  assert(storage.table_size() == 449);
  assert(storage.initialize(5));

  dht::peer::StormRecord first = make_record(1);
  dht::peer::StormRecord second = make_record(6);
  assert(storage.position_for(first.event_id) == 1);
  assert(storage.position_for(second.event_id) == 1);
  assert(storage.store(1, first) ==
         dht::peer::DhtStorage::StoreResult::Inserted);
  assert(storage.store(1, second) ==
         dht::peer::DhtStorage::StoreResult::Inserted);
  assert(storage.record_count() == 2);
  assert(storage.find(1) != nullptr);
  assert(storage.find(6) != nullptr);

  assert(storage.store(1, first) ==
         dht::peer::DhtStorage::StoreResult::AlreadyPresent);
  assert(storage.record_count() == 2);
  assert(storage.store(2, second) ==
         dht::peer::DhtStorage::StoreResult::InvalidPosition);
}

} // namespace

int main() {
  test_integer_parsing();
  test_csv_parsing();
  test_ring_state();
  test_collision_safe_storage();
  std::cout << "Peer component tests passed.\n";
  return 0;
}
