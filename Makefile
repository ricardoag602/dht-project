CXX ?= c++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -pedantic
CPPFLAGS ?= -I.

BIN_DIR ?= .

MANAGER_BIN := $(BIN_DIR)/manager
PEER_BIN := $(BIN_DIR)/peer
COMMON_SOURCES := common/network.cpp common/protocol.cpp
COMMON_HEADERS := common/network.hpp common/protocol.hpp
MANAGER_SOURCES := manager.cpp src/manager/manager.cpp
MANAGER_HEADERS := src/manager/manager.hpp
PEER_COMPONENT_SOURCES := src/peer/dht_storage.cpp \
	src/peer/ring_state.cpp \
	src/peer/storm_record.cpp
PEER_COMPONENT_HEADERS := src/peer/dht_storage.hpp \
	src/peer/ring_state.hpp \
	src/peer/storm_record.hpp
PEER_SOURCES := peer.cpp $(PEER_COMPONENT_SOURCES)
TEST_BIN := $(BIN_DIR)/peer_components_test
TEST_SOURCES := tests/peer_components_test.cpp $(PEER_COMPONENT_SOURCES)

.DEFAULT_GOAL := all

.PHONY: all clean test manager-test integration-test

all: $(MANAGER_BIN) $(PEER_BIN)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(MANAGER_BIN): $(MANAGER_SOURCES) $(MANAGER_HEADERS) $(COMMON_SOURCES) $(COMMON_HEADERS) | $(BIN_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(MANAGER_SOURCES) $(COMMON_SOURCES) -o $(MANAGER_BIN)

$(PEER_BIN): $(PEER_SOURCES) $(PEER_COMPONENT_HEADERS) $(COMMON_SOURCES) $(COMMON_HEADERS) | $(BIN_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(PEER_SOURCES) $(COMMON_SOURCES) -o $(PEER_BIN)

$(TEST_BIN): $(TEST_SOURCES) $(PEER_COMPONENT_HEADERS) $(COMMON_SOURCES) $(COMMON_HEADERS) | $(BIN_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(TEST_SOURCES) $(COMMON_SOURCES) -o $(TEST_BIN)

test: $(TEST_BIN)
	$(TEST_BIN)

manager-test: $(MANAGER_BIN)
	python3 tests/manager_protocol_test.py

integration-test: all
	python3 tests/dht_integration_test.py

clean:
	$(RM) $(MANAGER_BIN) $(PEER_BIN) $(TEST_BIN)
