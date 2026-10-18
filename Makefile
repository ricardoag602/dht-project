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

.DEFAULT_GOAL := all

.PHONY: all clean

all: $(MANAGER_BIN) $(PEER_BIN)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(MANAGER_BIN): $(MANAGER_SOURCES) $(MANAGER_HEADERS) $(COMMON_SOURCES) $(COMMON_HEADERS) | $(BIN_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(MANAGER_SOURCES) $(COMMON_SOURCES) -o $(MANAGER_BIN)

$(PEER_BIN): peer.cpp $(COMMON_SOURCES) $(COMMON_HEADERS) | $(BIN_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) peer.cpp $(COMMON_SOURCES) -o $(PEER_BIN)

clean:
	$(RM) $(MANAGER_BIN) $(PEER_BIN)
