CC = cc
CPPFLAGS += -D_POSIX_C_SOURCE=200809L
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -O2
LDLIBS = -pthread -lrt

.PHONY: all
all: build/elevator-unnamed

build/elevator-unnamed: simulation.c unnamed.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) $(CFLAGS) simulation.c unnamed.c -o $@ $(LDLIBS)
