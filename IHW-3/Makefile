CC = cc
CPPFLAGS += -D_POSIX_C_SOURCE=200809L
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -O2
LDLIBS = -pthread -lrt

.PHONY: all check
all: build/elevator-unnamed build/elevator-named build/elevator-server build/elevator-client \
     build/elevator-mq-server build/elevator-mq-client

build/elevator-unnamed: simulation.c processes.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) $(CFLAGS) simulation.c processes.c -o $@ $(LDLIBS)

build/elevator-named: simulation.c processes.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) -DNAMED_SEMAPHORES $(CFLAGS) simulation.c processes.c -o $@ $(LDLIBS)

build/elevator-server: simulation.c processes.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) -DNAMED_SEMAPHORES -DINDEPENDENT $(CFLAGS) simulation.c processes.c -o $@ $(LDLIBS)

build/elevator-client: simulation.c processes.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) -DNAMED_SEMAPHORES -DINDEPENDENT -DCLIENT $(CFLAGS) simulation.c processes.c -o $@ $(LDLIBS)

build/elevator-mq-server: simulation.c processes.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) -DNAMED_SEMAPHORES -DINDEPENDENT -DMESSAGE_QUEUES $(CFLAGS) simulation.c processes.c -o $@ $(LDLIBS)

build/elevator-mq-client: simulation.c processes.c simulation.h
	mkdir -p build
	$(CC) $(CPPFLAGS) -DNAMED_SEMAPHORES -DINDEPENDENT -DMESSAGE_QUEUES -DCLIENT $(CFLAGS) simulation.c processes.c -o $@ $(LDLIBS)

check: all
	python3 test.py
