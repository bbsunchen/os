CC=g++
#CFLAGS= -std=c++11 -Wall -pthread
CFLAGS= -std=c++0x -g -pthread -lrt


all: metadata client1 client2 client3 server
	@echo "compiling complete"

metadata: metadata.cc pck_util.cc connecter.cc
	$(CC) $(CFLAGS) -o $@ $^
	@echo "metadata compiled"

	$(CC) $(CFLAGS) -o $@ $^
client1: client1.cc client-cache.cc  connecter.cc pck_util.cc
	@echo "client1 compiled"

client2: client2.cc client-cache.cc  connecter.cc pck_util.cc
	$(CC) $(CFLAGS) -o $@ $^
	@echo "client2 compiled"
client3: client3.cc client-cache.cc  connecter.cc pck_util.cc
	$(CC) $(CFLAGS) -o $@ $^
	@echo "client3 compiled"
server: server.cc connecter.cc pck_util.cc
	$(CC) $(CFLAGS) -o $@ $^
	@echo "server compiled"

clean:
	rm metadata client1 client2 client3 server
