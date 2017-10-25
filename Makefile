CC= g++
CPPFLAGS= -Wextra -Wall -g -std=c++11 -pthread
HEADERS= MRFCore.h MapReduceClient.h MapReduceFramework.h SearchMRC.h
TAR_FILES= MRFCore.cpp MRFCore.h MapReduceFramework.cpp SearchMRC.cpp SearchMRC.h Search.cpp

all: Search libMapReduceFramework.a

MapReduceFramework.a: libMapReduceFramework.a

libMapReduceFramework.a: MapReduceFramework.o MRFCore.o
	ar rcs MapReduceFramework.a $^

Search: Search.o SearchMRC.o MapReduceFramework.o MRFCore.o
	$(CC) $(CPPFLAGS) $^ -o $@

Test1: test1.o MapReduceFramework.a
	$(CC) $(CPPFLAGS) $^ -o $@

Test2: test2.o MapReduceFramework.a
	$(CC) $(CPPFLAGS) $^ -o $@ 

Test3: test3.o MapReduceFramework.a
	$(CC) $(CPPFLAGS) $^ -o $@

Test4: test.o MapReduceFramework.a
	$(CC) $(CPPFLAGS) $^ -o $@

%.o: %.cpp $(HEADERS)
	$(CC) $(CPPFLAGS) -c $<

tar:
	tar -cvf ex3.tar $(TAR_FILES) README Makefile

clean:
	rm -rf *.o Search libMapReduceFramework.a MapReduceFramework.a

.PHONY: clean all tar
