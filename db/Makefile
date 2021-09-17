CC=g++
CFLAGS:=-std=c++11 -c -g -Wall -fgnu-tm -DON_DCPMM -faligned-new 
# INCLUDES=  -I../ -I../lib/ 
INCLUDES=-I../ -I../lib/pmem-rocksdb/include 
# SOURCES=$(wildcard *.cc)
SOURCES=$(wildcard pmem_rocksdb_db.cc roart_db.cc) 
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

