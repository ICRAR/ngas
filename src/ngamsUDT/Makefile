ifndef os
   os = LINUX
endif

ifndef arch
   arch = IA32
endif

CCFLAGS = -Wall -D$(os) -finline-functions -O3

ifeq ($(arch), IA32)
   CCFLAGS += -DIA32 #-mcpu=pentiumpro -march=pentiumpro -mmmx -msse
endif

ifeq ($(arch), POWERPC)
   CCFLAGS += -mcpu=powerpc
endif

ifeq ($(arch), IA64)
   CCFLAGS += -DIA64
endif

ifeq ($(arch), SPARC)
   CCFLAGS += -DSPARC
endif

LDFLAGS = -L./lib -ludt -lstdc++ -lpthread -lm

ifeq ($(os), UNIX)
   LDFLAGS += -lsocket
endif

ifeq ($(os), SUNOS)
   LDFLAGS += -lrt -lsocket
endif

DIR = $(shell pwd)

APP = ngamsUDTSender ngamsUDTReceiver ngamsUDTReceiver_Test

all: $(APP)

%.o: %.cpp
	$(CXX) $(CCFLAGS) $< -c

ngamsUDTSender: ngamsUDTSender.o ngamsUDTUtils.o
	$(CXX) $^ -o $@ $(LDFLAGS)
ngamsUDTReceiver: ngamsUDTReceiver.o ngamsUDTUtils.o
	$(CXX) $^ -o $@ $(LDFLAGS)
ngamsUDTReceiver_Test: ngamsUDTReceiver_Test.o
	$(CXX) $^ -o $@ $(LDFLAGS)


clean:
	rm -f *.o $(APP)

install:
	export PATH=$(DIR):$$PATH
