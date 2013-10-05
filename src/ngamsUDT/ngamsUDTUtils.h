#ifndef __NGAMSUDTUTILS_H__
#define __NGAMSUDTUTILS_H__

#include <map>
#include "udt.h"

using namespace std;

// http structure
typedef struct HTTPHeader {
   string status;
   map<string, string> vals;
} HTTPHeader;

typedef struct HTTPPayload {
	int64_t payloadsize;
	char* buff;
};

typedef struct SockeThrdArgs {
	string ngas_host;
	int ngas_port;
	UDTSOCKET fhandle;
};

// connect to a remote tcp socket
int connect(const char* host, const int port);

int reliableTCPWrite(int fd, const char* buf, int len);
int reliableTCPRecv(int u, char* buf, int len);
int reliableUDTWrite(int u, const char* buf, int len);
int reliableUDTRecv(int u, char* buf, int len);

// read a line deliminated by \n from a stream. recvfunc is any function with the below signature.
int readline(int fd, string& line, unsigned int maxlen, int (*recvfunc)(int, char*, int));
int HTTPHeaderToString(const HTTPHeader* hdr, string& str);
int readHTTPHeader(int fd, HTTPHeader* hdr, int (*recvfunc)(int, char*, int));

// read/write entire http packet
int readHTTPPacket(int fd, HTTPHeader* hdr, HTTPPayload* payload, int (*recvfunc)(int, char*, int));
int writeHTTPPacket(int fd, const HTTPHeader* hdr, const HTTPPayload* payload, int (*writefunc)(int, const char*, int));

#endif
