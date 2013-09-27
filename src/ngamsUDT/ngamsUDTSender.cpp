#include <cstdlib>
#include <netdb.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstring>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>

#include "udt.h"

using namespace std;


string udt_param_delimit = "&";

int checkUDTError(int status, bool exitOnError, const char* contextMsg) {
	if (UDT::ERROR == status) {
		cout << contextMsg << ": " << UDT::getlasterror().getErrorMessage() << endl; // this should be logged
		int errcode = UDT::getlasterror_code();
		if (exitOnError) {
			cout << "exiting" << endl;
			exit(errcode);
		} else {
			return errcode;
		}
	} else {
		return 0;
	}
}
/* set up the UDT connection, i.e. a stream */
UDTSOCKET getUDTSocket(char* argv[]) {
	UDT::startup(); // use this function to initialize the UDT library

	struct addrinfo hints, *peer;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	UDTSOCKET fhandle = UDT::socket(hints.ai_family, hints.ai_socktype, hints.ai_protocol);


	int snd_buf = 640000;
	int rcv_buf = 640000;
	UDT::setsockopt(fhandle, 0, UDP_SNDBUF, &snd_buf, sizeof(int));
	UDT::setsockopt(fhandle, 0, UDP_RCVBUF, &rcv_buf, sizeof(int));

	if (0 != getaddrinfo(argv[1], argv[2], &hints, &peer))
	{
	  cout << "incorrect server/peer address. " << argv[1] << ":" << argv[2] << endl;
	  return NULL;
	}
	// connect to the server, implict bind
	int status = UDT::connect(fhandle, peer->ai_addr, peer->ai_addrlen);
	checkUDTError(status, true, "connect");
	freeaddrinfo(peer);
	return fhandle;
}

int sendStringInfo(UDTSOCKET fhandle, const char* str) {
	int len = strlen(str);

	int status = UDT::send(fhandle, (char*)&len, sizeof(int), 0);
	checkUDTError(status, true, "send string length info");

	int ssize = 0;
	int ss;
	while (ssize < len) {
		ss = UDT::send(fhandle, str + ssize, len - ssize, 0);
		checkUDTError(ss, true, "send string info");
		ssize += ss;
	}
	return 0;
}

/**
 *  This function is not used, as we combine all parameter to send in one go
 *  but could be useful in the future.
 */
int sendSizeInfo(UDTSOCKET fhandle, int64_t size) {
	int status = UDT::send(fhandle, (char*)&size, sizeof(int64_t), 0);
	checkUDTError(status, true, "send size info");
	return 0;
}

int main(int argc, char* argv[]) {

	if (argc != 5 && argc != 6) {
	     cout << "usage: ngamsUDTSender server_ip server_port mime_type file_name [file_size]" << endl;
	     return -1;
	}

	string file = string(argv[4]);
	string mime = string(argv[3]);
	string param = mime + udt_param_delimit + file;
	int64_t filesize = 0;

	// size passed in
	if (argc == 6) {
		param += udt_param_delimit + string(argv[4]);
		char * endptr = NULL;
		filesize = strtoimax(argv[5], &endptr, 10);
	}
	else {
		struct stat filestatus;
		if (stat(file.c_str(), &filestatus ) < 0) {
			cout << "Error getting filesize" << endl;
			return -1;
		}
		ostringstream convert;
		convert << filestatus.st_size;
		param += udt_param_delimit + convert.str();
		filesize = filestatus.st_size;
	}

	UDTSOCKET fhandle = getUDTSocket(argv);

	/* Sending metadata first*/
	sendStringInfo(fhandle, param.c_str());
	fstream ifs(file.c_str(), ios::in | ios::binary);
	int64_t size = filesize;
	int64_t offset = 0;
	int status = UDT::sendfile(fhandle, ifs, offset, size);
	UDT::close(fhandle);
	ifs.close();
	checkUDTError(status, true, "send file");

	return 0;
}


