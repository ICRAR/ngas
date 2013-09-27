#include <cstdlib>
#include <netdb.h>
#include <fstream>
#include <iostream>
#include <cstring>
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

	status = UDT::send(fhandle, str, len, 0);
	checkUDTError(status, true, "send string info");

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
	if (argc != 7) {
	     cout << "usage: ngamsUDTSender server_ip server_port file_name file_path mime_type file_size" << endl;
	     return -1;
	}

	UDTSOCKET fhandle = getUDTSocket(argv);

	string param = string(argv[3]);
	for (int i = 4; i < argc; i++) {
		param += (udt_param_delimit + string(argv[i]));
	}

	/* Sending metadata first*/
	sendStringInfo(fhandle, param.c_str());
	cout << argv[3] << endl;
	fstream ifs(argv[3], ios::in | ios::binary);
	int64_t size = (int64_t) atol(argv[6]);
	int64_t offset = 0;
	int status = UDT::sendfile(fhandle, ifs, offset, size);
	checkUDTError(status, true, "send file");

	return 0;
}


