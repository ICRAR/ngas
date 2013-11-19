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
#include <libgen.h>
#include "ngamsUDTUtils.h"
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

	struct addrinfo hints, *peer, *res;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	// DP: Must bind port to local socket. In this case NULL is any local interface on PORT 7790
   string service("7790");
   if (0 != getaddrinfo(NULL, service.c_str(), &hints, &res))
   {
	  cout << "illegal port number or port is busy.\n" << endl;
	  return 0;
   }

	//UDTSOCKET fhandle = UDT::socket(hints.ai_family, hints.ai_socktype, hints.ai_protocol);
	// DP: Create socket!
	UDTSOCKET fhandle = UDT::socket(res->ai_family, res->ai_socktype, res->ai_protocol);

	// check for errors!
   if (UDT::ERROR == fhandle)
   {
	cout << "socket: " << UDT::getlasterror().getErrorMessage() << endl;
		return 0;
   }

	int snd_buf = 640000;
	int rcv_buf = 640000;
	UDT::setsockopt(fhandle, 0, UDP_SNDBUF, &snd_buf, sizeof(int));
	UDT::setsockopt(fhandle, 0, UDP_RCVBUF, &rcv_buf, sizeof(int));

	// DP: Now you must BIND the socket to the port from the getaddrinfo hints!
   if (UDT::ERROR == UDT::bind(fhandle, res->ai_addr, res->ai_addrlen))
   {
	  cout << "bind: " << UDT::getlasterror().getErrorMessage() << endl;
	  return 0;
   }

   // DP: This is setting up the remote side to connect to!
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

	/*int status = UDT::send(fhandle, (char*)&len, sizeof(int), 0);
	checkUDTError(status, true, "send string length info");*/

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

void buildHTTPHeader(char* header, const char* mimeType, const char* file_name, int64_t filesize) {
	const char* ngamsUSER_AGENT = "NG/AMS UDT-CClient";
	char contentDisp[16384];
	char authHdr[512];
	sprintf(authHdr, "\015\012Authorization: Basic bmdhc21ncjpuZ2FzbWdy");
	//*authHdr = '\0';
	const char* path = "QARCHIVE";
	sprintf(contentDisp, "attachment; filename=\"%s\"; no_versioning=1", file_name);
	sprintf(header, "POST /%.256s HTTP/1.0\015\012"
			"User-agent: %s\015\012"
			"Content-type: %s\015\012"
			"Content-length: %llu\015\012"
			"Content-disposition: %s%s\015\012\012", path, ngamsUSER_AGENT, mimeType, filesize, contentDisp, authHdr);
}

int main(int argc, char* argv[]) {

	if (argc != 5 && argc != 6) {
	     cout << "usage: ngamsUDTSender server_ip server_port mime_type file_name [file_size]" << endl;
	     return -1;
	}

	string file = string(argv[4]);
	string mime = string(argv[3]);
	string param = mime + udt_param_delimit + basename(argv[4]);
	int64_t filesize = 0;

	// size passed in
	if (argc == 6) {
		param += udt_param_delimit + string(argv[4]);
		char * endptr = NULL;
		filesize = strtoimax(argv[5], &endptr, 10);
	} else {
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

	// Sending metadata first
	char header[65536];
	buildHTTPHeader(header, mime.c_str(), basename(argv[4]), filesize);
	sendStringInfo(fhandle, header);

	fstream ifs(file.c_str(), ios::in | ios::binary);
	int64_t offset = 0;
	int status = UDT::sendfile(fhandle, ifs, offset, filesize);
	ifs.close();

	checkUDTError(status, true, "send file");

	HTTPHeader respHdr;
	HTTPPayload respPay;

	// read http response from UDT recv
	status = readHTTPPacket(fhandle, &respHdr, &respPay, reliableUDTRecv);
	if (status == 0) {
		// print out response
		cout << string(respPay.buff) << endl;
		delete[] respPay.buff;
		respPay.buff = NULL;
	}
	else {
		cout << "error getting response" << endl;
		UDT::close(fhandle);
		exit(-1);
	}

	UDT::close(fhandle);

	return 0;
}


