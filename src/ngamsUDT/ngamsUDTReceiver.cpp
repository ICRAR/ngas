#include <fstream>
#include <iostream>
#include <cstring>
#include <iostream>
#include <string>
#include <sstream>
#include <inttypes.h>
#include <cstdlib>
#include <netdb.h>

#include "udt.h"

using namespace std;

// prototype
void* recvFile(void*);

struct file_metadata {
    string file_name; //base name
    string file_path; //full path (including the base name)
    string mime_type; //e.g. application/octet-stream
    int64_t file_size;
};

/*int createSocket(const char* host, const int port) {
	int sockFd = 0, stat;
	struct hostent* hostRef;
	struct sockaddr_in servAddr;

	h_errno = 0;
	if ((hostRef = gethostbyname(host)) == NULL) {
		return -1;
	}

	memset((char *)&servAddr, 0, sizeof(servAddr));
	memcpy(&servAddr.sin_addr, hostRef->h_addr_list[0], hostRef->h_length);
	servAddr.sin_family = AF_INET;
	servAddr.sin_port = htons(port);

	return socket(PF_INET, SOCK_STREAM, 0);
}*/

int startUDTServer(const string& service)
{
	// use this function to initialize the UDT library
	UDT::startup();

	addrinfo hints;
	addrinfo* res;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	if (0 != getaddrinfo(NULL, service.c_str(), &hints, &res)) {
		cout << "illegal port number or port is busy.\n" << endl;
		return -1;
	}

	UDTSOCKET serv = UDT::socket(res->ai_family, res->ai_socktype, res->ai_protocol);

	int snd_buf = 64000;
	int rcv_buf = 64000;
	UDT::setsockopt(serv, 0, UDT_SNDBUF, &snd_buf, sizeof(int));
	UDT::setsockopt(serv, 0, UDT_RCVBUF, &rcv_buf, sizeof(int));
	UDT::setsockopt(serv, 0, UDP_SNDBUF, &snd_buf, sizeof(int));
	UDT::setsockopt(serv, 0, UDP_RCVBUF, &rcv_buf, sizeof(int));

	if (UDT::ERROR == UDT::bind(serv, res->ai_addr, res->ai_addrlen)) {
		cout << "bind: " << UDT::getlasterror().getErrorMessage() << endl;
		return -1;
	}

	freeaddrinfo(res);

	cout << "server is ready at port: " << service << endl;

	UDT::listen(serv, 10);

	sockaddr_storage clientaddr;
	int addrlen = sizeof(clientaddr);
	UDTSOCKET fhandle;

	while (true)
	{
		if (UDT::INVALID_SOCK == (fhandle = UDT::accept(serv, (sockaddr*)&clientaddr, &addrlen)))
		{
			 cout << "accept: " << UDT::getlasterror().getErrorMessage() << endl;
			 return -1;
		}

		char clienthost[NI_MAXHOST];
		char clientservice[NI_MAXSERV];
		getnameinfo((sockaddr *)&clientaddr, addrlen, clienthost, sizeof(clienthost), clientservice, sizeof(clientservice), NI_NUMERICHOST|NI_NUMERICSERV);
		cout << "new connection: " << clienthost << ":" << clientservice << endl;

		pthread_t filethread;
		pthread_create(&filethread, NULL, recvFile, new UDTSOCKET(fhandle));
		pthread_detach(filethread);
	}

	UDT::close(serv);

	// use this function to release the UDT library
	UDT::cleanup();

	return 0;
}


int reliableUDTRecv(UDTSOCKET u, char* buf, int len, int flags) {
	int read = 0;
	int toread = len;
	int ret = 0;

	while (read < toread) {
		ret = UDT::recv(u, buf+read, toread, flags);
		if (ret == UDT::ERROR)
			return UDT::ERROR;

		read += ret;
		toread -= ret;
	}

	return len;
}


void* recvFile(void* usocket)
{
   UDTSOCKET fhandle = *(UDTSOCKET*)usocket;
   delete (UDTSOCKET*)usocket;

   // aquiring file name information from client
   char metadata[16000];
   int len;

   memset(metadata, 0, sizeof(metadata));

   // read packet header
   if (UDT::ERROR == reliableUDTRecv(fhandle, (char*)&len, sizeof(int), 0)) {
      cout << "recv: " << UDT::getlasterror().getErrorMessage() << endl;
      return 0;
   }

   if (len > (int)sizeof(metadata)) {
	   // close connection and return
	   UDT::close(fhandle);
	   return 0;
   }

   // read metadata
   if (UDT::ERROR == reliableUDTRecv(fhandle, metadata, len, 0)) {
      cout << "recv: " << UDT::getlasterror().getErrorMessage() << endl;
      return 0;
   }

	// file_name&file_path&mime_type&file_size
	vector<string> tokens;
	char *p = strtok(metadata, "&");
	if (p != NULL)
	   tokens.push_back(string(p));

	while(p != NULL) {
	   printf("%s\n", p);
	   p = strtok(NULL, "&");
	   if (p != NULL)
		   tokens.push_back(string(p));
	}

	// error in the protocol
	if (tokens.size() != 4) {
		// close connection and return
		UDT::close(fhandle);
		return 0;
	}

	char *endptr;
	string filename = tokens[0];
	int64_t filesize = strtoimax(tokens[3].c_str(), &endptr, 10);

	// receive the file
	fstream ofs(filename.c_str(), ios::out | ios::binary | ios::trunc);
	int64_t offset = 0;
	int64_t recvsize = 0;

	if (UDT::ERROR == (recvsize = UDT::recvfile(fhandle, ofs, offset, filesize))) {
	  cout << "recvfile: " << UDT::getlasterror().getErrorMessage() << endl;
	  return 0;
	}

	UDT::close(fhandle);
	ofs.close();

	return 0;
}



int main(int argc, char *argv[])
{
	//usage: sendfile [server_port]
	if ((2 < argc) || ((2 == argc) && (0 == atoi(argv[1]))))
	{
		cout << "usage: sendfile [server_port]" << endl;
		return -1;
	}

	string service("9000");
	if (2 == argc)
		service = argv[1];

	startUDTServer(service);

	return 0;
}


