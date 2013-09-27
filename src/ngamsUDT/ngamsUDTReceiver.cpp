#include <fstream>
#include <iostream>
#include <cstring>
#include <iostream>
#include <string>
#include <sstream>
#include <inttypes.h>
#include <cstdlib>
#include <netdb.h>
#include <map>
#include <algorithm>
#include <errno.h>

#include "udt.h"

#define BUFFSIZE 64000
#define MAXLINE 16000

using namespace std;

// prototype
void* recvFile(void*);

int connect(const char* host, const int port) {

	struct hostent* hostRef;
	struct sockaddr_in servAddr;

	if ((hostRef = gethostbyname(host)) == NULL)
		return -1;

	memset((char *)&servAddr, 0, sizeof(servAddr));
	memcpy(&servAddr.sin_addr, hostRef->h_addr_list[0], hostRef->h_length);
	servAddr.sin_family = AF_INET;
	servAddr.sin_port = htons(port);

	int fd = socket(PF_INET, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;

	int ret = connect(fd, (struct sockaddr*)&servAddr, sizeof(struct sockaddr_in));
	if (ret < 0) {
		close(fd);
		return -1;
	}

	return fd;
}


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

	int snd_buf = 640000;
	int rcv_buf = 640000;
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

int reliableTCPWrite(int fd, const char* buf, int len) {
	int towrite = len;
	int written = 0;
	int ret = 0;

	while (written < towrite) {
		ret = write(fd, buf+written, towrite);
		if (ret <= 0)
			return ret;

		written += ret;
		towrite -= ret;
	}

	return len;
}

int reliableUDTRecv(int u, char* buf, int len) {
	int read = 0;
	int toread = len;
	int ret = 0;

	while (read < toread) {
		ret = UDT::recv(u, buf+read, toread, 0);
		if (ret == UDT::ERROR)
			return UDT::ERROR;

		read += ret;
		toread -= ret;
	}

	return len;
}

int reliableTCPRecv(int u, char* buf, int len) {
	int read = 0;
	int toread = len;
	int ret = 0;

	while (read < toread) {
		ret = recv(u, buf+read, toread, 0);
		if (ret <= 0)
			return ret;

		read += ret;
		toread -= ret;
	}

	return len;
}

int readline(int fd, string& line, unsigned int maxlen, int (*recvfunc)(int, char*, int)) {
	char c;
	int rc;

	line.clear();

	while (true) {
		rc = recvfunc(fd, &c, 1);
		if (rc <= 0)
			return -1;

		if (c =='\n')
			return line.size();

		line += c;

		if (line.size() == maxlen)
			return maxlen;
	}
}

typedef struct HTTPHeader {
   string status;
   map<string, string> vals;
} HTTPHeader;


int HTTPHeaderToString(const HTTPHeader* hdr, string& str) {
	str.clear();
	str.append(hdr->status);
	str.append("\015\012");

	map<string, string>::const_iterator iter;
	for (iter = (hdr->vals).begin(); iter != (hdr->vals).end(); iter++) {
		str.append(iter->first);
		str.append(":");
		str.append(iter->second);
		str.append("\015\012");
	}

	str.append("\015\012");

	return 0;
}


int readHTTPHeader(int fd, HTTPHeader* hdr, int (*recvfunc)(int, char*, int)) {

	string line;
	bool first = true;

	int read = 0;
	while (true) {
		read = readline(fd, line, MAXLINE, recvfunc);
		if (read == MAXLINE) {
			cout << "max line reached" << endl;
			return -1;
		}

		// the end of http header
		if (line.size() == 0) {
			cout << "end of header" << endl;
			return 0;
		}

		if (first) {
			hdr->status = line;
			cout << hdr->status << endl;
			first = false;
		}
		else {
			//split string into key:value
			size_t found = line.find(":");
			if (found != std::string::npos) {
				string key = line.substr(0, found);
				string value = line.substr(found+1, line.size());
				if (key.size() > 0) {
					// convert key to lowercase
					transform(key.begin(), key.end(), key.begin(), ::tolower);
					// store key:value pair in a collection map
					hdr->vals[key] = value;
					cout << key << " " << value << endl;
				}
			}
		} //end else
	} //end while

	return 0;
}


int redirectUDT(UDTSOCKET u, const string& ngasHost, int ngasPort, const string& header, int64_t filesize)
{
	int fd = 0;

	// connect to an NGAS instance
	fd = connect(ngasHost.c_str(), ngasPort);
	if (fd < 0) {
		cout << "error connecting to " << ngasHost << endl;
		return -1;
	}

	cout << "connected to " << ngasHost << endl;

	// write NGAS HTTP header
	if (reliableTCPWrite(fd, header.c_str(), header.size()) < 0) {
		cout << "error sending header to " << ngasHost << endl;
		close(fd);
		return -1;
	}

	cout << "header send to " << ngasHost << endl;

	char buf[BUFFSIZE];
	int64_t filetoread = filesize;
	int64_t fileread = 0;
	int read = 0;

	// stream file to NGAS
	while (fileread < filetoread) {
		// read from UDT
		read = UDT::recv(u, buf, BUFFSIZE, 0);
		if (read == UDT::ERROR) {
			cout << "UDT::recv error" << endl;
			close(fd);
			return -1;
		}

		// write to NGAS
		if (reliableTCPWrite(fd, buf, read) < 0) {
			cout << "write to ngas error" << endl;
			close(fd);
			return -1;
		}

		fileread += read;
		//cout << fileread << endl;
	}

	cout << "file read " << fileread << endl;

	// read the NGAS response header
	HTTPHeader hdr;
	int ret = readHTTPHeader(fd, &hdr, reliableTCPRecv);
	if (ret < 0) {
	   cout << "Invalid HTTP header" << endl;
	   close(fd);
	   return -1;
	}

	// get the payload from the HTTP header
	string content("content-length");
	if (hdr.vals.find(content) == hdr.vals.end()) {
	   cout << "content-length in HTTP header does not exist" << endl;
	   close(fd);
	   return -1;
	}

	char * endptr;
	// convert filesize from string to int64
	int64_t contentsize = strtoumax(hdr.vals[content].c_str(), &endptr, 10);
	if (contentsize == 0 || errno == ERANGE) {
	 cout << "error parsing content-length" << endl;
	 close(fd);
	 return -1;
	}

	// must return this to client
	char* bufpayload = new char[contentsize];
	ret = reliableTCPRecv(fd, bufpayload, contentsize);
	if (ret <= 0) {
		delete[] bufpayload;
		close(fd);
		return -1;
	}

	cout << string(bufpayload) << endl;

	delete[] bufpayload;
	// close socket to ngas
	close(fd);

	return 0;
}


void* recvFile(void* usocket)
{
   UDTSOCKET fhandle = *(UDTSOCKET*)usocket;
   delete (UDTSOCKET*)usocket;

   // read in the HTTP header from client
   HTTPHeader hdr;
   int ret = readHTTPHeader(fhandle, &hdr, reliableUDTRecv);
   if (ret < 0) {
	   cout << "Invalid HTTP header" << endl;
	   UDT::close(fhandle);
	   return NULL;
   }

   // get the file size from the HTTP header
   string content("content-length");
   if (hdr.vals.find(content) == hdr.vals.end()) {
	   cout << "content-length in HTTP header does not exist" << endl;
	   UDT::close(fhandle);
	   return NULL;
   }

   char * endptr;
   // convert filesize from string to int64
   int64_t filesize = strtoumax(hdr.vals[content].c_str(), &endptr, 10);
   if (filesize == 0 || errno == ERANGE) {
		cout << "error parsing content-length" << endl;
		UDT::close(fhandle);
		return NULL;
   }
   cout << "filesize: " << filesize << endl;

   // http struct convert to string
   string header;
   HTTPHeaderToString(&hdr, header);

   cout << header << endl;
   ret = redirectUDT(fhandle, "store02.icrar.org", 7778, header, filesize);

   // Place holder, send the header back to client from ngas
   char c = 1;
   UDT::send(fhandle, &c, 1, 0);

   UDT::close(fhandle);

   return 0;
}



int main(int argc, char *argv[])
{
	//usage: sendfile [server_port]
	if ((2 < argc) || ((2 == argc) && (0 == atoi(argv[1]))))
	{
		cout << "usage: ngamsUDTReceiver [server_port] [file dump path]" << endl;
		return -1;
	}

	string service("9000");
	if (2 == argc)
		service = argv[1];

	startUDTServer(service);

	return 0;
}


