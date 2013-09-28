#include <inttypes.h>
#include <netdb.h>
#include <iostream>
#include <errno.h>

#include "ngamsUDTUtils.h"
#include "udt.h"

#define BUFFSIZE 64000

using namespace std;

// prototype
void* recvFile(void*);

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


int redirectUDT(UDTSOCKET u, int fd, int64_t filesize)
{

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
			//close(fd);
			return -1;
		}

		// write to NGAS
		if (reliableTCPWrite(fd, buf, read) < 0) {
			cout << "write to ngas error" << endl;
			//close(fd);
			return -1;
		}

		fileread += read;
		//cout << fileread << endl;
	}

	cout << "UDT data read and transmitted to NGAS: " << fileread << endl;

	return 0;
}


void* recvFile(void* usocket)
{
   UDTSOCKET fhandle = *(UDTSOCKET*)usocket;
   delete (UDTSOCKET*)usocket;

   // read in the HTTP header from UDT client
   HTTPHeader reqHdr;
   int ret = readHTTPHeader(fhandle, &reqHdr, reliableUDTRecv);
   if (ret < 0) {
	   cout << "Invalid HTTP header" << endl;
	   UDT::close(fhandle);
	   return NULL;
   }

   // get the file size from the HTTP header
   string content("content-length");
   if (reqHdr.vals.find(content) == reqHdr.vals.end()) {
	   cout << "content-length in http header does not exist" << endl;
	   UDT::close(fhandle);
	   return NULL;
   }

   char * endptr;
   // convert filesize from string to int64
   int64_t filesize = strtoumax(reqHdr.vals[content].c_str(), &endptr, 10);
   if (filesize == 0 || errno == ERANGE) {
		cout << "error parsing content-length" << endl;
		UDT::close(fhandle);
		return NULL;
   }

   // http struct convert to string
   string reqHdrStr;
   HTTPHeaderToString(&reqHdr, reqHdrStr);

   cout << reqHdrStr << endl;

	string ngasHost("store02.icrar.org");
	int ngasPort = 7778;

	cout << "connecting to " << ngasHost << endl;

   	// connect to an NGAS instance
   	int fd = connect(ngasHost.c_str(), ngasPort);
   	if (fd < 0) {
   		cout << "error connecting to " << ngasHost << endl;
   		UDT::close(fhandle);
   		return NULL;
   	}

   	cout << "connected to " << ngasHost << endl;

   	// write http header to NGAS
   	if (reliableTCPWrite(fd, reqHdrStr.c_str(), reqHdrStr.size()) < 0) {
   		cout << "error sending header to " << ngasHost << endl;
   		close(fd);
   		UDT::close(fhandle);
   		return NULL;
   	}

   	// redirect UDT stream to NGAS TCP
   	ret = redirectUDT(fhandle, fd, filesize);
   	if (ret < 0) {
   		close(fd);
   		UDT::close(fhandle);
   		cout << "UDT TCP redirect failed" << endl;
   		return NULL;
   	}

	HTTPHeader respHdr;
	HTTPPayload respPay;
	// read http response from TCP NGAS
	ret = readHTTPPacket(fd, &respHdr, &respPay, reliableTCPRecv);
	if (ret < 0) {
		close(fd);
		UDT::close(fhandle);
		cout << "failed to read http response from ngas" << endl;
		return NULL;
	}

	// send http response to UDT client
	ret = writeHTTPPacket(fhandle, &respHdr, &respPay, reliableUDTWrite);
	if (ret < 0) {
		close(fd);
		UDT::close(fhandle);
		delete[] respPay.buff;
		cout << "failed to write http response to UDT client" << endl;
		return NULL;
	}

	delete[] respPay.buff;
	// clean up and close
	close(fd);
	UDT::close(fhandle);

	return 0;
}


int main(int argc, char *argv[])
{
	//usage: sendfile [server_port]
	if ((2 < argc) || ((2 == argc) && (0 == atoi(argv[1]))))
	{
		cout << "usage: ngamsUDTReceiver [server_port]" << endl;
		return -1;
	}

	string service("9000");
	if (2 == argc)
		service = argv[1];

	startUDTServer(service);

	return 0;
}


