/*
 * ngamsUDTUtils.cpp
 *
 *  Created on: Sep 28, 2013
 *      Author: dpallot
 */

#include <netdb.h>
#include <string>
#include <cstring>
#include <iostream>
#include <inttypes.h>
#include <errno.h>

#include "ngamsUDTUtils.h"
#include "udt.h"

#define MAXLINE 16384
#define MAXELEMENTS 100

int readline(int fd, string& line, unsigned int maxlen, int (*recvfunc)(int, char*, int)) {
	char c;
	int rc;

	line.clear();

	while (true) {
		rc = recvfunc(fd, &c, 1);
		if (rc < 0)
			return -1;

		if (c =='\n')
			return line.size();

		line += c;

		if (line.size() == maxlen)
			return maxlen;
	}
}


int HTTPHeaderToString(const HTTPHeader* hdr, string& str) {
	str.clear();
	str.append(hdr->status);
	str.append("\r\n");

	map<string, string>::const_iterator iter;
	for (iter = (hdr->vals).begin(); iter != (hdr->vals).end(); iter++) {
		str.append(iter->first);
		str.append(":");
		str.append(iter->second);
		str.append("\r\n");
	}

	str.append("\r\n");

	return 0;
}


int readHTTPHeader(int fd, HTTPHeader* hdr, int (*recvfunc)(int, char*, int)) {

	string line;
	bool first = true;

	int read = 0;
	while (true) {
		read = readline(fd, line, MAXLINE, recvfunc);
		if (read < 0) {
			//cout << "readline error" << endl;
			return -1;
		}

		if (read == MAXLINE) {
			//cout << "max line reached" << endl;
			return -1;
		}

		// Check if max number of HTTP lines reached
		if (hdr->vals.size() >= MAXELEMENTS) {
			//cout << "max number of http elements reached" << endl;
			return -1;
		}

		// the end of http header
		if (line.size() == 0) {
			//cout << "end of header" << endl;
			return 0;
		}

		// end of line but there is a single carridge return
		// http header ends with an empty line i.e \r\n
		if (line.size() == 1 and line[0] == '\r') {
			//cout << "end of header" << endl;
			return 0;
		}

		// read the status http header
		if (first) {
			hdr->status = line;
			//cout << hdr->status << endl;
			first = false;
		}
		else {
			// strip carridge return from end of string if it exists
			if (line[line.size()-1] == '\r') {
				//cout << "carridge return found at end of string" << endl;
				line = line.substr(0, line.size()-1);
			}

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
					//cout << key << " " << value << endl;
				}
			}
		} //end else
	} //end while

	return 0;
}

int writeHTTPPacket(int fd, const HTTPHeader* hdr, const HTTPPayload* payload, int (*writefunc)(int, const char*, int))
{
	string hdrStr;
	HTTPHeaderToString(hdr, hdrStr);

	// write header
	int ret = writefunc(fd, hdrStr.c_str(), hdrStr.size());
	if (ret <= 0)
		return -1;

	// write payload
	ret = writefunc(fd, payload->buff, payload->payloadsize);
	if (ret <= 0)
		return -1;

	return 0;
}

int readHTTPPacket(int fd, HTTPHeader* hdr, HTTPPayload* payload, int (*recvfunc)(int, char*, int))
{
	// read the NGAS response header
	int ret = readHTTPHeader(fd, hdr, recvfunc);
	if (ret < 0) {
	   cout << "invalid HTTP header" << endl;
	   return -1;
	}

	// get the payload size from the HTTP header
	string content("content-length");
	if (hdr->vals.find(content) == hdr->vals.end()) {
	   cout << "content-length in HTTP header does not exist" << endl;
	   return -1;
	}

	char* endptr;
	// convert filesize from string to int64
	int64_t contentsize = strtoumax(hdr->vals[content].c_str(), &endptr, 10);
	if (contentsize == 0 || errno == ERANGE) {
		cout << "error parsing content-length" << endl;
		return -1;
	}

	payload->payloadsize = contentsize;
	payload->buff = new char[contentsize];
	if (payload->buff == NULL)
		return -1;

	ret = recvfunc(fd, payload->buff, contentsize);
	if (ret <= 0) {
		cout << "error reading http payload" << endl;
		delete[] payload->buff;
		payload->buff = NULL;
		return -1;
	}

	return 0;
}


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


int reliableTCPWrite(int fd, const char* buf, int len) {
	int towrite = len;
	int written = 0;
	int ret = 0;

	while (written < towrite) {
		ret = write(fd, buf+written, towrite);
		if (ret <= 0)
			return -1;

		written += ret;
		towrite -= ret;
	}

	return len;
}

int reliableUDTWrite(int u, const char* buf, int len) {
	int towrite = len;
	int written = 0;
	int ret = 0;

	while (written < towrite) {
		ret = UDT::send(u, buf+written, towrite, 0);
		// UDT::ERROR == -1
		if (ret == UDT::ERROR)
			return -1;

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
			return -1;

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
			return -1;

		read += ret;
		toread -= ret;
	}

	return len;
}
