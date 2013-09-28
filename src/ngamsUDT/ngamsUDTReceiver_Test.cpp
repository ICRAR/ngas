#include <fstream>
#include <iostream>
#include <cstring>
#include "udt.h"

using namespace std;

struct file_metadata {
    string file_name; //base name
    string file_path; //full path (including the base name)
    string mime_type; //e.g. application/octet-stream
    int64_t file_size;
};


int main(int argc, char* argv[]) {
	if (argc != 7) {
	     cout << "usage: ngamsUDTSender server_ip server_port file_name file_path mime_type file_size" << endl;
	     return -1;
	}



	return 0;
}


