#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <queue>

#include <iostream>     // std::cout
#include <sstream>      // std::stringstream, std::stringbuf


#include "configure.h"
#include "connecter.hh"
#include "metadata.hh"
using namespace std;

/*split function*/
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (! item.empty()){
            elems.push_back(item);
        }
    }
    return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

class Server{

private:
	unordered_map<string, char*> block_context_hash;
    Listener *l;
    Connecter *c;
    unordered_map<int, int> * send_resp_count_list;
    unordered_map<int, int> * recv_resp_count_list;   

    priority_queue<time_pck_t, std::vector<time_pck_t>, msgq_compare> * msgq;    
public:
	int myPort;
	Server(int);
	//void* start();
	void * start();
	void* _recv();
	void* _send();
	string create_key(int file_id, int block_id);
	int create_block(int file_id, int block_id);
	int delete_block(int file_id, int block_id);
	int write_block(int file_id, int block_id, char* input);
	int read_block(int file_id, int block_id, char* output);
    int delete_file(int file_id);

};