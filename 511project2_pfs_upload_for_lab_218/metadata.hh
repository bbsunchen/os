#ifndef __MD_HH__
#define __MD_HH__
#include <iostream>
#include <ctime>
#include <unordered_map>
#include <map>
#include <string>
#include <algorithm>
#include "configure.h"
#include "connecter.hh"
#include <queue>
using namespace std;
const static int NUM_CLIENTS = 3;

class FileRecipe{
public:
    map<int, int> blockId_stripId;
    map<int, int> stripId_serverId;
};

class BlockRange{
public:
    BlockRange(int _start=-1, int _end=-1): start_index(_start), end_index(_end){}
    int start_index;
    int end_index;
};

class WaitingInfo{
public:
    int client_id;
    BlockRange block_range;
    char mode;
    string filename;
    int request_id;
    bool deleted;

    WaitingInfo(int _client_id = -1, BlockRange _token_range = BlockRange(), char _mode = 'r', string _filename = "", int _request_id = -1, bool _deleted = false){
        client_id = _client_id;
        block_range = _token_range;
        mode = _mode;
        filename = _filename;
        request_id = _request_id;
        deleted = _deleted;
    }
};

bool remove_WaitingInfo(const WaitingInfo & t){
    return t.deleted;
}

class ResponseInfo{
public:
    int request_id;
    BlockRange block_range;
    ResponseInfo(int _request_id = -1, BlockRange _block_range = BlockRange()){
        request_id  = _request_id;
        block_range = _block_range;
    }
};

struct less_than_start_index{
    inline bool operator() (const BlockRange& range1, const BlockRange& range2){
        return (range1.start_index < range2.start_index);
    }
};


class TokenInfo{
public:
    TokenInfo(int _client_id = -1, char _token_class='r', BlockRange _block_range=BlockRange()): client_id(_client_id), token_class(_token_class), block_range(_block_range){
        deleted = false;
    }
    int client_id;
    char token_class; // it can either be a read token or a write token
                     // 'r' read token, 'w' write token
    BlockRange block_range;
    bool deleted;
};

bool remove_TokenInfo(const TokenInfo & t){
    return t.deleted;
}

class Metadata{
public:
    string filename;
    int file_size;
    time_t creation_time;
    time_t modification_time;
    FileRecipe file_recipe;
    int block_num;
    int strip_num;
    map<int, int> stripId_blockNum;

    Metadata(string _filename="", int _file_size=-1, double _creation_time=0.0, int server_num = 1){
        //cout << "initialize Metadata" << endl;
        filename = _filename;
        file_size = _file_size;
        creation_time = _creation_time;
        modification_time = _creation_time;

        block_num = file_size;
        //cout << block_num << endl;
        strip_num = block_num / STRIP_SIZE;
        if(strip_num * STRIP_SIZE < block_num)
            strip_num ++;
        int block_id = 0;
        int strip_id = 0;
        int server_id = strip_id % server_num;

        while(block_id < block_num){

            for(int i = 0; i < STRIP_SIZE && block_id < block_num; i++){
                file_recipe.blockId_stripId.insert(pair<int, int>(block_id, block_id));
                file_recipe.stripId_serverId.insert(pair<int, int>(strip_id, server_id));
                block_id ++;

                if(stripId_blockNum.find(strip_id) != stripId_blockNum.end()){
                    stripId_blockNum[strip_id] ++;
                }else{
                    stripId_blockNum.insert(pair<int, int>(strip_id, 1));
                }

            }
            strip_id ++;
            server_id = strip_id % server_num;
        }
    }
};
    

class msgq_compare
{
public:
  bool operator() (const time_pck_t& lhs, const time_pck_t&rhs) const
  {
    return (lhs.t>rhs.t);
  }
};


class MetadataManager{
private:
    static double current_time;
    unordered_map<string, Metadata> filename_metadata_hash;
    int server_num; // server number defined in "configure.h"
    int real_server_num; // real time active server number
    int file_accu_id;
    int request_id;
    int cac_id;

    unordered_map<int, int> serverId_connectId_hash;
    unordered_map<int, int> serverId_port_hash;
    
    unordered_map<string, int> filename_fileid;
    unordered_map<int, string> fileid_filename;

    unordered_map<int, int> clientId_connectId_hash;
    unordered_map<int, int> connectId_clientId_hash;

    unordered_map<int, int> cacId_connectId;

    int server_connection[NUM_FILE_SERVERS];
    int client_connection[NUM_CLIENTS];

    time_t get_current_time(){
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            return t.tv_sec;
            //double _t = t.tv_sec + t.tv_nsec/BILLION;    
            //time_pck_t t_pck = {_t, pck, _sock};
    }

    Listener* l;
public:
    unordered_map<string, vector<TokenInfo> > filename_tokenAssignList_hash;
    
    unordered_map<string, vector<TokenInfo> > filename_tokenWaitingList_hash;

    // need a lock for this waiting_list
    vector<WaitingInfo > request_waiting_list;

    // need a lock
    unordered_map<int, vector<int> > requestId_responseClientList_hash;
    
    // need a lock
    unordered_map<int, vector<ResponseInfo> > clientId_responseinfoList_hash;

    unordered_map<int, int> * send_resp_count_list;
    unordered_map<int, int> * recv_resp_count_list;   

    unordered_map<string, int> filename_actimes;

    priority_queue<time_pck_t, std::vector<time_pck_t>, msgq_compare> * msgq;
    MetadataManager();
    void _start();
    int create_file(string filename, double block_size, int cac_id);
    int query_block(string filename, int block_id);
    int insert_block(string filename, int block_id);
    int delete_file(string filename);
    void register_server(int port, int connect_id);
    BlockRange intersect_two_range(BlockRange reference_range, BlockRange query_range, int & intersect_length);
    bool check_range_overlap(BlockRange reference_range, BlockRange query_range);
    vector<BlockRange> intersect_range_list(vector<vector<BlockRange> > candidate_range_list_vector);
    vector<BlockRange> union_range(vector<BlockRange> candidate_range_list);
    BlockRange choose_best_range(vector<BlockRange> candidate_distribute_range, BlockRange request_block_range);
    BlockRange read_file(const string & filename, const BlockRange & request_block_range, const int client_id);
    BlockRange write_file(const string & filename, const BlockRange & request_block_range, const int client_id);
    void assign_tokens(const string & filename, int client_id, char mode, const BlockRange & block_range);
    int close_file(string filename, int client_id);
    int file_stat(string filename, struct pfs_stat & f_stat);
    int revoke_tokens(string filename, int client_id, BlockRange block_range, string token_type);
    int prepare_repeat(const string & filename, const BlockRange & request_block_range, const int client_id, const int request_id);
    BlockRange repeat_read(const string & filename, const BlockRange & request_block_range, const int client_id);
    BlockRange repeat_write(const string & filename, const BlockRange & request_block_range, const int client_id);
    int check_server_port(const string & filename, int block_id);

};


#endif