#include "client-cache.hh"
#include "connecter.hh"
#include "configure.h"
#include "pck_util.hh"

#include <semaphore.h>
#include <algorithm>
#include <vector>
using namespace std;


/*#define SERVER_IP "127.0.0.1"
#define METADATA_SERVER_PORT 8000*/

#define SERVER_IP "127.0.0.1"
#define METADATA_SERVER_PORT 8000

#define UPPER_THRESHOLD 0.7
#define LOWER_THRESHOLD 0.3

#define NUM_CACHE_BLOCK  (2048)
#define CACHE_SIZE       (PFS_BLOCK_SIZE * NUM_CACHE_BLOCK)	
#define _UPPER_THRESHOLD_ (0.7*NUM_CACHE_BLOCK)
#define _LOWER_THRESHOLD_ (0.3*NUM_CACHE_BLOCK)


typedef struct fkey_t{
public:	
	int fd;
	int bid;
} fkey_t;

struct fileblock_t{
 	char data[PFS_BLOCK_SIZE_BYTE];
 	int  off[2];
 	bool deleted;
/* 	fileblock_t(){
 		memset(data, '\0', sizeof(data));
 		memset(off, '\0', sizeof(off));
 		deleted = false;
 	 };*/
};
typedef struct fileblock_t fileblock_t;

typedef string file_block_str_t ;
typedef string server_ip_t		 ;
typedef  pair<int, int> int_pair;
typedef pair<fkey_t, fileblock_t> update_msg_t;


class PFSCache;
class Client{
private:
	enum OPERATIONS{
		kCREATE = 0,
		kOPEN   = 1,
		kREAD   = 2,
		kWRITE  = 3,
		kCLOSE  = 4,
		kDELETE = 5,
		kFSTAT  = 6,
		kOPEND
	};
	string pfs_op[kOPEND];

	class Client_Token_Manager{
	public:
		unordered_map<int, vector<int_pair> > fd_range; // fd token range;
		unordered_map<int, vector<int_pair> > fd_used;
		Client_Token_Manager() {};

		void _insert(int fd, int_pair _pair)
		{
			auto range_list = fd_range[fd];
			for(int i = 0; i < range_list.size(); i++){
				int intersect_len = min(range_list[i].second, _pair.second) - max(range_list[i].first, _pair.first);
				if (intersect_len > 0){
					range_list[i].first = min(range_list[i].first, _pair.first);
					range_list[i].second = max(range_list[i].second, _pair.second);
					return;
				}
			}
			fd_range[fd].push_back(_pair);
			return;
		}

		void _remove(int fd, int_pair& _pair)
		{
			return;
		}

		int_pair _inquery(int fd, int_pair& _pair)
		{
			auto range_list = fd_range[fd];
			auto used_list = fd_used[fd];
			int_pair result(-1, -1);
			int max_intersect_len = 0;
			for(int i = 0; i < range_list.size(); i++){
				if(range_list[i].second >= _pair.second && range_list[i].first <= _pair.first){
					bool overlap = false;
					for(int j = 0; j < used_list.size(); j++){
						if(min(used_list[j].second, _pair.second) - max(used_list[j].first, _pair.first) > 0){
							overlap = true;
						}
					}
					if(overlap){
						break;
					}else{
						int overlap_index = -1;
						for(int j = 0; j < used_list.size(); j++){
							if(min(used_list[j].second, range_list[i].second) - max(used_list[j].first, range_list[i].first) > 0){
								overlap_index = j;
								break;
							}
						}
						if(overlap_index != -1)
						{
							int j = overlap_index;
							if(used_list[j].first - range_list[i].first > range_list[i].second - used_list[j].second){
								result.second = used_list[j].first;
								result.first = range_list[i].first;
								return result;
							}else{
								result.second = range_list[i].second;
								result.first = used_list[j].second;
								return result;
							}
						}else{
							return range_list[i];
						}
						
					}
				}
			}
			return result;
			//auto used_list = fd
		}

		bool checkifhastoken(int fd, int_pair& _pair)
		{
			auto range_list = fd_range[fd];
			for(int i = 0; i < range_list.size(); i++){
				if(range_list[i].first <= _pair.first && range_list[i].second >= _pair.second)
					return true;
			}
			return false;
		}
	};

	PFSCache* cache;

	Connecter* link_fs;
	Client_Token_Manager* tr;
	Client_Token_Manager* tw;
	list<string> fn_list;


public:
	Connecter* link;	
	sem_t read_finish, write_finish; 	
	unordered_map<string, int> fn_fd;
	unordered_map<file_block_str_t, server_ip_t> fb_server_hm; //fb->server ip
	unordered_map<server_ip_t, int> server_ip_socket_hm;	
 	Client();
 	Client(int port);
 	void * _start(){

 		return NULL;
 	}
	int pfs_create(const char *filename, int stripe_width);
	int pfs_open(const char *filename, const char mode);
	ssize_t pfs_read(int filedes, void *buf, ssize_t nbyte, off_t offset, int *cache_hit);
	ssize_t pfs_write(int filedes, const void *buf, size_t nbyte, off_t offset, int *cache_hit);
	int pfs_close(int filedes);
	int pfs_delete(const char *filename);
	int pfs_fstat(int filedes, struct pfs_stat *buf); // Check the config file for the definition of pfs_stat structure
	int find_block_server_port(int filedes, int bid);
};



enum CacheStatus{
	kCLEAN = 0,
	kDIRTY = 1,
};




class CacheNode {
public:
	fkey_t key;
	fileblock_t value;
	int status;
	CacheNode(){}
	CacheNode(fkey_t k,  fileblock_t v) :key(k), value(v), status(kCLEAN){}
};	

class LRUCache{

protected:
	// dirty_list
	Client * clt;
	list<string> dirtyList;
	list<string> cleanList;	
	// cache index: CacheNode, LRU-based
	list<string> cacheList;
	// a map of <key, cache index>
	unordered_map<string, CacheNode> cacheMap;
	int capacity;

public:
	LRUCache(int capacity);
	bool checkIfHit(fkey_t key);
	fileblock_t read(fkey_t key);
	void write(fkey_t key, fileblock_t value);
};

class PFSCache: public LRUCache{

public:
	pthread_t _harv_thd;
	pthread_t _flush_thd;

	PFSCache(Client * _client);
	void _send_fileserver(int _sock, fkey_t key, fileblock_t value);
	void _populate(fkey_t key, fileblock_t value);
	void _remove(list<CacheNode>::iterator it);
	void * _harvester();
	void * _flush();
	static void* harvester(void * cache);	
	static void* flush(void * cache);
	void del(int fd);


private:	

	Connecter * connect_metadata;
};
