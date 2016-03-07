#ifndef __CONNECTER_HH__
#define __CONNECTER_HH__
#include <ctime>
#include <sys/types.h>
#include <string>
#include <iostream>
#include <netinet/in.h>
#include <assert.h> 
#include <list>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <unordered_map>
#include <pthread.h>

#include "configure.h"

#include <semaphore.h>

#define NA -1
#define BILLION 1000000000L
#define SERVER_IP____ "127.0.0.1"
#define PFS_BLOCK_SIZE_BYTE (PFS_BLOCK_SIZE*1024)
struct pck_t{
  char opcode[8];
  char filename[32];
  int  blockId[2];
  int  stripe_width;
  int  fd;
  struct pfs_stat pstat;
  char data[PFS_BLOCK_SIZE_BYTE];
};

using namespace std;
struct send_flit{
      void * p;
      size_t s;
};

struct timestamp_pck_t{
    double t;
    pck_t pck;
    int connectid;
};



//Client Connector
typedef  struct timestamp_pck_t time_pck_t;
typedef  unordered_map<int, list<time_pck_t> > sock_pck_Map;
typedef unordered_map<int, int> sock_count_Map;
typedef  struct pck_t pck_t;

class Connecter;
struct client_start_t {Connecter * c;};
class Connecter{
private:
    string  ipAddr;
    int     ipPort;
    int     sockfd;
    void *  _recv_;
    int     rec_len;
    struct sockaddr_in    servaddr;    
    struct pck_t _pck;
    int pck_size;
    bool coming;

    struct send_flit sflit;
    sem_t send_lock;
public:
    Connecter(string ipAddr, int ipPort);

    ~Connecter();

    static void *  start( void * p);
    void* start_connection(Connecter * l );    
    void* _start(struct client_start_t * c_start);
    void* _start(Connecter * l );    
    void _send_(Connecter* c, void * p, size_t size);
    void _send(Connecter* c, void * p, size_t size);
    void _recv(Connecter* c);
    void * getrecv(Connecter* c);
};
//class ChildListener;
class Listener;
//Server child Listener
class ChildListener{
private:
  int _sock;
  int read_size;
  void *  _recv_;
  pck_t _pck;
  int packet_size;
  Listener *_l;
  sock_pck_Map* send_map;
  sock_pck_Map* recv_map;
public:
  ChildListener(int connect_fd, Listener *l);
  ChildListener();
  ~ChildListener();
  void _send(sock_pck_Map*);
  void _recv(sock_pck_Map*, sock_count_Map*);
  void _reduction(struct cl_t );
  static void* _start(void *);
   void print_recv_table(int sock, sock_pck_Map*);
  void print_recv_table(sock_pck_Map*  recv_map)   ;
};

struct cl_t {
ChildListener * cl;
unordered_map<int, list<time_pck_t> >* send_map;
unordered_map<int, list<time_pck_t> >* recv_map;
unordered_map<int, int> * send_resp_count_list;  //sock -> count
 unordered_map<int, int> * send_rqst_count_list;  //
  unordered_map<int, int> * recv_resp_count_list;
  unordered_map<int, int> * recv_rqst_count_list;
};

struct lister_start_t {
  Listener* l;
};
//Server Listener
class Listener{
private:
  int     ipPort;
  int     socket_fd, connect_fd; 
protected:
  struct sockaddr_in    servaddr, client;
  pthread_t thread_id;
public:
  unordered_map<int, pthread_mutex_t> connection_lock_map;
  list<ChildListener* > listeners;
  list<int> connecters;
  unordered_map<int, list<time_pck_t> >* client_send_map;
  unordered_map<int, list<time_pck_t> >* client_recv_map;


  unordered_map<int, int> * send_resp_count_list;  //sock -> count
  unordered_map<int, int> * send_rqst_count_list;  //
  unordered_map<int, int> * recv_resp_count_list;
  unordered_map<int, int> * recv_rqst_count_list;
/*  unordered_map<int, pthread_mutex_t> * send_count_lock_map;
  unordered_map<int, pthread_mutex_t> * recv_count_lock_map; */   
  unordered_map<int, ChildListener* >      sock_cl_map;
  sem_t send_sem, recv_sem;
  Listener(int ipPort)
  {
    sem_init(&send_sem, 0, 0);
    sem_init(&recv_sem, 0, 0);
    if( (socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1 ){
      printf("create socket error: %s(errno: %d)\n",strerror(errno),errno);
      exit(0);
    }
    //initialize address
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    //servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(ipPort);

    inet_aton(SERVER_IP____, &servaddr.sin_addr);    
    //bind local address with socket
    if( bind(socket_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) == -1){
      printf("bind socket error: %s(errno: %d)\n",strerror(errno),errno);
      exit(0);
    }
    
    //listen if there is client connection
    if( listen(socket_fd, 128) == -1){
        printf("listen socket error: %s(errno: %d)\n",strerror(errno),errno);
        exit(0);
    }
    client_send_map = new unordered_map<int, list<time_pck_t> >();
    client_recv_map = new unordered_map<int, list<time_pck_t> >();

    send_resp_count_list = new unordered_map<int, int>();  
    send_rqst_count_list = new unordered_map<int, int>();
    recv_resp_count_list = new unordered_map<int, int>();   
    recv_rqst_count_list = new unordered_map<int, int>();    
  }

  ~Listener(){
    close(socket_fd);
  }
  bool recv_resp_is_all_0()
  {
    for (auto it = (*recv_resp_count_list).begin(); it != (*recv_resp_count_list).end(); ++it )
      if (it->second != 0) return false;
    return true;
  }

  void* _start(Listener * l);

  static void* start(void * p);
};


#endif
