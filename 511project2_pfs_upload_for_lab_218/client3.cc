#include "client.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#define ONEKB 1024


int Client::pfs_create(const char *filename, int stripe_width)
{
    struct pck_t pck;
    
    string op("C");
    string fn(filename);
    int bd[2] = {0, stripe_width*STRIP_SIZE};
    int sw = stripe_width;
    int fd = NA;
    struct pfs_stat ps;
    char* data = NULL;
    //en-package the pck
  
    Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));

     link->_recv(link);

     auto pck_recv = link->getrecv(link);
     while (pck_recv == NULL) 
        {
          link->_recv(link);
          pck_recv = link->getrecv(link);
        }

     pck = (*((struct pck_t*)pck_recv));
     cout <<"Received From Server: " << pck.opcode<<endl;
     fn_fd[fn] = pck.fd;

     return 0;
}

int Client::pfs_open(const char *filename, const char mode)
{
    struct pck_t pck;
    string op("OR");
    if (mode == 'w')
      op = "OW";

    string fn(filename);
    int bd[2] = {0, 0};
    int sw = NA;
    int fd = NA;
    struct pfs_stat ps;
    char* data = NULL;

    Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));

    link->_recv(link);

    auto pck_recv = link->getrecv(link);
    while (pck_recv == NULL) 
    {
        link->_recv(link);
        pck_recv = link->getrecv(link);
    }

    pck = (*((struct pck_t*)pck_recv));
    cout <<"Received From Server: " << pck.opcode<<endl;
    fn_fd[fn] = pck.fd;    
    return fn_fd[fn];
}

Client::Client(){
  //assert(0);
  sem_init(&read_finish, 0, 0); sem_init(&write_finish, 0, 0);
  tr = new Client_Token_Manager();
  tw = new Client_Token_Manager(); 
  cache = new PFSCache(this);
  link = new Connecter(SERVER_IP, METADATA_SERVER_PORT);
  //link_fs = new Connecter(SERVER_IP, METADATA_SERVER_PORT);

   pfs_op[0] = "pfs_create";
    pfs_op[1] = "pfs_open";
  pfs_op[2] = "pfs_read";
  pfs_op[3] = "pfs_write";
  pfs_op[4] = "pfs_close";
  pfs_op[5] = "pfs_delete";
  pfs_op[6] ="pfs_fstat";
}

Client::Client(int port){
  tr = new Client_Token_Manager();
  sem_init(&read_finish, 0, 0); sem_init(&write_finish, 0, 0);
  tw = new Client_Token_Manager();  
  cache = new PFSCache(this);
  link = new Connecter(SERVER_IP, port);
  //link_fs = new Connecter(SERVER_IP, METADATA_SERVER_PORT);

   pfs_op[0] = "pfs_create";
    pfs_op[1] = "pfs_open";
  pfs_op[2] = "pfs_read";
  pfs_op[3] = "pfs_write";
  pfs_op[4] = "pfs_close";
  pfs_op[5] = "pfs_delete";
  pfs_op[6] ="pfs_fstat";
}



int Client::find_block_server_port(int filedes, int bid)
{
    int block_server_port;
      struct pck_t pck;
      string op("Q");
      string fn("");
      int bd[2] = {bid, bid};
      int sw = NA;
      int fd = filedes;
      struct pfs_stat ps;
      char* data = NULL;
  
      Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));
      link->_recv(link);

      auto pck_recv = link->getrecv(link);
      while (pck_recv == NULL) 
      {
        link->_recv(link);
        pck_recv = link->getrecv(link);
      }

      pck = (*((struct pck_t*)pck_recv));
      cout <<"Received From Meta: " << pck.opcode<<endl;
      return  block_server_port = pck.stripe_width;
}
ssize_t Client::pfs_read(int filedes, void *buf, ssize_t nbyte, off_t offset, int *cache_hit)
{
  struct pck_t pck;

  char * readbuf = (char *)buf;

  int startBID = offset/PFS_BLOCK_SIZE_BYTE;
  int endBID   = (offset + nbyte)/PFS_BLOCK_SIZE_BYTE;

  int fisrtB_offset = offset - startBID * PFS_BLOCK_SIZE_BYTE;
  int lastB_offset  = offset + nbyte - endBID * PFS_BLOCK_SIZE_BYTE;
  fkey_t key; 
  fileblock_t fb;
  int_pair _pair(startBID, endBID);
  if(tr->checkifhastoken(filedes, _pair) == false)
  {
      string op("R");
      string fn("");
      int bd[2] = {startBID, endBID};
      int sw = NA;
      int fd = filedes;
      struct pfs_stat ps;
      char* data = NULL;
  
      Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));
      
      link->_recv(link);

      auto pck_recv = link->getrecv(link);
      while (pck_recv == NULL) 
      {
        link->_recv(link);
        pck_recv = link->getrecv(link);
      }

      pck = (*((struct pck_t*)pck_recv));
      cout <<"Received From Meta: " << pck.opcode<<endl;


      if (pck.blockId[1] == -1 && pck.blockId[0] == -1)
        return -1;      
      
      int_pair _ar_pair = {pck.blockId[0], pck.blockId[1]};
      tr->_insert(filedes, _ar_pair);
      cout << "Returned Token First: "<< _ar_pair.first << " Sec: " << _ar_pair.second <<endl;  
      cout << "Returned Token First: "<< _pair.first << " Sec: " << _pair.second <<endl;  

      if (tr->checkifhastoken(filedes, _pair) == false)
        return -2;
  }
  
  for (int bid = startBID; bid <= endBID; bid++) {
    key={filedes, bid}; 
    
    if (true == cache->checkIfHit(key))
    {   
        fb = cache->read(key);
        memcpy(fb.data, readbuf + bid*PFS_BLOCK_SIZE_BYTE, sizeof(fb.data));
    } else {


      int  block_server_port = find_block_server_port(filedes, bid);
      Connecter * link_server = new Connecter(SERVER_IP, block_server_port);


      string op("F");
      string fn("");
      int bd[2] = {startBID, endBID};
      int sw = NA;
      int fd = filedes;
      struct pfs_stat ps;
      char* data = NULL;
  
      Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    void* send_pck = (void*)(&pck);

    link_server->start_connection(link_server);
     link_server->_send_(link_server, send_pck, sizeof(pck_t));
          cout<<"Clinent Sent: "<< op <<endl;

      link_server->_recv(link_server);

      auto pck_recv = link_server->getrecv(link_server);
      while (pck_recv == NULL) 
      {
        link_server->_recv(link_server);
        pck_recv = link_server->getrecv(link_server);
      }

      pck = (*((struct pck_t*)pck_recv));
      cout <<"Received From File Server: " << pck.opcode<<endl;

      memcpy(fb.data, pck.data, sizeof(fb.data));      
      memcpy(readbuf + bid*PFS_BLOCK_SIZE_BYTE, fb.data, sizeof(fb.data));
      cache->write(key, fb);
      delete link_server;      
    }
  }
      return nbyte;
}

ssize_t Client::pfs_write(int filedes, const void *buf, size_t nbyte, off_t offset, int *cache_hit)
{
  struct pck_t pck;

  char * writebuf = (char *)buf;

  int startBID = offset/PFS_BLOCK_SIZE_BYTE;
  int endBID   = (offset + nbyte)/PFS_BLOCK_SIZE_BYTE;

  int fisrtB_offset = offset - startBID * PFS_BLOCK_SIZE_BYTE;
  int lastB_offset  = offset + nbyte - endBID * PFS_BLOCK_SIZE_BYTE;
  fkey_t key; 
  
  fileblock_t fb;

  int_pair _pair(startBID, endBID);
 
  if(tw->checkifhastoken(filedes, _pair) == false)
  {
      string op("W");
      string fn("");
      int bd[2] = {startBID, endBID};
      int sw = NA;
      int fd = filedes;
      struct pfs_stat ps;
      char* data = NULL;
  
      Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));
      
      link->_recv(link);

      auto pck_recv = link->getrecv(link);
      while (pck_recv == NULL) 
      {
        link->_recv(link);
        pck_recv = link->getrecv(link);
      }

      pck = (*((struct pck_t*)pck_recv));
      cout <<"Received From Meta: " << pck.opcode<<endl;
      if (pck.blockId[1] == -1 && pck.blockId[0] == -1)
        return -1;      
      
      int_pair _ar_pair = {pck.blockId[0], pck.blockId[1]};
      tw->_insert(filedes, _ar_pair);
      cout << "Returned Token First: "<< _ar_pair.first << " Sec: " << _ar_pair.second <<endl;  
      cout << "Returned Token First: "<< _pair.first << " Sec: " << _pair.second <<endl;  

      if (tw->checkifhastoken(filedes, _pair) == false)
        return -2;
  }


  
  for (int bid = startBID; bid <= endBID; bid++) {
    fb.off[0] = 0;
    fb.off[1] = 1024;     
    memcpy(fb.data, writebuf + bid*PFS_BLOCK_SIZE, sizeof(fb.data));    
    if (bid == startBID)
    {
        fb.off[0] = fisrtB_offset;
        fb.off[1] = 1024; 
    } 
    else if (bid == endBID)
    {
        fb.off[0] = 0;
        fb.off[1] = lastB_offset; 
    }
    key={filedes, bid}; 

    
    if (true == cache->checkIfHit(key))
    {   
        cache->write(key, fb);
    } else {


      int  block_server_port = find_block_server_port(filedes, bid);
      Connecter * link_server = new Connecter(SERVER_IP, block_server_port);

      string op("W");
      string fn("");
      int bd[2] = {startBID, endBID};
      int sw = NA;
      int fd = filedes;
      struct pfs_stat ps;
      char* data = NULL;
  
      Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
    link_server->start_connection(link_server);    
     link_server->_send(link_server, send_pck, sizeof(pck_t));
      link_server->_recv(link_server);

      auto pck_recv = link_server->getrecv(link_server);
      while (pck_recv == NULL) 
      {
        link_server->_recv(link_server);
        pck_recv = link_server->getrecv(link_server);
      }

      pck = (*((struct pck_t*)pck_recv));
      cout <<"Received From FileServer: " << pck.opcode<<endl;
      
      struct fileblock_t _return_fb;
      memcpy(_return_fb.data, pck.data, sizeof(fb.data));
      cache->write(key, _return_fb); 
      cache->write(key, fb); 
      delete link_server;
    }
  }
      return nbyte;
}

int Client::pfs_close(int filedes){
    struct pck_t pck;
    string op("L");
    string fn("");
    int bd[2] = {0, 0};
    int sw = NA;
    int fd = filedes;
    struct pfs_stat ps;
    char* data = NULL;

    Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));


    string filename("");
    
    for (auto it = fn_fd.begin(); it != fn_fd.end(); ++it)  
      if (it->second == filedes)
        {
          filename = it->first;
          break;
        }
  

    cout<<"File "<< filename << "closed" <<endl;
    return 0;
}

int Client::pfs_delete(const char *filename){
    struct pck_t pck;
    string op("D");
    string fn(filename);
    int bd[2] = {0, 0};
    int sw = NA;
    int fd = fn_fd[fn];
    struct pfs_stat ps;
    char* data = NULL;

    cache->del(fd);    
    Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));



    cout<<"File "<< filename << "Deleted" <<endl;    
    
    return 0;
}

int Client::pfs_fstat(int filedes, struct pfs_stat *buf)
{

    struct pck_t pck;
    string op("S");
    string fn("");
    int bd[2] = {0, 0};
    int sw = NA;
    int fd = filedes;
    struct pfs_stat ps;
    char* data = NULL;

    Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
    cout<<"Clinent Send: "<< op <<endl;
    void* send_pck = (void*)(&pck);
     link->_send(link, send_pck, sizeof(pck_t));
 
    auto pck_recv = link->getrecv(link);
    while (pck_recv == NULL) 
    {
        link->_recv(link);
        pck_recv = link->getrecv(link);
    }

    pck = (*((struct pck_t*)pck_recv));
    cout <<"Received From Server: " << pck.opcode<<endl;
   
    memcpy(buf, &(pck.pstat), sizeof(struct pfs_stat));
    
    string filename("");
    
    for (auto it = fn_fd.begin(); it != fn_fd.end(); ++it)  
      if (it->second == filedes)
        {
          filename = it->first;
          break;
        }    
    cout<<filename <<" Last Modification Time: ";
    cout<<(pck.pstat).pst_mtime <<" Create Time: ";
    cout<<(pck.pstat).pst_mtime << " File Size " << (pck.pstat).pst_size<<endl;
    return 0;
}

#define handle_error_en(en, msg) \
               do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)




int main(int argc, char *argv[])
{

  Client* c = new Client();
  pthread_t client_thd;
  struct client_start_t c_start = {c->link};
  void * p = (void *)(&c_start);
  int rc ;
  if ((rc = pthread_create( &client_thd, NULL, &(Connecter::start), (void *)p) )  != 0)
    cout<<"Client ERROR: "<<errno<<endl;
  sleep(5);

  int ifdes, fdes;
  int err_value;
  char input_fname[20];
  char *buf;
  ssize_t nread;
  struct pfs_stat mystat;
  int cache_hit;

  // Initialize the client
 // initialize(argc, argv);
  
  // the command line arguments include an input filename
  if (argc < 2)
    {
      printf("usage: a.out <input filename>\n");
      exit(0);
    }
  strcpy(input_fname, argv[1]);
  ifdes = open(input_fname, O_RDONLY);
  buf = (char *)malloc(4*ONEKB);
  nread = c->pfs_read(ifdes, (void *)buf, 3*ONEKB,8*ONEKB, &cache_hit);

  // All the clients open the pfs file 
  fdes = c->pfs_open(input_fname, 'w');
  if(fdes < 0)
    {
      printf("Error opening file\n");
      exit(0);
    }

  //At client 3: print the file metadata
  c->pfs_fstat(fdes, &mystat);
  printf("File Metadata:\n");
  printf("Time of creation: %s\n", ctime(&(mystat.pst_ctime)));
  printf("Time of last modification: %s\n", ctime(&(mystat.pst_mtime)));
  printf("File Size: %d\n", mystat.pst_size);
  
  //Write the next 3 kbytes of data from the input file onto pfs_file
  err_value = c->pfs_write(fdes, (void *)buf, 3*ONEKB, 8*ONEKB, &cache_hit);
  printf("Wrote %d bytes to the file\n", err_value);

  err_value = c->pfs_read(fdes, (void *)buf, 2*ONEKB, ONEKB, &cache_hit);
  printf("Read %d bytes of data from the file\n", err_value);
  printf("%s\n",buf);

  c->pfs_close(fdes);
  c->pfs_delete(input_fname);
  free(buf);
  close(ifdes);


 // c->pfs_delete(filename);
  while(1);
	return 0;
}
