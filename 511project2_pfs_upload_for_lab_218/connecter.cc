  #include "connecter.hh"
#include <sys/unistd.h>
#include <sys/fcntl.h>
  ChildListener::ChildListener(){}
  ChildListener::ChildListener(int connect_fd, Listener *l){
    _sock = connect_fd;
    _recv_ = (void *)malloc(sizeof(pck_t));    
    packet_size = sizeof(pck_t);
    _l = l;
  }
  ChildListener::~ChildListener(){
    close(_sock);
  }
   void  ChildListener::_send(sock_pck_Map* send_map){
    if ((*send_map)[_sock].size() == 0) 
      return;
    _pck = (((*send_map)[_sock]).front()).pck; ((*send_map)[_sock]).pop_front();
    _recv_ = (void*)(&(_pck));
    if(send(_sock, _recv_, packet_size, 0) == -1)
        perror("send error");

    cout<<"Send to "<< _sock << _pck.opcode<<endl;
  }
  void  ChildListener::_reduction(struct cl_t cl_str)
  {
      if ((*(cl_str.recv_resp_count_list))[_sock] > 0)
      {
        for (int i = (*(cl_str.recv_resp_count_list))[_sock]; i > 0 ; i --)
        {
          (*cl_str.recv_map)[_sock].pop_front();
        }
        (*(cl_str.recv_rqst_count_list))[_sock] -= (*(cl_str.recv_resp_count_list))[_sock];
        (*(cl_str.recv_resp_count_list))[_sock] = 0;
      }
  }
  void   ChildListener::_recv(sock_pck_Map* recv_map, sock_count_Map* recv_rqst_count_list){
    if((read_size = recv(_sock , _recv_ , sizeof(struct pck_t), 0)) == -1) {
      if (errno == EINTR || errno == EAGAIN) 
      return ; // try again
      else {
        perror("recv error");
        assert(0);
      }
    }
    string str((*((struct pck_t*)_recv_)).filename);
    string printstr("print_recv");
    string printallstr("print_all");
    if (read_size == 0) return ;
    cout << (*((struct pck_t*)_recv_)).filename <<"Recvied" <<endl;
    if (printstr.compare(str) == 0)
      print_recv_table(_sock, recv_map);
    if (printallstr.compare(str) == 0)
      print_recv_table(recv_map);

    cout<<"Recv Pack from "<<_sock<<endl;
    struct pck_t tmp_pck = (*((struct pck_t*)_recv_));
    struct timespec t;
    
    clock_gettime(CLOCK_REALTIME, &t);
    double _t = t.tv_sec + t.tv_nsec/BILLION;    
    
    time_pck_t t_pck = {_t, tmp_pck, _sock};
    
    ((*recv_map)[_sock]).push_back(t_pck);
    (*recv_rqst_count_list)[_sock] ++; 
  }
  void*   ChildListener::_start(void * p){
    struct cl_t cl_str = *((struct cl_t *) p);
    ChildListener * c = cl_str.cl;
    while (1){
      c->_send(cl_str.send_map);
      c->_recv(cl_str.recv_map, cl_str.recv_rqst_count_list);
      c->_reduction(cl_str);

    }
    return NULL;
  }
   void ChildListener::print_recv_table(int sock, sock_pck_Map*  recv_map)
  {
    auto it = ((*recv_map)[sock]).begin();
    for (;it != ((*recv_map)[sock]).end();it++)
      cout<< (it->pck).filename<<endl;
    return;
  }

  void ChildListener::print_recv_table(sock_pck_Map*  recv_map)
  {
    for(auto it = recv_map->begin(); it != recv_map->end(); ++it){
      auto sock = it->first;
      auto itt = ((*recv_map)[sock]).begin();
        for (;itt != ((*recv_map)[sock]).end();itt++)
       cout<< (itt->pck).filename<<endl;
    }

    return;
  }

  void* Listener::_start(Listener * l)
  {
    int c = sizeof(struct sockaddr_in);
    printf("======waiting for client's request======\n");
    while((connect_fd = accept((l->socket_fd), (struct sockaddr *)&client, (socklen_t*)&c)) != -1){
      cout << "Connection accepted: " << connect_fd << endl;
      fcntl(connect_fd, F_SETFL, O_NONBLOCK);
      //locking and try accept connection from client
      pthread_mutex_t c_mutex, sc_mutex, rc_mutex;
      pthread_mutex_init(&c_mutex, NULL);
/*      pthread_mutex_init(&sc_mutex, NULL);
      pthread_mutex_init(&rc_mutex, NULL);   */   
      ChildListener* cl = new ChildListener(connect_fd, this);
      listeners.push_back(cl);
      connecters.push_back(connect_fd);

      connection_lock_map[connect_fd] = c_mutex;
/*      (*send_count_lock_map)[connect_fd] = sc_mutex;
      (*recv_count_lock_map)[connect_fd] = rc_mutex;*/

      (*send_resp_count_list)[connect_fd] = 0;
      (*send_rqst_count_list)[connect_fd] = 0;
      (*recv_resp_count_list)[connect_fd] = 0;
      (*recv_rqst_count_list)[connect_fd] = 0;
      
      list<time_pck_t>  recv_queue;
      list<time_pck_t>  send_queue;
      struct cl_t cl_str;
      cl_str = {cl,
                client_send_map,
                client_recv_map,
                  send_resp_count_list ,  
              send_rqst_count_list ,
    recv_resp_count_list ,   
    recv_rqst_count_list    
               };
    
      client_send_map->insert(pair<int, list<time_pck_t> >(connect_fd, send_queue));
      client_recv_map->insert(pair<int, list<time_pck_t> >(connect_fd, recv_queue));
      sock_cl_map.insert(pair<int, ChildListener*>(connect_fd, cl));

      if( pthread_create( &thread_id , NULL , &(ChildListener::_start), (void *)(&cl_str)) < 0){
          printf("accept socket error: %s(errno: %d)",strerror(errno),errno);
          assert(0);
      }
      cout << "Connection Handled" << endl;
    }
    return NULL;
  }

   void* Listener::start(void * p)
  {
    struct lister_start_t* listener_start = (struct lister_start_t*)p;
    Listener * l = listener_start->l;

    return l->_start(l);
  }  


    Connecter::Connecter(string ipAddr, int ipPort){

      sem_init(&send_lock, 0, 0);
      if( (sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        printf("create socket error: %s(errno: %d)\n", strerror(errno),errno);
        assert(0);
      }
    
      pck_size = sizeof(struct pck_t);
      memset(&servaddr, 0, sizeof(servaddr));
      servaddr.sin_family = AF_INET;
      servaddr.sin_port = htons(ipPort);
    
      //inet_pton is a function under linux, convert ip to integer.
    
      if(inet_pton(AF_INET, ipAddr.c_str(), &servaddr.sin_addr) <= 0){
          printf("inet_pton error for %s\n",ipAddr.c_str());
          assert(0);
        }
       _recv_ = (void*) malloc(sizeof(pck_t));  
       coming = false;
    }

    Connecter::~Connecter(){
      close(sockfd);
    }
    void *  Connecter::start( void * p){
      struct client_start_t * c_start = ( struct client_start_t *) p;
       Connecter * c = c_start->c;
      return c->_start(c_start);
    }
    void* Connecter::_start(struct client_start_t * c_start){
      cout<<"Connecter Start"<< endl;
      Connecter * l = c_start->c;
      if(connect(l->sockfd, (struct sockaddr*)&(l->servaddr), sizeof(l->servaddr)) < 0){
        printf("connect error: %s(errno: %d)\n",strerror(errno),errno);
        assert(0);
      }
      fcntl(l->sockfd, F_SETFL, O_NONBLOCK);      
      while (1)
      {
        sem_wait(&(l->send_lock));
          l->_send_(l, (l->sflit).p, (l->sflit).s);
      }
      return NULL;
    }
    void* Connecter::start_connection(Connecter * l ){
      cout<<"Connecter Start"<< endl;
      //Connecter * l = c_start->c;
      if(connect(l->sockfd, (struct sockaddr*)&(l->servaddr), sizeof(l->servaddr)) < 0){
        printf("connect error: %s(errno: %d)\n",strerror(errno),errno);
        assert(0);
      }
      fcntl(l->sockfd, F_SETFL, O_NONBLOCK);      
      return NULL;
    }      
    void* Connecter::_start(Connecter * l ){
      cout<<"Connecter Start"<< endl;
      //Connecter * l = c_start->c;
      if(connect(l->sockfd, (struct sockaddr*)&(l->servaddr), sizeof(l->servaddr)) < 0){
        printf("connect error: %s(errno: %d)\n",strerror(errno),errno);
        assert(0);
      }
      fcntl(l->sockfd, F_SETFL, O_NONBLOCK);      
      while (1)
      {
        sem_wait(&(l->send_lock));
          l->_send_(l, (l->sflit).p, (l->sflit).s);
      }
      return NULL;
    }    
    void Connecter::_send_(Connecter* c, void * p, size_t size)
    {
      if(send(c->sockfd, p, size, 0) < 0)
      {
        printf("send msg error: %s(errno: %d)\n", strerror(errno), errno);
        assert(0);
      }
    }

    void Connecter::_send(Connecter* c, void * p, size_t size)
    {
      c->sflit = {p, size};
      sem_post(&(c->send_lock));
    }
    void Connecter::_recv(Connecter* c)
    {
     // if (c->coming) return ;
      if((rec_len = recv(c->sockfd, c->_recv_, sizeof(struct pck_t), 0)) == -1) {
        if (errno == EINTR || errno == EAGAIN) 
        {
          c->coming = false;
          return ; // try again
        }
        else {
          perror("recv error");
          assert(0);
       }
      }
     // pck_t pck = (*(pck_t*)(c->_recv_));
      //cout <<"ALMOST RECIVED: "<< pck.opcode<<endl;
      c->coming = true;
    }
    void * Connecter::getrecv(Connecter* c)
    {
      if (c->coming) return c->_recv_;
      else return NULL;
    }  