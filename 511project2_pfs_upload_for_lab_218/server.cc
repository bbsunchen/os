#include "server.hh"
#include "pck_util.hh"

string to_to_string(int & i){
    int j = i;
    return std::to_string((long long int)j);
}

Server::Server(int ipport){
    myPort = 	ipport;
	l = new Listener(myPort);
    ipport++;
    c = new Connecter(SERVER_IP____, 8000);
    msgq = new priority_queue<time_pck_t, std::vector<time_pck_t>, msgq_compare> ();
    send_resp_count_list = new unordered_map<int, int> ();
    recv_resp_count_list = new unordered_map<int, int> ();    

}

void * Server::start()
{
  int rc ;
  pthread_t listener_thd;
  struct lister_start_t l_start = {l};
  void * p = (void *)(&l_start);
  if ((rc = pthread_create( &listener_thd, NULL, &(Listener::start), (void *)p))  != 0)
  	cout<<"Server Listener  ERROR: "<<errno<<endl;

  //
  
  pthread_t connecter_thd;
  struct client_start_t c_start = {c};
  p = (void *)(&c_start);

  if ((rc = pthread_create( &connecter_thd, NULL, &(Connecter::start), (void *)p) )  != 0)
    cout<<"Server Connecter ERROR: "<<errno<<endl;
sleep(5);
  struct pck_t pck;

  string op("G");
  string fn("");
  int bd[2] = {0,0};
  int sw = 0;
  int fd = myPort;//filename_fileid[filename];
  struct pfs_stat ps;
  char* data = NULL;
  Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);

  void* send_pck = (void*)(&pck);
  c->_send(c, send_pck, sizeof(pck_t));


  while (1)
  {
  	//From Clients:
    list<int> connecter_it;
      if (l->recv_resp_is_all_0() == true)
      {
          for(auto ff = l->connecters.begin(); ff != l->connecters.end(); ff++)
          //for (auto & it: l->connecters)
          {
              auto it = *ff;
              (*recv_resp_count_list)[it] = (*(l->recv_rqst_count_list))[it];
              auto recv_it = (*(l->client_recv_map))[it].begin();
              auto recv_end = (*(l->client_recv_map))[it].end();
              int i = (*recv_resp_count_list)[it];
              for (recv_it;i > 0 || recv_it != recv_end; recv_it++, i--)
                msgq->push(*recv_it);
            connecter_it.push_back(it);
          }
      } // recv
      else{
        continue;
      } 
      
      // compute
      while (msgq->empty() == false)
      {

        auto & it = msgq->top();  msgq->pop();
        auto message = (it).pck;
        string op = string(message.opcode);
        string filename = string(message.filename);
        int* block_range = message.blockId;
        int _sock = (it).connectid;
        cout <<"I am Server" << myPort<< "--- Processing Packet From Client "<< _sock << " Op: " << op <<endl;
        if(op == "F"){
            // from client, create file
            //create_file(filename, block_range[1]);
            struct pck_t pck;

            read_block(message.fd, message.blockId[0], message.data);
            string op("AF");
            string fn("");
            int bd[2] = {message.blockId[0],message.blockId[1]};
            int sw = 0;
            int fd = message.fd;//filename_fileid[filename];
            struct pfs_stat ps;
            char* data = message.data;
            //en-package the pck

            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);
            //(*(l->send_rqst_count_list))[_sock]++;
        } else if(op == "W"){
            write_block(message.fd, message.blockId[0], message.data);
            string op("AW");
            string fn("");
            int bd[2] = {message.blockId[0],message.blockId[1]};
            int sw = 0;
            int fd = message.fd;//filename_fileid[filename];
            struct pfs_stat ps;
            char* data = message.data;
            //en-package the pck

            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);
        }
      }
      for(auto ff = connecter_it.begin(); ff != connecter_it.end(); ff++){
      //for (auto & it: connecter_it){
        auto it = *ff;
       (*(l->recv_resp_count_list))[it] = (*recv_resp_count_list)[it];
      }
      delete(msgq);
      msgq = new priority_queue<time_pck_t, std::vector<time_pck_t>, msgq_compare> ();


      //From MetaData:
      	c->_recv(c);
     	auto pck_recv = c->getrecv(c);

      	
     	if (pck_recv != NULL) 
		  {
     	  pck_t message = (*((struct pck_t*)pck_recv));
        string op = string(message.opcode);
       	string filename = string(message.filename);
       	int* block_range = message.blockId;
     		
			  if(op == "C"){ 
            // from client, create file
            //create_file(filename, block_range[1]);
        	if (message.stripe_width < 0){
            struct pck_t pck;
			      cout <<"Received From Meta "<<" : " << message.opcode<<endl;

            string op("AC");
            string fn("");
            int bd[2] = {block_range[0],block_range[1]};
            int sw = message.stripe_width;
            int fd = message.fd;//filename_fileid[filename];
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck

            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
  			    void* send_pck = (void*)(&pck);
  			     c->_send(c, send_pck, sizeof(pck_t));
     		    cout <<"Sent To Meta "<<" : " << pck.opcode<<endl;

        	} else{
            	for (int bid = block_range[0]; bid < block_range[1]; bid++)
        			create_block(message.fd, bid);
        	}	
     		} 
        else if(op == "ADD"){ 
/*            struct pck_t pck;
            cout <<"Deleted From Meta "<<" : " << message.opcode<<endl;

            //en-package the pck
 			delete_file(message.fd);
            */
        } 
        else if(op == "D"){ 
            struct pck_t pck;
            cout <<"Deleted From Meta "<<" : " << message.opcode<<endl;

            //en-package the pck
 			delete_file(message.fd);
            
        } 
     		else if(op == "AG")
     		{
				cout <<"Received From Meta "<<" : " << message.opcode<<endl;
     		}
            // from client, open file with write mode
      }

	}
  return NULL;
}

string Server::create_key(int file_id, int block_id){
	return to_to_string(file_id)+"_"+to_to_string(block_id);
}

int Server::create_block(int file_id, int block_id){
	string block_key = create_key(file_id, block_id);

	if(block_context_hash.find(block_key) == block_context_hash.end()){
		cout << "Error when creating block, block already exists" << endl;
		return -1;
	}

	block_context_hash.insert(pair<string, char*>(block_key, new char[PFS_BLOCK_SIZE_BYTE]));
	return 0;
}

int Server::delete_file(int file_id){
  for (auto it = block_context_hash.begin(); it != block_context_hash.end();) {
    string key = it->first;
    string id = split(key, '_')[0];
    string delete_id = to_to_string(file_id);
    if(id == delete_id) {
      it = block_context_hash.erase(it);
    }
    else{
      it++;
    }
  }
  return 0;
}

int Server::delete_block(int file_id, int block_id){
	string block_key = create_key(file_id, block_id);
	if(block_context_hash.find(block_key) != block_context_hash.end()){
		cout << "Error when delete block, block not exists" << endl;
		return -1;
	}
	delete block_context_hash[block_key];
	block_context_hash.erase(block_key);
	return 0;
}

int Server::write_block(int file_id, int block_id, char* input){

	string block_key = create_key(file_id, block_id);
	
	if(block_context_hash.find(block_key) != block_context_hash.end()){
		cout << "Error when write block, block not exists" << endl;
		return -1;
	}
	memcpy(block_context_hash[block_key], input, PFS_BLOCK_SIZE_BYTE);
	return 0;
}

int Server::read_block(int file_id, int block_id, char* output){
	// read file

	string block_key = create_key(file_id, block_id);

	if(block_context_hash.find(block_key) != block_context_hash.end()){
		cout << "Error when read block, block not exists" << endl;
		return -1;
	}

	output = block_context_hash[block_key];
	return 0;
}

int main(int argc, char *argv[]){

/*	cout << "h,w" << endl;
	cout << "connect to metadata manager" << endl;
	// message from metadata is from the connection in metadata

	cout << "open socket and receive message from client" << endl;*/
  if (argc < 2)
  {
      printf("usage: a.out <input filename>\n");
      exit(0);
  }
  int ipport;
  string port(argv[1]);
 ipport = atoi(port.c_str());
	Server* s = new Server(ipport);
	s->start();
	return 0;
}
