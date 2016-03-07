#include "metadata.hh"
#include "client.hh"
// Problems:
// if writing into one block, the file will not be equally distributed across servers. Do we need to re-allocate the file strips?
// lala
double MetadataManager::current_time = 0.0;


MetadataManager::MetadataManager(){
    //server_num = NUM_FILE_SERVERS;
    server_num = 0;
    real_server_num = 0;
    file_accu_id = 0;
    request_id = 0;
    cout <<"Manager UP..." <<endl;
    //memset('\0', server_connection, sizeof(server_connection));


    msgq = new priority_queue<time_pck_t, std::vector<time_pck_t>, msgq_compare> ();
    send_resp_count_list = new unordered_map<int, int> ();
    recv_resp_count_list = new unordered_map<int, int> ();     
    l = new Listener(8000);
}

int MetadataManager::create_file(string filename, double block_size, int cac_id){
    // create metadata
    cout << "creating file: " << filename << "\t" << block_size << endl;
    time_t creation_time = get_current_time();
    //cout << "current time: " << creation_time << endl;
    Metadata metadata(filename, block_size, creation_time, server_num);
    filename_metadata_hash.insert(pair<string, Metadata>(filename, metadata));
    file_accu_id ++; //  
    filename_fileid.insert(pair<string, int>(filename, file_accu_id));
    fileid_filename.insert(pair<int, string>(file_accu_id, filename));
    vector<TokenInfo> tokeninfo_vector;
    vector<TokenInfo> tokenwaiting_vector;
    this->filename_tokenAssignList_hash.insert(pair<string, vector<TokenInfo> >(filename, tokeninfo_vector));
    this->filename_tokenWaitingList_hash.insert(pair<string, vector<TokenInfo> >(filename, tokenwaiting_vector));

    auto file_recipe = metadata.file_recipe;
    int block_num = metadata.block_num;
    for(int i = 0; i < block_num; i++){
        int block_id = i;
        int stripe_id = file_recipe.blockId_stripId[block_id];
        int server_id = file_recipe.stripId_serverId[stripe_id];
        int server_connection = serverId_connectId_hash[server_id];
        int _sock = server_connection;
        struct pck_t pck;
        string op("C");
        string fn(filename);
        int bd[2] = {block_id, block_id+1};
        int sw = 0;
        int fd = filename_fileid[filename];
        struct pfs_stat ps;
        char* data = NULL;
        //en-package the pck
        Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        double _t = t.tv_sec + t.tv_nsec/BILLION;    
        time_pck_t t_pck = {_t, pck, _sock};
        (*(l->client_send_map))[_sock].push_back(t_pck);
    }

    for(int i = 0; i < server_num; i++){
        int server_connection = serverId_connectId_hash[i];
        int _sock = server_connection;
        struct pck_t pck;
        string op("C");
        string fn(filename);
        int bd[2] = {-1, -1};
        int sw = -1*cac_id;
        int fd = filename_fileid[filename];
        struct pfs_stat ps;
        char* data = NULL;
        //en-package the pck
        Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        double _t = t.tv_sec + t.tv_nsec/BILLION;    
        time_pck_t t_pck = {_t, pck, _sock};
        (*(l->client_send_map))[_sock].push_back(t_pck);
    }
    filename_actimes[filename] = server_num;
}

int MetadataManager::query_block(string filename, int block_id){
    auto & mdata = filename_metadata_hash[filename];
    auto & recipe = filename_metadata_hash[filename].file_recipe;
    int server_id = -1;
    if(recipe.blockId_stripId.find(block_id) != recipe.blockId_stripId.end()){
        int strip_id = recipe.blockId_stripId[block_id];
        if(recipe.stripId_serverId.find(strip_id)!= recipe.stripId_serverId.end()){
            server_id = recipe.stripId_serverId[strip_id];
        }
    }
    return server_id;
}

int MetadataManager::insert_block(string filename, int block_id){
    auto & mdata = filename_metadata_hash[filename];
    auto & recipe = filename_metadata_hash[filename].file_recipe;
    int server_id = -1;

    int strip_num = mdata.strip_num;
    int last_strip_id = strip_num - 1;
    int last_block_num = mdata.stripId_blockNum[last_strip_id];

    if (last_block_num < STRIP_SIZE){
        int new_block_id = mdata.block_num;
        recipe.blockId_stripId.insert(pair<int, int>(new_block_id, last_strip_id));
        mdata.block_num ++;
        server_id = recipe.stripId_serverId[last_strip_id];
        mdata.stripId_blockNum[last_strip_id] ++;
        cout << "TODO communicate with server, ask for new block" << endl;
        return server_id;
    }else{
        int new_block_id = mdata.block_num;
        int new_strip_id = mdata.strip_num;
        mdata.block_num ++;
        mdata.strip_num ++;
        int new_server_id = mdata.strip_num / server_num;
        recipe.blockId_stripId.insert(pair<int, int>(new_block_id, new_strip_id));
        recipe.stripId_serverId.insert(pair<int, int>(new_strip_id, new_server_id));
        mdata.stripId_blockNum.insert(pair<int, int>(new_strip_id, 1));
        cout << "TODO: communicate with server, ask for a new block" << endl;
        return new_server_id;
    }
}

int MetadataManager::close_file(string filename, int client_id){
    time_t _time = get_current_time();
    filename_metadata_hash[filename].modification_time = _time;
    for(auto it = filename_tokenAssignList_hash.begin(); it != filename_tokenAssignList_hash.end(); ++it){
        string filename = it->first;
        vector<TokenInfo> & token_assign_list = filename_tokenAssignList_hash[filename];
        for(int i = 0; i < token_assign_list.size(); i++){
            if(token_assign_list[i].client_id == client_id){
                token_assign_list[i].deleted = true;
            }
        }
        token_assign_list.erase( remove_if(token_assign_list.begin(), token_assign_list.end(), remove_TokenInfo),
                                token_assign_list.end());
    }

    for(auto it = filename_tokenWaitingList_hash.begin(); it != filename_tokenWaitingList_hash.end(); ++it){
        string filename = it->first;
        vector<TokenInfo> & token_assign_list = filename_tokenWaitingList_hash[filename];
        for(int i = 0; i < token_assign_list.size(); i++){
            if(token_assign_list[i].client_id == client_id){
                token_assign_list[i].deleted = true;
            }
        }
        token_assign_list.erase( remove_if(token_assign_list.begin(), token_assign_list.end(), remove_TokenInfo),
                                token_assign_list.end());
    }
}

int MetadataManager::delete_file(string filename){
    auto it = filename_metadata_hash.find(filename);
    if(it != filename_metadata_hash.end()){

        cout << "communicate with server to delete file" << endl;
        for(int i = 0; i < server_num; i++){
            int _sock = serverId_connectId_hash[i];
            struct pck_t pck;
            string op("D");
            string fn(filename);
            int bd[2] = {-1, -1};
            int sw = 0;
            int fd = filename_fileid[filename];
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);
        }

        auto ait = filename_tokenAssignList_hash.find(filename);
        if(ait != filename_tokenAssignList_hash.end()){
            for(int i = 0; i < filename_tokenAssignList_hash[filename].size(); i++){
                TokenInfo token_info = filename_tokenAssignList_hash[filename][i];
                cout << "send message to client " << token_info.client_id << " to invalidate the file" << endl;
                int _sock = token_info.client_id;
                struct pck_t pck;
                string op("I");
                string fn(filename);
                int bd[2] = {-1, -1};
                int sw = 0;
                int fd = filename_fileid[filename];
                struct pfs_stat ps;
                char* data = NULL;
                //en-package the pck
                Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
                struct timespec t;
                clock_gettime(CLOCK_REALTIME, &t);
                double _t = t.tv_sec + t.tv_nsec/BILLION;    
                time_pck_t t_pck = {_t, pck, _sock};
                (*(l->client_send_map))[_sock].push_back(t_pck);
            }
            filename_tokenAssignList_hash.erase(ait);
        }


        for(auto wit = request_waiting_list.begin(); wit != request_waiting_list.end(); wit++){
            auto token_info = *wit;
            if(token_info.filename != filename)
                continue;
            cout << "send message to client " << token_info.client_id << " saying file does not exist" << endl;
            int _sock = token_info.client_id;
            struct pck_t pck;
            string op("");
            if(token_info.mode == 'r'){
                op = "AR";
            }else{
                op = "AW";
            }
            string fn(token_info.filename);
            int bd[2] = {-1, -1};
            int sw = 0;
            int fd = filename_fileid[token_info.filename];
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);
            (*wit).deleted = true;
        }

        request_waiting_list.erase( remove_if(request_waiting_list.begin(), request_waiting_list.end(), remove_WaitingInfo),
                            request_waiting_list.end());

        auto fit = filename_fileid.find(filename);
        int fid = -1;
        if(fit != filename_fileid.end()){
            fid = filename_fileid[filename];
            filename_fileid.erase(fit);
        }

        auto iit = fileid_filename.find(fid);
        if(iit != fileid_filename.end()){
            fileid_filename.erase(iit);
        }

        filename_metadata_hash.erase(it);

        return 0;
    }else{
        return -1;
    }
    
}

void MetadataManager::register_server(int port, int connect_id){
    int server_id = real_server_num;
    serverId_connectId_hash.insert(pair<int, int>(server_id, connect_id));
    serverId_port_hash.insert(pair<int, int>(server_id, port));
    real_server_num ++;
    server_num ++;
}

BlockRange MetadataManager::intersect_two_range(BlockRange reference_range, BlockRange query_range, int & intersect_length){
    BlockRange result_range(-1, -1);
    result_range.start_index = max(reference_range.start_index, query_range.start_index);
    result_range.end_index = min(reference_range.end_index, query_range.end_index);
    intersect_length = result_range.end_index - result_range.start_index;
    return result_range;
}

bool MetadataManager::check_range_overlap(BlockRange reference_range, BlockRange query_range){
    if(min(reference_range.end_index, query_range.end_index) - max(reference_range.start_index, query_range.start_index) > 0)
        return true;
    return false;
}

vector<BlockRange> MetadataManager::intersect_range_list(vector<vector<BlockRange> > candidate_range_list_vector){
    vector<BlockRange> intersect_range_list;

    if(candidate_range_list_vector.size() == 0)
        return intersect_range_list;

    if(candidate_range_list_vector.size() == 1)
        return candidate_range_list_vector[0];

    auto previous_intersect_range_list = candidate_range_list_vector[0];

    for(int i = 1; i < candidate_range_list_vector.size(); i++){
        auto candidate_range_list = candidate_range_list_vector[i];

        for(int j = 0; j < previous_intersect_range_list.size(); j++){
            auto reference_range = previous_intersect_range_list[j];
            for(int k = 0; k < candidate_range_list.size(); k++){
                auto query_range = candidate_range_list[k];
                int intersect_length = -1;
                auto intersection = intersect_two_range(reference_range, query_range, intersect_length);
                if(intersect_length > 0){
                    intersect_range_list.push_back(intersection);
                }
            }
        }

        previous_intersect_range_list = intersect_range_list;
        intersect_range_list.clear();
    }

    return intersect_range_list;
}

vector<BlockRange> MetadataManager::union_range(vector<BlockRange> candidate_range_list){
    vector<BlockRange> union_range_list;

    if(candidate_range_list.size() == 0)
        return union_range_list;

    //sort(candidate_range_list.begin(), candidate_range_list.end()， less_than_start_index());

    BlockRange previous_range = candidate_range_list[0];
    for(int i = 1; i < candidate_range_list.size(); i++){
        BlockRange candidate_range = candidate_range_list[i];
        //assert(candidate_range.size() == 2);
        if(candidate_range.start_index <= previous_range.end_index){
            // can be unioned
            previous_range.end_index = candidate_range.end_index;
        }else{
            union_range_list.push_back(previous_range);
            previous_range = candidate_range;
        }
    }
    union_range_list.push_back(previous_range);
    return union_range_list;
}

BlockRange MetadataManager::choose_best_range(vector<BlockRange> candidate_distribute_range, BlockRange request_block_range){
    BlockRange best_range(-1, -1);
    if(candidate_distribute_range.size() == 0)
        return best_range;
    vector<BlockRange> candidate_overlapping_range;
    for(int i = 0; i < candidate_distribute_range.size(); i++){
        BlockRange candidate_range = candidate_distribute_range[i];
        if(check_range_overlap(candidate_range, request_block_range))
            candidate_overlapping_range.push_back(candidate_range);
    }
    if(candidate_overlapping_range.size() == 0)
        return best_range;

    int max_overlap_length = 0;
    int max_length_index = -1;
    for(int i = 0; i < candidate_overlapping_range.size(); i++){
        BlockRange candidate_range = candidate_overlapping_range[i];
        int candidate_range_length = candidate_range.end_index - candidate_range.start_index;
        if(candidate_range_length > max_overlap_length){
            max_overlap_length = candidate_range_length;
            max_length_index = i;
        }
    }
    return candidate_overlapping_range[max_length_index];
}

void MetadataManager::assign_tokens(const string & filename, int client_id, char mode, const BlockRange & block_range){
    TokenInfo assigned_tokens(client_id, mode, block_range);
    filename_tokenAssignList_hash[filename].push_back(assigned_tokens);
    return;
}

// if return -1, can not have that repeat
// if return 0, can have that repeat
int MetadataManager::prepare_repeat(const string & filename, const BlockRange & request_block_range, const int client_id, const int request_id){
    BlockRange empty_range(-1, -1);
    
    vector<int> responseClientList= requestId_responseClientList_hash[request_id];
    
    auto revoked_range = empty_range;
    assert(responseClientList.size() > 0);

    
    for(int i = 0; i < responseClientList.size(); i++){
        int response_client = responseClientList[i];
        //pthread_mutex_lock(&())
        if(clientId_responseinfoList_hash.find(response_client) != clientId_responseinfoList_hash.end()){

            auto response_info_list = clientId_responseinfoList_hash[response_client];
            bool has_response = false;
            for(int k = 0; k < response_info_list.size(); k++){
                auto response_info = response_info_list[k];
                if(response_info.request_id == request_id){
                    has_response = true;
                }
            }
            if(! has_response){
                return -1;
            }
        }else{
            return -1;
        }
    }
   
    return 0;
}

BlockRange MetadataManager::repeat_read(const string & filename, const BlockRange & request_block_range, const int client_id){
    cout << "read file : filename " << filename  << "\t block_range" << request_block_range.start_index << "," << request_block_range.end_index << "\t client_id" << client_id << endl;
    
    // token policy
    int start_block_index = request_block_range.start_index;
    int end_block_index = request_block_range.end_index;

    vector<TokenInfo> current_token_assign_list = filename_tokenAssignList_hash[filename];
    auto mdata = filename_metadata_hash[filename];

    // if not range given out, give out whole range
    if(current_token_assign_list.size() == 0) {
        BlockRange whole_block_range(0, mdata.block_num);
        assign_tokens(filename, client_id, 'r', whole_block_range);
        return whole_block_range;
    }

    vector<vector<BlockRange> > candidate_interset_range;

    bool restriction = false; //check 

    BlockRange empty_range(-1, -1);
    
    for(int i = 0; i < current_token_assign_list.size(); i++){
        TokenInfo token_assign = current_token_assign_list[i];
        int assign_token_class = token_assign.token_class;

        if('r' == assign_token_class){ // if assigned token is read token
            continue;
        }

        BlockRange assign_range = token_assign.block_range;
        int assign_start = assign_range.start_index;
        int assign_end = assign_range.end_index;
        int assign_client = token_assign.client_id;
        int coverage = min(assign_end, end_block_index) - max(assign_start, start_block_index);

        vector<BlockRange> free_range;
        // free_range stands for all block range that can be hand out from this client's point of view
        // free_range combine front_free_range, revoked_range and back_free_range;
        // assigned_range is blocks assigned to this client
        // front_free_range is blocks before assigned_range
        // back_free_range is blocks after assigned_range
        // revoked_range is blocks revoked from assigned_range

        if (coverage > 0){
            
            restriction = true;
            int response_client = assign_client;
            //pthread_mutex_lock(&())
            if(clientId_responseinfoList_hash.find(response_client) != clientId_responseinfoList_hash.end()){
                auto response_info_list = clientId_responseinfoList_hash[response_client];
                bool has_response = false;
                for(int k = 0; k < response_info_list.size(); k++){
                    auto response_info = response_info_list[k];
                    if(response_info.request_id == request_id){
                        has_response = true;
                        free_range.push_back(response_info.block_range);
                    }
                }
                if(! has_response){
                    return empty_range;
                }
            }else{
                return empty_range;
            }
            
            if (assign_start > 0){
                BlockRange front_free_range(0, assign_start);
                free_range.push_back(front_free_range);
            }

            if (assign_end < mdata.block_num){
                BlockRange back_free_range(assign_end, mdata.block_num);
                free_range.push_back(back_free_range);
            }

            vector<BlockRange> union_free_range = union_range(free_range);
            candidate_interset_range.push_back(union_free_range);
        }
    }

    // if does not find restriction, can not return request_block_range directly
    //if(restriction == false) return request_block_range;

    vector<BlockRange> candidate_distribute_range = intersect_range_list(candidate_interset_range);
    BlockRange result_range = choose_best_range(candidate_distribute_range, request_block_range);
    if(request_block_range.start_index <= result_range.start_index && request_block_range.end_index >= result_range.end_index){
        assign_tokens(filename, client_id, 'r', result_range);
        return result_range;
    }

    return empty_range;
}

BlockRange MetadataManager::repeat_write(const string & filename, const BlockRange & request_block_range, const int client_id){
    cout << "write file : filename " << filename  << "\t block_range" << request_block_range.start_index << "," << request_block_range.end_index << "\t client_id" << client_id << endl;
    
    // token policy
    int start_block_index = request_block_range.start_index;
    int end_block_index = request_block_range.end_index;

    vector<TokenInfo> current_token_assign_list = filename_tokenAssignList_hash[filename];
    auto mdata = filename_metadata_hash[filename];

    // if not range given out, give out whole range
    if(current_token_assign_list.size() == 0) {
        BlockRange whole_block_range(0, mdata.block_num);
        assign_tokens(filename, client_id, 'w', whole_block_range);
        return whole_block_range;
    }

    vector<vector<BlockRange> > candidate_interset_range;

    bool restriction = false; //check 

    BlockRange empty_range(-1, -1);
    
    for(int i = 0; i < current_token_assign_list.size(); i++){
        TokenInfo token_assign = current_token_assign_list[i];
        int assign_token_class = token_assign.token_class;

        BlockRange assign_range = token_assign.block_range;
        int assign_start = assign_range.start_index;
        int assign_end = assign_range.end_index;
        int assign_client = token_assign.client_id;
        int coverage = min(assign_end, end_block_index) - max(assign_start, start_block_index);

        vector<BlockRange> free_range;
        // free_range stands for all block range that can be hand out from this client's point of view
        // free_range combine front_free_range, revoked_range and back_free_range;
        // assigned_range is blocks assigned to this client
        // front_free_range is blocks before assigned_range
        // back_free_range is blocks after assigned_range
        // revoked_range is blocks revoked from assigned_range

        if (coverage > 0){
            
            restriction = true;
            int response_client = assign_client;
            //pthread_mutex_lock(&())
            if(clientId_responseinfoList_hash.find(response_client) != clientId_responseinfoList_hash.end()){
                auto response_info_list = clientId_responseinfoList_hash[response_client];
                bool has_response = false;
                for(int k = 0; k < response_info_list.size(); k++){
                    auto response_info = response_info_list[k];
                    if(response_info.request_id == request_id){
                        has_response = true;
                        free_range.push_back(response_info.block_range);
                    }
                }
                if(! has_response){
                    return empty_range;
                }
            }else{
                return empty_range;
            }
            
            if (assign_start > 0){
                BlockRange front_free_range(0, assign_start);
                free_range.push_back(front_free_range);
            }

            if (assign_end < mdata.block_num){
                BlockRange back_free_range(assign_end, mdata.block_num);
                free_range.push_back(back_free_range);
            }

            vector<BlockRange> union_free_range = union_range(free_range);
            candidate_interset_range.push_back(union_free_range);
        }
    }

    // if does not find restriction, can not return request_block_range directly
    //if(restriction == false) return request_block_range;

    vector<BlockRange> candidate_distribute_range = intersect_range_list(candidate_interset_range);
    BlockRange result_range = choose_best_range(candidate_distribute_range, request_block_range);
    if(request_block_range.start_index <= result_range.start_index && request_block_range.end_index >= result_range.end_index){
        assign_tokens(filename, client_id, 'w', result_range);
        return result_range;
    }

    return empty_range;
}


BlockRange MetadataManager::read_file(const string & filename, const BlockRange & request_block_range, const int client_id){
    // token policy
    cout << "read file : filename " << filename  << "\t block_range" << request_block_range.start_index << "," << request_block_range.end_index << "\t client_id" << client_id << endl;
    int start_block_index = request_block_range.start_index;
    int end_block_index = request_block_range.end_index;

    vector<TokenInfo> current_token_assign_list = filename_tokenAssignList_hash[filename];
    auto mdata = filename_metadata_hash[filename];
    cout << mdata.filename << "\t" << mdata.block_num << endl;

    // if not range given out, give out whole range
    if(current_token_assign_list.size() == 0) {
        cout << "get read token" << endl;
        BlockRange whole_block_range(0, mdata.block_num);
        assign_tokens(filename, client_id, 'r', whole_block_range);
        cout <<"Finish assign" <<endl;
        cout << whole_block_range.end_index << "\t" << whole_block_range.start_index << endl;
        return whole_block_range;
    }

    vector<vector<BlockRange> > candidate_interset_range;

    bool restriction = false; //check 

    request_id ++;
    vector<int> responseClientList;
    requestId_responseClientList_hash[request_id] = responseClientList;
    
    for(int i = 0; i < current_token_assign_list.size(); i++){
        TokenInfo token_assign = current_token_assign_list[i];
        int assign_token_class = token_assign.token_class;
        if('r' == assign_token_class){ // if assigned token is read token
            continue;
        }
        BlockRange assign_range = token_assign.block_range;
        int assign_start = assign_range.start_index;
        int assign_end = assign_range.end_index;
        int assign_client = token_assign.client_id;
        int coverage = min(assign_end, end_block_index) - max(assign_start, start_block_index);
        //if(client_id == assign_client)
        //    continue;

        vector<BlockRange> free_range;
        // free_range stands for all block range that can be hand out from this client's point of view
        // free_range combine front_free_range, revoked_range and back_free_range;
        // assigned_range is blocks assigned to this client
        // front_free_range is blocks before assigned_range
        // back_free_range is blocks after assigned_range
        // revoked_range is blocks revoked from assigned_range

        if (coverage > 0){
            restriction = true;
            cout << "send revoke message to client " << assign_client << endl;
            requestId_responseClientList_hash[request_id].push_back(assign_client);
            // add to send list
            
            int _sock = assign_client;
            struct pck_t pck;
            string op("V");
            string fn("r");
            int bd[2] = {request_block_range.start_index,request_block_range.end_index};
            int sw = 0;
            int fd = filename_fileid[filename];
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);


            //BlockRange revoked_range; //returned revoked token range should be assigned to this revoked range;
            //free_range.push_back(revoked_range);

            //---------------------------------------------------
        }else if(restriction = false){
            if (assign_start > 0){
                BlockRange front_free_range(0, assign_start);
                free_range.push_back(front_free_range);
            }
            if (assign_end < mdata.block_num){
                BlockRange back_free_range(assign_end, mdata.block_num);
                free_range.push_back(back_free_range);
            }
            vector<BlockRange> union_free_range = union_range(free_range);
            candidate_interset_range.push_back(union_free_range);
        }
    }

    BlockRange empty_range(-1, -1);

    if(restriction == false){
        vector<BlockRange> candidate_distribute_range = intersect_range_list(candidate_interset_range);
        BlockRange result_range = choose_best_range(candidate_distribute_range, request_block_range);
        
        if(result_range.start_index <= request_block_range.start_index && result_range.end_index >= request_block_range.end_index){
            assign_tokens(filename, client_id, 'r', result_range);
            requestId_responseClientList_hash.erase(request_id);
            request_id --;
            return result_range;
        }else{
            cout << "e in  assgin read tokens" << endl;
            WaitingInfo waiting_info(client_id, request_block_range, 'r', filename, request_id);
            request_waiting_list.push_back(waiting_info);
            return empty_range;
        }
    }else{
        // add to waiting list
        WaitingInfo waiting_info(client_id, request_block_range, 'r', filename, request_id);
        request_waiting_list.push_back(waiting_info);
    }
    
    return empty_range;
}

BlockRange MetadataManager::write_file(const string & filename, const BlockRange & request_block_range, const int client_id){
    // token policy
    cout << "read file : filename " << filename  << "\t block_range" << request_block_range.start_index << "," << request_block_range.end_index << "\t client_id" << client_id << endl;
    int start_block_index = request_block_range.start_index;
    int end_block_index = request_block_range.end_index;

    vector<TokenInfo> current_token_assign_list = filename_tokenAssignList_hash[filename];
    auto mdata = filename_metadata_hash[filename];

    // if not range given out, give out whole range
    if(current_token_assign_list.size() == 0) {
        cout << "get write token" << endl;
        BlockRange whole_block_range(0, mdata.block_num);
        assign_tokens(filename, client_id, 'w', whole_block_range);
        return whole_block_range;
    }

    vector<vector<BlockRange> > candidate_interset_range;

    bool restriction = false; //check 

    request_id ++;
    vector<int> responseClientList;
    requestId_responseClientList_hash[request_id] = responseClientList;
    
    for(int i = 0; i < current_token_assign_list.size(); i++){
        TokenInfo token_assign = current_token_assign_list[i];
        int assign_token_class = token_assign.token_class;

        BlockRange assign_range = token_assign.block_range;
        int assign_start = assign_range.start_index;
        int assign_end = assign_range.end_index;
        int assign_client = token_assign.client_id;
        //if(client_id == assign_client)
        //    continue;
        int coverage = min(assign_end, end_block_index) - max(assign_start, start_block_index);

        vector<BlockRange> free_range;
        // free_range stands for all block range that can be hand out from this client's point of view
        // free_range combine front_free_range, revoked_range and back_free_range;
        // assigned_range is blocks assigned to this client
        // front_free_range is blocks before assigned_range
        // back_free_range is blocks after assigned_range
        // revoked_range is blocks revoked from assigned_range

        if (coverage > 0){
            restriction = true;
            cout << "send revoke message to client " << assign_client << endl;
            requestId_responseClientList_hash[request_id].push_back(assign_client);
            // add to send list
            
            int _sock = assign_client;
            struct pck_t pck;
            string op("V");
            string fn("w");
            int bd[2] = {request_block_range.start_index,request_block_range.end_index};
            int sw = 0;
            int fd = filename_fileid[filename];
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);


            //BlockRange revoked_range; //returned revoked token range should be assigned to this revoked range;
            //free_range.push_back(revoked_range);

            //---------------------------------------------------
        }else if(restriction = false){
            if (assign_start > 0){
                BlockRange front_free_range(0, assign_start);
                free_range.push_back(front_free_range);
            }
            if (assign_end < mdata.block_num){
                BlockRange back_free_range(assign_end, mdata.block_num);
                free_range.push_back(back_free_range);
            }
            vector<BlockRange> union_free_range = union_range(free_range);
            candidate_interset_range.push_back(union_free_range);
        }
    }

    BlockRange empty_range(-1, -1);

    if(restriction == false){
        vector<BlockRange> candidate_distribute_range = intersect_range_list(candidate_interset_range);
        BlockRange result_range = choose_best_range(candidate_distribute_range, request_block_range);
        
        if(result_range.start_index <= request_block_range.start_index && result_range.end_index >= request_block_range.end_index){
            assign_tokens(filename, client_id, 'w', result_range);
            requestId_responseClientList_hash.erase(request_id);
            request_id --;
            return result_range;
        }else{
            cout << "xx when assgin write tokens" << endl;
            WaitingInfo waiting_info(client_id, request_block_range, 'w', filename, request_id);
            request_waiting_list.push_back(waiting_info);
            return empty_range;
        }
    }else{
        // add to waiting list
        WaitingInfo waiting_info(client_id, request_block_range, 'w', filename, request_id);
        request_waiting_list.push_back(waiting_info);
    }
    
    return empty_range;
}


int MetadataManager::file_stat(string filename, struct pfs_stat & f_stat){
    if(filename_metadata_hash.find(filename) != filename_metadata_hash.end()){
        
    }
    auto mdata = filename_metadata_hash[filename];
    f_stat.pst_mtime = mdata.modification_time; /* time of last data modification */
    f_stat.pst_ctime = mdata.creation_time; /* time of creation */
    f_stat.pst_size = mdata.block_num * PFS_BLOCK_SIZE * 1024;    /* File size in bytes */
    return 0;
}

int MetadataManager::revoke_tokens(string filename, int client_id, BlockRange block_range, string token_type){
    assert(token_type == "r" || token_type == "w");
    auto & token_assign_list = filename_tokenAssignList_hash[filename];
    for(auto it = token_assign_list.begin(); it != token_assign_list.end(); it++){
        if((*it).client_id != client_id)
            continue;
        if((*it).token_class != token_type[0])
            continue;
        if(min(block_range.end_index, (*it).block_range.end_index) - max(block_range.start_index, (*it).block_range.start_index) > 0){
            if(block_range.end_index >= (*it).block_range.end_index && block_range.start_index <= (*it).block_range.start_index){
                (*it).deleted = true;
            }else if(block_range.end_index < (*it).block_range.end_index ){
                (*it).block_range.start_index = block_range.end_index;
            }else if(block_range.start_index > (*it).block_range.start_index){
                (*it).block_range.end_index = block_range.start_index;
            }
        }
    }
    token_assign_list.erase( remove_if(token_assign_list.begin(), token_assign_list.end(), remove_TokenInfo),
                                token_assign_list.end());
}

int MetadataManager::check_server_port(const string & filename, int block_id){
    auto mdata = filename_metadata_hash[filename];
    int stripe_id;
    if(mdata.file_recipe.blockId_stripId.find(block_id) == mdata.file_recipe.blockId_stripId.end()){
        return -1;
    }
    stripe_id = mdata.file_recipe.blockId_stripId[block_id];
    auto server_id = mdata.file_recipe.stripId_serverId[stripe_id];
    int port = serverId_port_hash[server_id];
    return port;
}

void MetadataManager::_start()
{
    pthread_t listener_thd;
     struct lister_start_t l_start = {l};
    void * p = (void *)(&l_start);
    pthread_create( &listener_thd, NULL, &(Listener::start), (void *)p);
  ;
  while (1)
  {
    list<int> connecter_it;
      if (l->recv_resp_is_all_0() == true)
      {
          for(auto k = l->connecters.begin(); k != l->connecters.end(); k++)
          //for (auto & it: l->connecters)
          {
              auto & it = *k;
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
        int client_id = _sock;
        int fd = message.fd;
        int stripe_width = message.stripe_width;
        if(fileid_filename.find(fd) != fileid_filename.end()){
            filename = fileid_filename[fd];
        }
        cout << "Processing Packet From "<< _sock << " Op: " << op <<endl;
        if(op == "C"){
            // from client, create file
            cac_id ++;
            cacId_connectId[cac_id] = client_id;
            create_file(filename, block_range[1], cac_id);



            //(*(l->send_rqst_count_list))[_sock]++;
        }else if(op == "OR"){
            // from client, open file with read mode
            struct pck_t pck;
            string op("AOR");
            string fn(filename);
            int bd[2] = {0,0};
            int sw = 0;
            
            int fd = -1;
            if(filename_fileid.find(filename) != filename_fileid.end()){
                fd = filename_fileid[filename];
            }
            
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);

        }else if(op == "OW"){
            // from client, open file with write mode
            struct pck_t pck;
            string op("AOW");
            string fn(filename);
            int bd[2] = {0,0};
            int sw = 0;
            
            int fd = -1;
            if(filename_fileid.find(filename) != filename_fileid.end()){
                fd = filename_fileid[filename];
            }
            
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);

        }else if(op == "R"){
            // from client, read file
            BlockRange request_block_range(block_range[0], block_range[1]);
            BlockRange result_range = read_file(filename, request_block_range, client_id);

            if(result_range.end_index - result_range.start_index > 0){
                cout << "send token" << endl;
                // send result_range to client
                struct pck_t pck;
                string op("AR");
                string fn(filename);
                int bd[2] = {result_range.start_index,result_range.end_index};
                int sw = 0;
                int fd = filename_fileid[filename];
                struct pfs_stat ps;
                char* data = NULL;
                //en-package the pck
                Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
                struct timespec t;
                clock_gettime(CLOCK_REALTIME, &t);
                double _t = t.tv_sec + t.tv_nsec/BILLION;    
                time_pck_t t_pck = {_t, pck, _sock};
                (*(l->client_send_map))[_sock].push_back(t_pck);
            }

        }else if(op == "W"){
            // from client, write file

            BlockRange request_block_range(block_range[0], block_range[1]);
            BlockRange result_range = write_file(filename, request_block_range, client_id);
            if(result_range.end_index - result_range.start_index > 0){
                // send result_range to client
                struct pck_t pck;
                string op("AW");
                string fn(filename);
                int bd[2] = {result_range.start_index,result_range.end_index};
                int sw = 0;
                int fd = filename_fileid[filename];
                struct pfs_stat ps;
                char* data = NULL;
                //en-package the pck
                Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
                struct timespec t;
                clock_gettime(CLOCK_REALTIME, &t);
                double _t = t.tv_sec + t.tv_nsec/BILLION;    
                time_pck_t t_pck = {_t, pck, _sock};
                (*(l->client_send_map))[_sock].push_back(t_pck);
            }
        }else if(op == "L"){
            // from client, close file
            close_file(filename, client_id);
        }else if(op == "D"){
            // from client, delete file
            delete_file(filename);
        }else if(op == "S"){
            // from client, request file stat
            
            struct pck_t pck;
            string op("AS");
            string fn(filename);
            int bd[2] = {0,0};
            int sw = 0;
            int fd = filename_fileid[filename];
            struct pfs_stat ps;

            file_stat(filename, ps);
            
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);
        }else if(op == "Q"){
            // from client, find block in which server
            int server_port = check_server_port(filename, block_range[0]);
            if(server_port != -1){
                struct pck_t pck;
                string op("AQ");
                string fn(filename);
                int bd[2] = {0,0};
                int sw = server_port;
                int fd = filename_fileid[filename];
                struct pfs_stat ps;            
                char* data = NULL;
                //en-package the pck
                Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
                struct timespec t;
                clock_gettime(CLOCK_REALTIME, &t);
                double _t = t.tv_sec + t.tv_nsec/BILLION;    
                time_pck_t t_pck = {_t, pck, _sock};
                (*(l->client_send_map))[_sock].push_back(t_pck);
            }else{
                cout << "can not find server" << endl;
            }
        }else if(op == "AV"){
            // from client, response of revoke token "V"

            // calling a special function and write into special data structure
            BlockRange revoked_block_range(block_range[0], block_range[1]);
            assert(filename == "r" || filename == "w");
            string token_type = filename;
            revoke_tokens(filename, client_id, revoked_block_range, token_type);
            for(auto it = request_waiting_list.begin(); it != request_waiting_list.end(); it++){
                auto waiting_info = *it;
                if(waiting_info.filename != fileid_filename[fd])
                    continue;
                if(prepare_repeat(waiting_info.filename, waiting_info.block_range, waiting_info.client_id, waiting_info.request_id) == 0){
                    BlockRange result_range=(-1, -1);
                    if(waiting_info.mode = 'r'){
                        result_range = repeat_read(waiting_info.filename, waiting_info.block_range, waiting_info.client_id);
                    }else{
                        result_range = repeat_write(waiting_info.filename, waiting_info.block_range, waiting_info.client_id);
                    }
                    struct pck_t pck;
                    int ar_sock = waiting_info.client_id;
                    string op("AR");
                    string fn(waiting_info.filename);
                    int bd[2] = {result_range.start_index,result_range.end_index};
                    int sw = 0;
                    int fd = filename_fileid[waiting_info.filename];
                    struct pfs_stat ps;
                    char* data = NULL;
                    //en-package the pck
                    Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
                    struct timespec t;
                    clock_gettime(CLOCK_REALTIME, &t);
                    double _t = t.tv_sec + t.tv_nsec/BILLION;    
                    time_pck_t t_pck = {_t, pck, ar_sock};
                    (*(l->client_send_map))[ar_sock].push_back(t_pck);
                    request_waiting_list.erase(it);
                    break;
                }
            }
        }else if(op == "AC"){
            // from server, response of create file "C"
            cout <<"stripe_width: "<<stripe_width<<endl;
            filename = fileid_filename[fd];
            cout << filename << "\t" << filename_actimes[filename] << endl;
            filename_actimes[filename] --;
            if(filename_actimes[filename] == 0){
                assert(stripe_width < 0);
                int ac_sock = cacId_connectId[-1*stripe_width];
                struct pck_t pck;
                string op("AC");
                string fn(filename);
                int bd[2] = {0,0};
                int sw = 0;
                int fd = filename_fileid[filename];
                struct pfs_stat ps;
                char* data = NULL;
                //en-package the pck
                Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
                struct timespec t;
                clock_gettime(CLOCK_REALTIME, &t);
                double _t = t.tv_sec + t.tv_nsec/BILLION;    
                time_pck_t t_pck = {_t, pck, ac_sock};
                (*(l->client_send_map))[ac_sock].push_back(t_pck);
            }

        }else if(op == "G"){
            // from server, register with its port number
            int port_number = fd;
            cout<<"Recived Server Port: "<<port_number<<endl;
            register_server(port_number, _sock);
            struct pck_t pck;
            string op("AG");
            string fn(filename);
            int bd[2] = {0,0};
            int sw = 0;
            int fd = filename_fileid[filename];
            struct pfs_stat ps;
            char* data = NULL;
            //en-package the pck
            Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);
            struct timespec t;
            clock_gettime(CLOCK_REALTIME, &t);
            double _t = t.tv_sec + t.tv_nsec/BILLION;    
            time_pck_t t_pck = {_t, pck, _sock};
            (*(l->client_send_map))[_sock].push_back(t_pck);
            // finished
        }
      }
      for(auto ff = connecter_it.begin(); ff != connecter_it.end(); ff++){
      //for (auto & it: connecter_it){
       auto it = *ff;
       (*(l->recv_resp_count_list))[it] = (*recv_resp_count_list)[it];
      }
      delete(msgq);
      msgq = new priority_queue<time_pck_t, std::vector<time_pck_t>, msgq_compare> ();

  }
}

int main(){
    cout << "Hello, World" << endl;

    MetadataManager * mm = new MetadataManager();
    mm->_start();
/*    mm->create_file("1", 10);
    BlockRange br(1,2);
    BlockRange get = mm->read_file("1", br, 1);
    cout << get.start_index << "\t" << get.end_index << endl;

    br = BlockRange(5,6);
    get = mm->write_file("1", br, 2);
    cout << get.start_index << "\t" << get.end_index << endl;
    
    mm->close_file("1", 1);

    auto token_list = mm->filename_tokenAssignList_hash["1"];

    cout << "F haibo" << endl;
*/
}
