 /*******************************************************************************
 * File:     serial.c
 * PFS. CSE 511 Project II
 *
 * All parallel code are summed up in these two words: sem_wait and sem_post!
 * 
 * Copyright (C) Dec.2015 Haibo, Chen
 *
 * This program is not free software: you can not redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, neither version 3 of the License, nor
 * (at your option) any later version.
 * 
 * This program is not distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details.
 * 
 * You will not receive a copy of the GNU General Public License
 * along with this program. 
 *
 * Contact Information:
 * Haibo <haibo@cse.psu.edu>
 * 111 IST, University Park, PA, 16302
 *
 ******************************************************************************/
#include "connecter.hh"
#include "client.hh"

#include <semaphore.h>
#include <algorithm>    // std::find
#include <sstream>
LRUCache::LRUCache(int capacity = CACHE_SIZE) {
this->capacity = capacity;
}

string to_to_string(int & i){
    long long int j = i;
    return to_string(j);
}

bool LRUCache::checkIfHit(fkey_t key)
{
	string  k= (to_to_string(key.fd)  + "B" + to_to_string(key.bid));		
	auto curr  = find (cacheList.begin(), cacheList.end(), k);	
	if (curr == cacheList.end()) return false;
	else return true;
}
/**100% hit*/
/*
 *  read when it has token
 * if (checkifhit() ==true)
 * 	read(key)
 * if (checkifhit() == false)
 *  {
 *		if (findfromserver(key, block) == true)
			{
				wrtie (key, block);
				read (key);
			}
		else  (findfromserver(key, block) == false)
			assert(0);	
 *	}
 */

fileblock_t LRUCache::read(fkey_t key) {
	string  k = (to_to_string(key.fd) + "B" + to_to_string(key.bid));
	auto curr  = find (cacheList.begin(), cacheList.end(), k);
	if (curr == cacheList.end())// miss
	{ 
		assert(0);
	}
	// if there is a hit, update the node, by moving it to the head of list.
	// and refresh the index in the map.
	
	cacheList.erase(curr);
	cacheList.push_back(k);
	return cacheMap[k].value;
}
/**100% hit*/
/*
 * write when it has token
 * if (checkifhit() ==true)
 * 	write(key, block, 0)
 * if (checkifhit() == false)
 *  {
 *		if (findfromserver(key, block) == true)
			{
				wrtie (key, block);
				write (key, value);
			}
		else  (findfromserver(key, block) == false)
			assert(0);	
 *	}
 */
void LRUCache::write(fkey_t key, fileblock_t value) {
	std::stringstream ss;
	ss << key.fd << "B" << key.bid;
	string  k = ss.str() ;
	auto it  = find (cacheList.begin(), cacheList.end(), k);

	if (it == cacheList.end()) { // if write cache miss
		if (cacheList.size() == capacity) { 
			// remove the last element.
			assert(0);//
/*			fkey_t lastk = cacheList.back().key;
			string  lastkstr(to_string(lastk.fd) + to_string(lastk.bid));				
			cacheMap.erase(lastkstr);
			cacheList.pop_back();*/
		}
		// insert new element to the list head, and update it in the map
		cacheList.push_back(k);
		cacheMap[k] = CacheNode(key, value);
		cleanList.push_back(k);
		capacity --;
		cacheMap[k].status = kCLEAN; 		
		if (capacity <= LOWER_THRESHOLD)
			sem_post(&(clt->write_finish));
		
	} else { // dirty write
	 	fileblock_t tmp_v = cacheMap[k].value;
	 	//memcpy(tmp_v.data + value.off[0], value.data + value.off[0], value.off[1] - value.off[0]);
		// update the block, move it the the head, and update it in the map
		tmp_v = value;
		cacheMap[k].value = tmp_v;

		cacheList.erase(it);
		cacheList.push_back(k);

		cacheMap[k].status = kDIRTY; 
		cleanList.remove(k);
		dirtyList.push_back(k);

	}
}


void * PFSCache::_harvester()
{
	while (1)
	{
		sem_wait(&(clt->write_finish));

		auto it = cacheList.begin();
		while (it != cacheList.end()  && (capacity < UPPER_THRESHOLD))
		{
			if(cacheMap[*it].status == kCLEAN) 
			{
				cacheMap.erase(*it);
				cacheList.erase(it++);
				capacity++;
			} else {
				++it;
			} 

		}
		
	}
	return NULL;
}
void * PFSCache::_flush()
{		
	while(1){
		sleep(30);
		cout<<"FLUSHing..Working..."<<endl;
		for (auto it = dirtyList.begin(); it != dirtyList.end(); ++it)
		{	
			auto cn = cacheMap[*it];
			_populate(cn.key, cn.value); 
			cn.status = kCLEAN;
			cleanList.push_back(*it);
		}
		dirtyList.clear();
		cout<<"FLUSHing..Completed."<<endl;
	}
	return NULL;
}	
void* PFSCache::harvester(void * cache)
{

	return (((PFSCache *)cache)->_harvester());			
}

void* PFSCache::flush(void * cache)
{

	return (((PFSCache *)cache)->_flush());
}

PFSCache::PFSCache(Client * _client)
{
	clt = _client;
	pthread_create( &_harv_thd, NULL, &PFSCache::harvester, (void *)this);
	pthread_create( &_flush_thd, NULL, &PFSCache::flush, (void *)(this));
}
void PFSCache::_send_fileserver(int _sock, fkey_t key, fileblock_t value)
{
	pck_t pck;
	string op("W");
 	string fn("");
 	int bd[2] = {key.bid, key.bid};
 	int sw = NA;
 	int fd = key.fd;
 	struct pfs_stat ps;
 	char* data = value.data;

 	Pck_Ut::set_pck(&pck, op, fn, bd, sw, fd, &ps, data);

 	void * _recv_ = (void *) (&pck);
 	int packet_size = sizeof(pck_t);
	if(send(_sock, _recv_, packet_size, 0) == -1)
     	perror("send error");
}
void PFSCache::_populate(fkey_t key, fileblock_t value)
{
	int fd = key.fd;
	int bid = key.bid;
	string  k = (to_to_string(key.fd) + "B" + to_to_string(key.bid));
	string fs_ip = (clt->fb_server_hm)[k];
	int fs_sock =  (clt->server_ip_socket_hm)[fs_ip];
	_send_fileserver(fs_sock, key, value);
}

void PFSCache::del(int fd)
{
	string fd_str =to_to_string(fd)+"B";
	for ( auto it = cacheList.begin(); it != cacheList.end(); )
	{
    	string key = *it;
  		std::size_t found = key.find(fd_str);
  		if (found!=std::string::npos)
   		 {
        	it = cacheList.erase(it);
			dirtyList.remove(key);
			cleanList.remove(key);	
			cacheMap.erase(key);
			capacity++;
   		 }
   		 else
        	++it;
	}
}
