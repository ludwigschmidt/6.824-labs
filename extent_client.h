// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <map>
#include "pthread.h"
#include "extent_protocol.h"
#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include "fab_client.h"

class extent_client {
 private:
  struct cache_entry {
    enum extent_server_state { EXISTS, UNKNOWN, SERVER_NOENT };
    enum extent_client_state { UNCHANGED, REMOVED, WRITTEN };
    std::string data;
    extent_protocol::attr attributes;
    extent_server_state server_state;
    extent_client_state client_state;
    bool got_get_attributes;
    bool got_put_attributes;
    bool got_data;
  };

#define FAB
#ifdef FAB
  fab_client* cl;
#else
  rpcc *cl;
#endif
  pthread_mutex_t mutex;
  typedef std::map<extent_protocol::extentid_t, cache_entry> cache_t;
  typedef std::map<extent_protocol::extentid_t, cache_entry>::iterator
      cache_iterator_t;
  cache_t cache;


 public:
  extent_client(std::string dst);

  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  extent_protocol::status flush(extent_protocol::extentid_t eid);
};


class extent_client_lock_release_user : public lock_release_user {
 private:
  extent_client* client;

 public:
  extent_client_lock_release_user(extent_client* client_)
      : client(client_) { }
  void dorelease(lock_protocol::lockid_t lid);
};

#endif 

