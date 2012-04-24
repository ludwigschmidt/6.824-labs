#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
  struct lock_entry {
    std::string locked_by;
    bool revoked;
    typedef std::set<std::string> client_set;
    client_set waiting;
    lock_entry(): revoked(false) {}
  };

 private:
  int nacquire;
  pthread_mutex_t server_mutex;
  typedef std::map<lock_protocol::lockid_t, lock_entry> lock_map;
  lock_map locks;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
