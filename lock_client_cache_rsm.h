// lock client interface.

#ifndef lock_client_cache_rsm_h

#define lock_client_cache_rsm_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"
#include "rpc/fifo.h"

#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};


class lock_client_cache_rsm;

// Clients that caches locks.  The server can revoke locks using 
// lock_revoke_server.
class lock_client_cache_rsm : public lock_client {
  enum lock_state { NONE, ACQUIRING, LOCKED, FREE, RELEASING };
  struct lock_entry {
    lock_state state;
    bool retry;
    bool revoked;
    lock_protocol::xid_t xid;
    lock_entry(): state(NONE), retry(false), revoked(false), xid(0) {}
  };
  struct release_entry {
    lock_protocol::lockid_t lid;
    lock_protocol::xid_t xid;
    release_entry(lock_protocol::lockid_t lid_ = 0,
        lock_protocol::xid_t xid_ = 0) : lid(lid_), xid(xid_) {}
  };

 private:
  rsm_client *rsmc;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  lock_protocol::xid_t xid;

  pthread_mutex_t client_mutex;
  pthread_cond_t acquire_cv;
  typedef std::map<lock_protocol::lockid_t, lock_entry> lock_map;
  lock_map locks;
  fifo<release_entry> release_queue;

 public:
  static int last_port;
  lock_client_cache_rsm(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache_rsm() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  virtual lock_protocol::status release(lock_protocol::lockid_t);
  void releaser();
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
				        lock_protocol::xid_t, int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
				       lock_protocol::xid_t, int &);
};


#endif
