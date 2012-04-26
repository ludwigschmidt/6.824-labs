#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>
#include <map>
#include <set>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"
#include "rpc/fifo.h"

class lock_server_cache_rsm : public rsm_state_transfer {
 public:
  typedef std::map<std::string, int> client_reply_map;
  typedef std::set<std::string> client_set;
  typedef std::map<std::string, lock_protocol::xid_t> client_xid_map;

  struct lock_entry {
    std::string locked_by;
    client_set waiting;
    client_xid_map highest_xid_from_client;
    client_reply_map highest_xid_acquire_reply;
    client_reply_map highest_xid_release_reply;

    lock_entry() {}
  };

  struct task_entry {
    std::string id;
    lock_protocol::lockid_t lid;
    lock_protocol::xid_t xid;
    task_entry(const std::string& id_ = "", lock_protocol::lockid_t lid_ = 0,
        lock_protocol::xid_t xid_ = 0) : id(id_), lid(lid_), xid(xid_) {}
  };

 private:
  int nacquire;
  class rsm *rsm;

  pthread_mutex_t server_mutex;
  typedef std::map<lock_protocol::lockid_t, lock_entry> lock_map;
  lock_map locks;
  fifo<task_entry> retry_queue;
  fifo<task_entry> revoke_queue;

 public:
  lock_server_cache_rsm(class rsm *rsm = 0);
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void retryer();
  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, 
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

template <class C> marshall &
operator<<(marshall &m, std::set<C> s)
{
	m << (unsigned int) s.size();
	for (typename std::set<C>::const_iterator iter = s.begin();
      iter != s.end(); ++iter) {
    m << *iter;
  }
	return m;
}

template <class C> unmarshall &
operator>>(unmarshall &u, std::set<C> &s)
{
	unsigned n;
	u >> n;
	for(unsigned i = 0; i < n; i++){
		C z;
		u >> z;
		s.insert(z);
	}
	return u;
}

marshall& operator <<(marshall& m, const lock_server_cache_rsm::lock_entry& e);
unmarshall& operator >>(unmarshall& m, lock_server_cache_rsm::lock_entry& e);


#endif
