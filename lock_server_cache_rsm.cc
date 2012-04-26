// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


static void *
revokethread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_mutex_init(&server_mutex, NULL);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);
}

void
lock_server_cache_rsm::revoker()
{
  while (true) {
    task_entry e;
    revoke_queue.deq(&e);
    
    if (rsm->amiprimary()) {
      int r;
      //printf("sending revoke for lock %lld to %s\n", e.lid, e.id.c_str());
      handle(e.id).safebind()->call(rlock_protocol::revoke, e.lid, e.xid, r);
    }
  }
}


void
lock_server_cache_rsm::retryer()
{
  while (true) {
    task_entry e;
    retry_queue.deq(&e);

    if (rsm->amiprimary()) {
      int r;
      //printf("sending retry for lock %lld to %s\n", e.lid, e.id.c_str());
      handle(e.id).safebind()->call(rlock_protocol::retry, e.lid, e.xid, r);
    }
  }
}


int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id, 
             lock_protocol::xid_t xid, int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  std::string send_revoke_to;

  pthread_mutex_lock(&server_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    iter = locks.insert(std::make_pair(lid, lock_entry())).first;
  }
  
  lock_entry& le = iter->second;
  client_xid_map::iterator xid_iter = le.highest_xid_from_client.find(id);

  if (xid_iter == le.highest_xid_from_client.end()
      || xid_iter->second < xid) {
    le.highest_xid_release_reply.erase(id);
    le.highest_xid_from_client[id] = xid;

    if (!le.locked_by.empty()) {
      ret = lock_protocol::RETRY;
      le.waiting.insert(id);
      const std::string& client_to_revoke = le.locked_by;
      revoke_queue.enq(task_entry(client_to_revoke, lid,
          le.highest_xid_from_client[client_to_revoke]));
    } else {
      ret = lock_protocol::OK;
      iter->second.locked_by = id;
      iter->second.waiting.erase(id);

      //printf("gave lock %lld to %s\n", lid, id.c_str());

      if (!le.waiting.empty()) {
        revoke_queue.enq(task_entry(id, lid, xid));
      }
    }

    le.highest_xid_acquire_reply[id] = ret;
  } else if (xid_iter->second == xid) {
    ret = le.highest_xid_acquire_reply[id];
  } else {
    printf("ERROR: received acquire with old xid. "
        "Highest seen: %lld, current xid: %lld\n", xid_iter->second, xid);
    ret = lock_protocol::RPCERR;
  }

  pthread_mutex_unlock(&server_mutex);

  return ret;
}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
         lock_protocol::xid_t xid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;

  //printf("entry: %s release lock %lld\n", id.c_str(), lid);

  pthread_mutex_lock(&server_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    printf("ERROR: received release for non-existing lock.\n");
    ret = lock_protocol::NOENT;
  } else {
    lock_entry& le = iter->second;

    client_xid_map::iterator xid_iter = le.highest_xid_from_client.find(id);
    if (xid_iter == le.highest_xid_from_client.end()) {
      printf("ERROR: received release for lock with no recorded xid for this "
          " client.\n");
      ret = lock_protocol::RPCERR;
    } else if (xid < xid_iter->second) {
      printf("ERROR: received release with incorrect xid. "
          " xid: %lld, highest acquire xid: %lld\n", xid, xid_iter->second);
      ret = lock_protocol::RPCERR;
    } else {
      client_reply_map::iterator reply_iter =
          le.highest_xid_release_reply.find(id);
      if (reply_iter == le.highest_xid_release_reply.end()) {
        if (le.locked_by != id) {
          ret = lock_protocol::IOERR;
          le.highest_xid_release_reply.insert(make_pair(id, ret));
          printf("ERROR: received release from client not holding the lock.\n");
        } else {
          le.locked_by = "";
          le.highest_xid_release_reply.insert(make_pair(id, ret));
          //printf("%s release lock %lld\n", id.c_str(), lid);
          if (!le.waiting.empty()) {
            const std::string& client_to_wake_up = *le.waiting.begin();
            retry_queue.enq(task_entry(client_to_wake_up, lid,
                le.highest_xid_from_client[client_to_wake_up]));
          }
        }
      } else {
        ret = reply_iter->second;
      }
    }
  }

  pthread_mutex_unlock(&server_mutex);

  return ret;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  pthread_mutex_lock(&server_mutex);

  marshall rep;
  rep << locks;

  pthread_mutex_unlock(&server_mutex);

  return rep.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  pthread_mutex_lock(&server_mutex);

  unmarshall rep(state);
  rep >> locks;

  pthread_mutex_unlock(&server_mutex);
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}


marshall& operator <<(marshall& m, const lock_server_cache_rsm::lock_entry& e) {
  m << e.locked_by;
  m << e.waiting;
  m << e.highest_xid_from_client;
  m << e.highest_xid_acquire_reply;
  m << e.highest_xid_release_reply;
  return m;
}

unmarshall& operator >>(unmarshall& m, lock_server_cache_rsm::lock_entry& e) {
  m >> e.locked_by;
  m >> e.waiting;
  m >> e.highest_xid_from_client;
  m >> e.highest_xid_acquire_reply;
  m >> e.highest_xid_release_reply;
  return m;
}
