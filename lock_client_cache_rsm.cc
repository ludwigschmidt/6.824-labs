// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "rsm_client.h"

static void *
releasethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  xid = 0;

  pthread_mutex_init(&client_mutex, NULL);
  pthread_cond_init(&acquire_cv, NULL);

  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  VERIFY (r == 0);
}


void
lock_client_cache_rsm::releaser()
{
  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.

  while (true) {
    release_entry e;
    release_queue.deq(&e);

    if (lu != NULL) {
      lu->dorelease(e.lid);
    }

    int r;
    rsmc->call(lock_protocol::release, e.lid, id, e.xid, r);

    pthread_mutex_lock(&client_mutex);
    lock_map::iterator iter = locks.find(e.lid);
    iter->second.state = NONE;
    pthread_cond_broadcast(&acquire_cv);
    pthread_mutex_unlock(&client_mutex);
  }
}


lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
  bool have_lock = false;
  bool error = false;
  int ret = lock_protocol::OK;

  pthread_mutex_lock(&client_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    iter = locks.insert(std::make_pair(lid, lock_entry())).first;
  }

  do {
    if (iter->second.state == NONE
        || (iter->second.state == ACQUIRING && iter->second.retry)) {
      iter->second.retry = false;
      iter->second.state = ACQUIRING;
      iter->second.xid = xid;
      lock_protocol::xid_t current_xid = iter->second.xid;
      ++xid;

      pthread_mutex_unlock(&client_mutex);

      int r;
      lock_protocol::status rpcret = rsmc->call(lock_protocol::acquire, lid, id,
          current_xid, r);

      pthread_mutex_lock(&client_mutex);

      if (rpcret == lock_protocol::OK) {
        iter->second.state = LOCKED;
        have_lock = true;
      } else if (rpcret == lock_protocol::RETRY) {
      } else {
        error = true;
        ret = rpcret;
      }
    } else if (iter->second.state == FREE) {
      iter->second.state = LOCKED;
      have_lock = true;
    } else if (iter->second.state == LOCKED 
        || (iter->second.state == ACQUIRING && !iter->second.retry)
        || iter->second.state == RELEASING) {
      pthread_cond_wait(&acquire_cv, &client_mutex);
    }

  } while (!have_lock && !error);

  pthread_mutex_unlock(&client_mutex);

  return ret;
}


lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;

  pthread_mutex_lock(&client_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    printf("ERROR: Called release for lock id not seen before.");
    ret = lock_protocol::NOENT;
  } else {
    if (iter->second.revoked) {
      iter->second.state = RELEASING;
      iter->second.revoked = false;

      lock_protocol::xid_t xid = iter->second.xid;

      pthread_mutex_unlock(&client_mutex);

      if (lu != NULL) {
        lu->dorelease(lid);
      }

      int r;
      ret = rsmc->call(lock_protocol::release, lid, id, xid, r);

      pthread_mutex_lock(&client_mutex);
      iter->second.state = NONE;
      pthread_cond_broadcast(&acquire_cv);
    } else {
      iter->second.state = FREE;
      pthread_cond_broadcast(&acquire_cv);
    }
  }

  pthread_mutex_unlock(&client_mutex);

  return ret;
}


rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
			          lock_protocol::xid_t xid, int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&client_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    printf("ERROR: Got revoke for lock id not seen before.");
  } else if (iter->second.xid == xid) {
    if (iter->second.state == FREE) {
      iter->second.state = RELEASING;
      iter->second.revoked = false;
      
      release_queue.enq(release_entry(lid, xid));
    } else {
      iter->second.revoked = true;
    }
  } else {
    printf("ERROR: Got revoke with incorrect xid.\n");
  }

  pthread_mutex_unlock(&client_mutex);

  return ret;
}


rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
			         lock_protocol::xid_t xid, int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&client_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    printf("ERROR: Got retry for lock id not seen before.\n");
  } else if (iter->second.xid == xid) {
    iter->second.retry = true;
    pthread_cond_broadcast(&acquire_cv);
  } else {
    printf("ERROR: Got retry with incorrect xid.\n");
  }

  pthread_mutex_unlock(&client_mutex);

  return ret;
}
