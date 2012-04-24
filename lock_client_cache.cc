// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();

  pthread_mutex_init(&client_mutex, NULL);
  pthread_cond_init(&acquire_cv, NULL);

}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
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
        || (iter->second.state == ACQUIRING && iter->second.retry)
        || iter->second.state == RELEASING) {
      iter->second.retry = false;
      iter->second.state = ACQUIRING;

      pthread_mutex_unlock(&client_mutex);

      int r;
      lock_protocol::status rpcret = cl->call(lock_protocol::acquire, lid, id,
          r);

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
        || (iter->second.state == ACQUIRING && !iter->second.retry)) {
      pthread_cond_wait(&acquire_cv, &client_mutex);
    }

  } while (!have_lock && !error);

  pthread_mutex_unlock(&client_mutex);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
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

      pthread_mutex_unlock(&client_mutex);

      if (lu != NULL) {
        lu->dorelease(lid);
      }

      int r;
      ret = cl->call(lock_protocol::release, lid, id, r);

      pthread_mutex_lock(&client_mutex);
    } else {
      iter->second.state = FREE;
      pthread_cond_broadcast(&acquire_cv);
    }
  }

  pthread_mutex_unlock(&client_mutex);

  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&client_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    printf("ERROR: Got revoke for lock id not seen before.");
  } else {
    if (iter->second.state == FREE) {
      iter->second.state = RELEASING;
      iter->second.revoked = false;

      pthread_mutex_unlock(&client_mutex);

      if (lu != NULL) {
        lu->dorelease(lid);
      }

      int r;
      ret = cl->call(lock_protocol::release, lid, id, r);

      pthread_mutex_lock(&client_mutex);
    } else {
      iter->second.revoked = true;
    }
  }

  pthread_mutex_unlock(&client_mutex);

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;

  pthread_mutex_lock(&client_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    printf("ERROR: Got retry for lock id not seen before.");
  } else {
    iter->second.retry = true;
    pthread_cond_broadcast(&acquire_cv);
  }

  pthread_mutex_unlock(&client_mutex);

  return ret;
}
