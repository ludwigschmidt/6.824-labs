// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&server_mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  std::string send_revoke_to;

  pthread_mutex_lock(&server_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    iter = locks.insert(std::make_pair(lid, lock_entry())).first;
  }

  if (!iter->second.locked_by.empty()) {
    ret = lock_protocol::RETRY;
    iter->second.waiting.insert(id);
    if (!iter->second.revoked) {
      iter->second.revoked = true;
      send_revoke_to = iter->second.locked_by;
    }
  } else {
    ret = lock_protocol::OK;
    iter->second.locked_by = id;
    iter->second.revoked = false;
    iter->second.waiting.erase(id);
  }

  pthread_mutex_unlock(&server_mutex);

  if (!send_revoke_to.empty()) {
    int r;
    handle(send_revoke_to).safebind()->call(rlock_protocol::revoke, lid, r);
  }

  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  std::string client_to_wake_up;
  bool also_send_revoke = false;

  pthread_mutex_lock(&server_mutex);

  lock_map::iterator iter = locks.find(lid);
  if (iter == locks.end()) {
    ret = lock_protocol::NOENT;
  } else {
    iter->second.locked_by = "";
    iter->second.revoked = false;
    if (!iter->second.waiting.empty()) {
      client_to_wake_up = *iter->second.waiting.begin();
      if (iter->second.waiting.size() > 1) {
        also_send_revoke = true;
      }
    }
  }

  pthread_mutex_unlock(&server_mutex);

  if (!client_to_wake_up.empty()) {
    handle(client_to_wake_up).safebind()->call(rlock_protocol::retry, lid, r);

    if (also_send_revoke) {
      handle(client_to_wake_up).safebind()->call(rlock_protocol::revoke, lid,
          r);
    }
  }

  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

