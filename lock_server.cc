// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&locks_mutex, NULL);
  pthread_cond_init(&release_cv, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&locks_mutex);

  while (locks.find(lid) != locks.end()) {
    pthread_cond_wait(&release_cv, &locks_mutex);
  }

  locks[lid] = clt;

  pthread_mutex_unlock(&locks_mutex);

  lock_protocol::status ret = lock_protocol::OK;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&locks_mutex);

  std::map<lock_protocol::lockid_t, int>::iterator iter = locks.find(lid);
  if (iter != locks.end()) {
    if (iter->second == clt) {
      locks.erase(iter);
      pthread_cond_broadcast(&release_cv);
    }
  }
  pthread_mutex_unlock(&locks_mutex);

  lock_protocol::status ret = lock_protocol::OK;
  return ret;
}
