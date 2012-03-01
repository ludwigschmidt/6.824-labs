// the extent server implementation

#include "extent_server.h"
#include "rpc/slock.h"
#include <sstream>
#include <stdio.h>
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
  pthread_mutex_init(&mutex, NULL);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  ScopedLock l(&mutex);
  extent_map::iterator iter = data.find(id);
  if (iter == data.end()) {
    iter = data.insert(make_pair(id, make_pair(std::string(),
        extent_protocol::attr()))).first;
  }
  iter->second.first = buf;
  iter->second.second.ctime = iter->second.second.mtime = time(NULL);
  iter->second.second.size = buf.length();
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  ScopedLock l(&mutex);
  extent_map::iterator iter = data.find(id);
  if (iter == data.end()) {
    return extent_protocol::NOENT;
  } else {
    buf = iter->second.first;
    iter->second.second.atime = time(NULL);
    return extent_protocol::OK;
  }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  ScopedLock l(&mutex);
  extent_map::iterator iter = data.find(id);
  if (iter == data.end()) {
    return extent_protocol::NOENT;
  } else {
    a = iter->second.second;
    return extent_protocol::OK;
  }
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  ScopedLock l(&mutex);
  extent_map::iterator iter = data.find(id);
  if (iter == data.end()) {
    return extent_protocol::NOENT;
  } else {
    data.erase(iter);
    return extent_protocol::OK;
  }
}

