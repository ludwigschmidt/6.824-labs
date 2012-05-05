// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
#ifdef FAB
  cl = new fab_client(dst);
#else
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
#endif
  pthread_mutex_init(&mutex, NULL);
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  pthread_mutex_lock(&mutex);

  extent_protocol::status ret = extent_protocol::OK;
  
  cache_iterator_t iter = cache.find(eid);
  if (iter == cache.end()) {
    ret = cl->call(extent_protocol::get, eid, buf);
    if (ret == extent_protocol::OK) {
      cache_entry new_entry;
      extent_protocol::attr attributes;
      attributes.atime = time(NULL);
      attributes.size = buf.length();
      new_entry.data = buf;
      new_entry.attributes = attributes;
      new_entry.server_state = cache_entry::EXISTS;
      new_entry.client_state = cache_entry::UNCHANGED;
      new_entry.got_get_attributes = true;
      new_entry.got_put_attributes = false;
      new_entry.got_data = true;
      cache.insert(std::make_pair(eid, new_entry));
    } else if (ret == extent_protocol::NOENT) {
      cache_entry new_entry;
      extent_protocol::attr attributes;
      new_entry.data = "";
      new_entry.attributes = attributes;
      new_entry.server_state = cache_entry::SERVER_NOENT;
      new_entry.client_state = cache_entry::UNCHANGED;
      new_entry.got_get_attributes = true;
      new_entry.got_put_attributes = true;
      new_entry.got_data = true;
      cache.insert(std::make_pair(eid, new_entry));
    }
  } else {
    if (iter->second.client_state == cache_entry::REMOVED
        || (iter->second.server_state == cache_entry::SERVER_NOENT
        && iter->second.client_state != cache_entry::WRITTEN)) {
      ret = extent_protocol::NOENT;
    } else if (!iter->second.got_data) {
      ret = cl->call(extent_protocol::get, eid, buf);
      if (ret == extent_protocol::OK) {
        iter->second.data = buf;
        iter->second.attributes.atime = time(NULL);
        iter->second.attributes.size = buf.length();
        iter->second.got_data = true;
        iter->second.got_get_attributes = true;
      }
    } else {
      buf = iter->second.data;
      iter->second.attributes.atime = time(NULL);
    }
  }

  pthread_mutex_unlock(&mutex);

  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  pthread_mutex_lock(&mutex);

  extent_protocol::status ret = extent_protocol::OK;
  
  cache_iterator_t iter = cache.find(eid);
  if (iter == cache.end()) {
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if (ret == extent_protocol::OK) {
      cache_entry new_entry;
      new_entry.data = "";
      new_entry.attributes = attr;
      new_entry.server_state = cache_entry::EXISTS;
      new_entry.client_state = cache_entry::UNCHANGED;
      new_entry.got_get_attributes = true;
      new_entry.got_put_attributes = true;
      new_entry.got_data = false;
      cache.insert(std::make_pair(eid, new_entry));
    } else if (ret == extent_protocol::NOENT) {
      cache_entry new_entry;
      new_entry.data = "";
      new_entry.attributes = attr;
      new_entry.server_state = cache_entry::SERVER_NOENT;
      new_entry.client_state = cache_entry::UNCHANGED;
      new_entry.got_get_attributes = false;
      new_entry.got_put_attributes = false;
      new_entry.got_data = false;
      cache.insert(std::make_pair(eid, new_entry));
    }
  } else {
    if (iter->second.client_state == cache_entry::REMOVED
        || (iter->second.server_state == cache_entry::SERVER_NOENT
        && iter->second.client_state != cache_entry::WRITTEN)) {
      ret = extent_protocol::NOENT;
    } else if (!iter->second.got_get_attributes
        || !iter->second.got_put_attributes) {
      ret = cl->call(extent_protocol::getattr, eid, attr);
      if (ret == extent_protocol::OK) {
        if (!iter->second.got_get_attributes
            && !iter->second.got_put_attributes) {
          iter->second.attributes.size = attr.size;
        }
        if (!iter->second.got_get_attributes) {
          iter->second.attributes.atime = attr.atime;
          iter->second.got_get_attributes = true;
        }
        if (!iter->second.got_put_attributes) {
          iter->second.attributes.ctime =
              iter->second.attributes.mtime = attr.mtime;
          iter->second.got_put_attributes = true;
        }
      } else if (ret == extent_protocol::NOENT) {
        if (!iter->second.got_get_attributes) {
          iter->second.attributes.atime = 0;
          iter->second.got_get_attributes = true;
        }
        ret = extent_protocol::OK;
      }
    } else {
      attr = iter->second.attributes;
    }
  }

  pthread_mutex_unlock(&mutex);

  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  pthread_mutex_lock(&mutex);

  extent_protocol::status ret = extent_protocol::OK;

  cache_iterator_t iter = cache.find(eid);
  if (iter == cache.end()) {
    cache_entry new_entry;
    extent_protocol::attr attributes;
    attributes.ctime = iter->second.attributes.mtime = time(NULL);
    attributes.size = buf.length();
    new_entry.data = buf;
    new_entry.attributes = attributes;
    new_entry.server_state = cache_entry::UNKNOWN;
    new_entry.client_state = cache_entry::WRITTEN;
    new_entry.got_get_attributes = false;
    new_entry.got_put_attributes = true;
    new_entry.got_data = true;
    cache.insert(std::make_pair(eid, new_entry));
  } else {
    iter->second.client_state = cache_entry::WRITTEN;
    iter->second.data = buf;
    iter->second.got_data = true;
    iter->second.got_put_attributes = true;
    iter->second.attributes.ctime = iter->second.attributes.mtime = time(NULL);
    iter->second.attributes.size = buf.length();
  }

  pthread_mutex_unlock(&mutex);

  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  pthread_mutex_lock(&mutex);

  extent_protocol::status ret = extent_protocol::OK;

  cache_iterator_t iter = cache.find(eid);
  if (iter == cache.end()) {
    int r;
    ret = cl->call(extent_protocol::remove, eid, r);
    
    if (ret == extent_protocol::NOENT || ret == extent_protocol::OK) {
      cache_entry new_entry;
      extent_protocol::attr attributes;
      new_entry.data = "";
      new_entry.attributes = attributes;
      new_entry.server_state = cache_entry::SERVER_NOENT;
      new_entry.client_state = cache_entry::UNCHANGED;
      new_entry.got_get_attributes = true;
      new_entry.got_put_attributes = true;
      new_entry.got_data = true;
      cache.insert(std::make_pair(eid, new_entry));
    }
  } else {
    if (iter->second.client_state == cache_entry::REMOVED
        || (iter->second.server_state == cache_entry::SERVER_NOENT
        && iter->second.client_state != cache_entry::WRITTEN)) {
      return extent_protocol::NOENT;
    } else {
      iter->second.data = "";
      iter->second.client_state = cache_entry::REMOVED;
      iter->second.got_get_attributes = true;
      iter->second.got_put_attributes = true;
      iter->second.got_data = true;
    }
  }
      
  pthread_mutex_unlock(&mutex);

  return ret;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid) {
  pthread_mutex_lock(&mutex);

  extent_protocol::status ret = extent_protocol::OK;

  cache_iterator_t iter = cache.find(eid);
  if (iter != cache.end()) {
    if (iter->second.client_state == cache_entry::REMOVED &&
        iter->second.server_state != cache_entry::SERVER_NOENT) {
      int r;
      ret = cl->call(extent_protocol::remove, eid, r);
    } else if (iter->second.client_state == cache_entry::WRITTEN) {
      int r;
      ret = cl->call(extent_protocol::put, eid, iter->second.data, r);
    }

    cache.erase(iter);
  }

  pthread_mutex_unlock(&mutex);

  return ret;
}

void extent_client_lock_release_user::dorelease(lock_protocol::lockid_t lid) {
  client->flush(lid);
}
