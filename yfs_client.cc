// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdlib>
#include <ctime>
#include <algorithm>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  srand(time(NULL));

  ec = new extent_client(extent_dst);

  std::string rootdir;
  int r = ec->get(1, rootdir);
  if (r == extent_protocol::NOENT) {
    ec->put(1, std::string());
  }
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

bool yfs_client::parse_dir(const std::string& data,
    std::list<dirent>* entries) {
  entries->clear();

  if (data.length() == 0) {
    return true;
  }

  size_t last_pos = 0;
  size_t cur_pos = 0;
  size_t cur_length = 0;
  size_t sep_pos = 0;
  std::string cur_part;
  do {
    cur_pos = data.find("//", last_pos);
    cur_length = (cur_pos == std::string::npos ? data.length() : cur_pos)
        - last_pos;
    cur_part = data.substr(last_pos, cur_length);
    sep_pos = cur_part.find('/');
    if (sep_pos == std::string::npos) {
      return false;
    } else {
      dirent entry;
      entry.name = cur_part.substr(0, sep_pos);
      entry.inum = n2i(cur_part.substr(sep_pos + 1,
          cur_part.length() - sep_pos - 1));
      entries->push_back(entry);
    }
    if (cur_pos != std::string::npos) {
      last_pos = cur_pos + 2;
    } else {
      last_pos = cur_pos;
    }
  } while(last_pos != std::string::npos);

  return true;
}

std::string yfs_client::serialize_dir(const std::list<dirent>& entries) {
  std::ostringstream ost;
  for (std::list<dirent>::const_iterator iter = entries.begin();
      iter != entries.end(); ++iter) {
    if (iter != entries.begin()) {
      ost << "//";
    }
    ost << iter->name << '/' << iter->inum;
  }
  return ost.str();
}

int yfs_client::get_dir_data(inum dir, std::list<dirent>* data) {
  std::string buf;
  int rep = ec->get(dir, buf);
  if (rep == extent_protocol::NOENT) {
    return NOENT;
  } else if (rep != extent_protocol::OK) {
    if (rep == extent_protocol::RPCERR) {
      return RPCERR;
    } else {
      return IOERR;
    }
  }
  if (!parse_dir(buf, data)) {
    return IOERR;
  }
  return OK;
}

int yfs_client::put_dir_data(inum dir, const std::list<dirent>& data) {
  std::string buf = serialize_dir(data);
  int rep = ec->put(dir, buf);
  if (rep == extent_protocol::NOENT) {
    return NOENT;
  } else if (rep != extent_protocol::OK) {
    if (rep == extent_protocol::RPCERR) {
      return RPCERR;
    } else {
      return IOERR;
    }
  }
  return OK;
}

int yfs_client::lookup(inum dir, const std::string& name, inum* file) {
  std::list<dirent> entries;
  int r = get_dir_data(dir, &entries);
  if (r != OK) {
    return r;
  }
  for (std::list<dirent>::iterator iter = entries.begin();
      iter != entries.end(); ++iter) {
    if (iter->name == name) {
      *file = iter->inum;
      return OK;
    }
  }
  return NOENT;
}

int yfs_client::create_file(inum parent, const std::string& name, inum* file) {
  std::list<dirent> entries;
  int r = get_dir_data(parent, &entries);
  if (r != OK) {
    return r;
  }
  for (std::list<dirent>::iterator iter = entries.begin();
      iter != entries.end(); ++iter) {
    if (iter->name == name) {
      *file = iter->inum;
      return EXIST;
    }
  }
  *file = generate_file_id();
  ec->put(*file, std::string());

  dirent newent;
  newent.name = name;
  newent.inum = *file;
  entries.push_back(newent);

  return put_dir_data(parent, entries);
}

int yfs_client::generate_file_id() {
  return 0x80000000 | (rand() % 0x80000000);
}

int yfs_client::read_dir(inum dir, std::list<dirent>* entries) {
  return get_dir_data(dir, entries);
}

int yfs_client::set_file_size(inum file, int size) {
  std::string buf;
  int r1 = get_file_data(file, &buf);
  if (r1 != OK) {
    return r1;
  }
  buf.resize(size, '\0');
  return put_file_data(file, buf);
}

int yfs_client::get_file_data(inum file, std::string* buf) {
  int rep = ec->get(file, *buf);
  if (rep == extent_protocol::NOENT) {
    return NOENT;
  } else if (rep != extent_protocol::OK) {
    if (rep == extent_protocol::RPCERR) {
      return RPCERR;
    } else {
      return IOERR;
    }
  }
  return OK;
}

int yfs_client::put_file_data(inum file, const std::string& buf) {
  int rep = ec->put(file, buf);
  if (rep == extent_protocol::NOENT) {
    return NOENT;
  } else if (rep != extent_protocol::OK) {
    if (rep == extent_protocol::RPCERR) {
      return RPCERR;
    } else {
      return IOERR;
    }
  }
  return OK;
}

int yfs_client::read_file(inum file, int size, int offset, std::string* buf) {
  std::string file_buf;
  int rep = get_file_data(file, &file_buf);
  if (rep != OK) {
    return rep;
  }
  if (offset >= static_cast<int>(file_buf.length())) {
    *buf = std::string();
  } else {
    int max_length = file_buf.size() - offset;
    int to_read = std::min(max_length, size);
    *buf = file_buf.substr(offset, to_read);
  }
  return OK;
}

int yfs_client::write_file(inum file, const char* buf, int size, int offset) {
  std::string file_buf;
  int rep = get_file_data(file, &file_buf);
  if (rep != OK) {
    return rep;
  }
  if (size + offset > static_cast<int>(file_buf.length())) {
    file_buf.resize(size + offset, '\0');
  }
  for (int ii = 0; ii < size; ++ii) {
    file_buf[ii + offset] = buf[ii];
  }
  return put_file_data(file, file_buf);
}
