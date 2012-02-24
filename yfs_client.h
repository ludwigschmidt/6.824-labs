#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  static bool parse_dir(const std::string&, std::list<dirent>*);
  static std::string serialize_dir(const std::list<dirent>&);
  int get_dir_data(inum dir, std::list<dirent>* data);
  int put_dir_data(inum dir, const std::list<dirent>& data);
  int get_file_data(inum file, std::string* buf);
  int put_file_data(inum file, const std::string& buf);
  int static generate_file_id();

 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int lookup(inum dir, const std::string& name, inum* file);
  int create_file(inum parent, const std::string& name, inum* file);
  int read_dir(inum dir, std::list<dirent>* entries);
  int set_file_size(inum file, int size);
  int read_file(inum file, int size, int offset, std::string* buf);
  int write_file(inum file, const char* buf, int size, int offset);
};

#endif 
