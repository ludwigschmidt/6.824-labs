#ifndef fab_protocol_h
#define fab_protocol_h

#include "rpc.h"
#include "extent_protocol.h"
#include "rsm_protocol.h"


class fab_client_protocol {
 public:
  enum xxstatus { OK, ERR, BUSY};
  typedef int status;
  enum rpc_numbers {
    invoke = 0x9001,
    members,
  };
};


class fab_protocol {
 public:
  enum xxstatus { OK, ERR, BUSY};
  typedef int status;
  enum rpc_numbers {
    invoke = 0x10001,
    transferreq,
    transferdonereq,
    joinreq,
    put,
    get,
    getattr,
    remove,
    order,
  };

  enum fab_status {
    INTERNAL_OK,
    INTERNAL_OLD,
    INTERNAL_ERR,
  };

  typedef long long unsigned int timestamp;
  static timestamp get_current_timestamp() {
    long long ret = 0;
    timeval tv;
    gettimeofday(&tv, 0);
    ret = tv.tv_sec * 1000000LL + tv.tv_usec;
    return ret;
  }

  struct transferres {
    std::string state;
    viewstamp last;
  };
  
  struct joinres {
    std::string log;
  };

  struct fabresult {
    std::string val;
    int status;
    timestamp ts;
    extent_protocol::attr attr;
  };

};

inline marshall& operator<<(marshall &m, fab_protocol::fabresult f) {
  m << f.val;
  m << f.status;
  m << f.attr;
  m << f.ts;
  return m;
}

inline unmarshall& operator>>(unmarshall& u, fab_protocol::fabresult& f) {
  u >> f.val;
  u >> f.status;
  u >> f.attr;
  u >> f.ts;
  return u;
}

inline marshall &
operator<<(marshall &m, fab_protocol::transferres r)
{
  m << r.state;
  m << r.last;
  return m;
}

inline unmarshall &
operator>>(unmarshall &u, fab_protocol::transferres &r)
{
  u >> r.state;
  u >> r.last;
  return u;
}

inline marshall &
operator<<(marshall &m, fab_protocol::joinres r)
{
  m << r.log;
  return m;
}

inline unmarshall &
operator>>(unmarshall &u, fab_protocol::joinres &r)
{
  u >> r.log;
  return u;
}

class fab_test_protocol {
 public:
  enum xxstatus { OK, ERR};
  typedef int status;
  enum rpc_numbers {
    net_repair = 0x12001,
    breakpoint = 0x12002,
  };
};

#endif 
