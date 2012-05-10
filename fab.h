// replicated state machine interface.

#ifndef fab_h
#define fab_h

#include <string>
#include <vector>
#include <set>
#include "fab_protocol.h"
#include "fab_state_transfer.h"
#include "rpc.h"
#include <arpa/inet.h>
#include "config.h"
#include "extent_server.h"

class fab : public config_view_change, public fab_state_transfer {
 public:
  typedef std::set<extent_protocol::extentid_t> extent_set;
  typedef std::set<std::string> server_set;
  typedef std::map<std::string, extent_set> server_to_extent_map_t;
  typedef std::map<extent_protocol::extentid_t, server_set>
      extent_to_server_map_t;

  struct global_state {
    server_to_extent_map_t server_to_extent_map;
    extent_to_server_map_t extent_to_server_map;
  };

 private:
  void reg1(int proc, handler *);

  global_state state;

  struct extent_timestamps {
    fab_protocol::timestamp valTs;
    fab_protocol::timestamp ordTs;
  };
  typedef std::map<extent_protocol::extentid_t, extent_timestamps>
      timestamp_map_t;

  timestamp_map_t timestamp_map;


 protected:
  std::map<int, handler *> procs;
  config *cfg;
  rpcs *fabrpc;
	extent_server *fabes;
	
  // On slave: expected viewstamp of next invoke request
  // On primary: viewstamp for the next request from fab_client
  viewstamp myvs;
  viewstamp last_myvs;   // Viewstamp of the last executed request
  std::string primary;
  //bool insync; 
  bool inviewchange;
  unsigned vid_commit;  // Latest view id that is known to fab layer
  unsigned vid_insync;  // The view id that this node is synchronizing for
  std::set<std::string> backups;   // A list of unsynchronized backups

  // For testing purposes
  rpcs *testsvr;
  bool partitioned;
  bool dopartition;
  bool break1;
  bool break2;

  int client_put(extent_protocol::extentid_t id, std::string val, int& r);
  int client_get(extent_protocol::extentid_t id, std::string &);
  int client_getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int client_remove(extent_protocol::extentid_t id, int& r);
	
  int put(extent_protocol::extentid_t id, std::string val,
      fab_protocol::timestamp ts, int& status);
  int get(extent_protocol::extentid_t id, fab_protocol::fabresult& result);
  int getattr(extent_protocol::extentid_t id, fab_protocol::fabresult& result);
  int remove(extent_protocol::extentid_t id, fab_protocol::timestamp ts,
      int& status);
  int order(extent_protocol::extentid_t id, fab_protocol::timestamp ts,
      bool return_value, fab_protocol::fabresult& result);
	
  fab_client_protocol::status client_members(int i, 
					     std::vector<std::string> &r);
  fab_protocol::status invoke(int proc, viewstamp vs, std::string mreq, 
			      int &dummy);
  fab_protocol::status transferreq(std::string src, viewstamp last, unsigned vid,
				   fab_protocol::transferres &r);
  fab_protocol::status transferdonereq(std::string m, unsigned vid, int &);
  fab_protocol::status joinreq(std::string src, viewstamp last, 
			       fab_protocol::joinres &r);
  fab_test_protocol::status test_net_repairreq(int heal, int &r);
  fab_test_protocol::status breakpointreq(int b, int &r);

  pthread_mutex_t fab_mutex;
  pthread_mutex_t invoke_mutex;
  pthread_cond_t recovery_cond;
  pthread_cond_t sync_cond;

  void execute(int procno, std::string req, std::string &r);
  fab_client_protocol::status client_invoke(int procno, std::string req, 
              std::string &r);
  bool statetransfer(std::string m);
  //bool statetransferdone(std::string m);
  bool join(std::string m);
  void set_primary(unsigned vid);
  std::string find_highest(viewstamp &vs, std::string &m, unsigned &vid);
  //bool sync_with_backups();
  bool sync_with_primary();
  void net_repair_wo(bool heal);
  void breakpoint1();
  void breakpoint2();
  void partition1();
  void commit_change_wo(unsigned vid);
  void update_metadata(std::string metadata);
 public:
  fab (std::string _first, std::string _me);
  ~fab() {};

  std::string marshal_state();
  void unmarshal_state(std::string state);

  bool amiprimary();
  void recovery();
  void commit_change(unsigned vid);

  template<class S, class A1, class R>
    void reg(int proc, S*, int (S::*meth)(const A1 a1, R &));
  template<class S, class A1, class A2, class R>
    void reg(int proc, S*, int (S::*meth)(const A1 a1, const A2 a2, R &));
  template<class S, class A1, class A2, class A3, class R>
    void reg(int proc, S*, int (S::*meth)(const A1 a1, const A2 a2, 
            const A3 a3, R &));
  template<class S, class A1, class A2, class A3, class A4, class R>
    void reg(int proc, S*, int (S::*meth)(const A1 a1, const A2 a2, 
            const A3 a3, const A4 a4, R &));
  template<class S, class A1, class A2, class A3, class A4, class A5, class R>
    void reg(int proc, S*, int (S::*meth)(const A1 a1, const A2 a2, 
            const A3 a3, const A4 a4, 
            const A5 a5, R &));
};

template<class S, class A1, class R> void
  fab::reg(int proc, S*sob, int (S::*meth)(const A1 a1, R & r))
{
  class h1 : public handler {
  private:
    S * sob;
    int (S::*meth)(const A1 a1, R & r);
  public:
  h1(S *xsob, int (S::*xmeth)(const A1 a1, R & r))
      : sob(xsob), meth(xmeth) { }
    int fn(unmarshall &args, marshall &ret) {
      A1 a1;
      R r;
      args >> a1;
      VERIFY(args.okdone());
      int b = (sob->*meth)(a1,r);
      ret << r;
      return b;
    }
  };
  reg1(proc, new h1(sob, meth));
}

template<class S, class A1, class A2, class R> void
  fab::reg(int proc, S*sob, int (S::*meth)(const A1 a1, const A2 a2, R & r))
{
 class h1 : public handler {
  private:
    S * sob;
    int (S::*meth)(const A1 a1, const A2 a2, R & r);
  public:
  h1(S *xsob, int (S::*xmeth)(const A1 a1, const A2 a2, R & r))
    : sob(xsob), meth(xmeth) { }
    int fn(unmarshall &args, marshall &ret) {
      A1 a1;
      A2 a2;
      R r;
      args >> a1;
      args >> a2;
      VERIFY(args.okdone());
      int b = (sob->*meth)(a1,a2,r);
      ret << r;
      return b;
    }
  };
  reg1(proc, new h1(sob, meth));
}

template<class S, class A1, class A2, class A3, class R> void
  fab::reg(int proc, S*sob, int (S::*meth)(const A1 a1, const A2 a2, 
             const A3 a3, R & r))
{
 class h1 : public handler {
  private:
    S * sob;
    int (S::*meth)(const A1 a1, const A2 a2, const A3 a3, R & r);
  public:
  h1(S *xsob, int (S::*xmeth)(const A1 a1, const A2 a2, const A3 a3, R & r))
    : sob(xsob), meth(xmeth) { }
    int fn(unmarshall &args, marshall &ret) {
      A1 a1;
      A2 a2;
      A3 a3;
      R r;
      args >> a1;
      args >> a2;
      args >> a3;
      VERIFY(args.okdone());
      int b = (sob->*meth)(a1,a2,a3,r);
      ret << r;
      return b;
    }
  };
  reg1(proc, new h1(sob, meth));
}

template<class S, class A1, class A2, class A3, class A4, class R> void
  fab::reg(int proc, S*sob, int (S::*meth)(const A1 a1, const A2 a2, 
             const A3 a3, const A4 a4, R & r))
{
 class h1 : public handler {
  private:
    S * sob;
    int (S::*meth)(const A1 a1, const A2 a2, const A3 a3, const A4 a4, R & r);
  public:
  h1(S *xsob, int (S::*xmeth)(const A1 a1, const A2 a2, const A3 a3, 
            const A4 a4, R & r))
    : sob(xsob), meth(xmeth) { }
    int fn(unmarshall &args, marshall &ret) {
      A1 a1;
      A2 a2;
      A3 a3;
      A4 a4;
      R r;
      args >> a1;
      args >> a2;
      args >> a3;
      args >> a4;
      VERIFY(args.okdone());
      int b = (sob->*meth)(a1,a2,a3,a4,r);
      ret << r;
      return b;
    }
  };
  reg1(proc, new h1(sob, meth));
}


template<class S, class A1, class A2, class A3, class A4, class A5, class R> void
  fab::reg(int proc, S*sob, int (S::*meth)(const A1 a1, const A2 a2, 
             const A3 a3, const A4 a4, 
             const A5 a5, R & r))
{
 class h1 : public handler {
  private:
    S * sob;
    int (S::*meth)(const A1 a1, const A2 a2, const A3 a3, const A4 a4, 
       const A5 a5, R & r);
  public:
  h1(S *xsob, int (S::*xmeth)(const A1 a1, const A2 a2, const A3 a3, 
            const A4 a4, const A5 a5, R & r))
    : sob(xsob), meth(xmeth) { }
    int fn(unmarshall &args, marshall &ret) {
      A1 a1;
      A2 a2;
      A3 a3;
      A4 a4;
      A5 a5;
      R r;
      args >> a1;
      args >> a2;
      args >> a3;
      args >> a4;
      VERIFY(args.okdone());
      int b = (sob->*meth)(a1,a2,a3,a4,a5,r);
      ret << r;
      return b;
    }
  };
  reg1(proc, new h1(sob, meth));
}

template <class C> marshall &
operator<<(marshall &m, std::set<C> s)
{
	m << (unsigned int) s.size();
	for (typename std::set<C>::const_iterator iter = s.begin();
      iter != s.end(); ++iter) {
    m << *iter;
  }
	return m;
}

template <class C> unmarshall &
operator>>(unmarshall &u, std::set<C> &s)
{
	unsigned n;
	u >> n;
	for(unsigned i = 0; i < n; i++){
		C z;
		u >> z;
		s.insert(z);
	}
	return u;
}

marshall& operator <<(marshall& m, const fab::global_state& g);
unmarshall& operator >>(unmarshall& m, fab::global_state& g);

#endif /* fab_h */
