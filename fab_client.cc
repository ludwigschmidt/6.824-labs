#include "fab_client.h"
#include <vector>
#include <arpa/inet.h>
#include <stdio.h>
#include <handle.h>
#include "lang/verify.h"


fab_client::fab_client(std::string dst)
{
  printf("create fab_client\n");
  std::vector<std::string> mems;

  pthread_mutex_init(&fab_client_mutex, NULL);
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  primary = dst;

  {
    ScopedLock ml(&fab_client_mutex);
    VERIFY (init_members());
  }
  printf("fab_client: done\n");
}

// Assumes caller holds fab_client_mutex 
void
fab_client::primary_failure()
{
  // You fill this in for Lab 7
  primary = known_mems.back();
  known_mems.pop_back();
}

fab_protocol::status
fab_client::invoke(int proc, std::string req, std::string &rep)
{
  int ret;
  ScopedLock ml(&fab_client_mutex);
  while (1) {
    printf("fab_client::invoke proc %x primary %s\n", proc, primary.c_str());
    handle h(primary);

    VERIFY(pthread_mutex_unlock(&fab_client_mutex)==0);
    rpcc *cl = h.safebind();
    if (cl) {
      ret = cl->call(fab_client_protocol::invoke, proc, req, 
                     rep, rpcc::to(5000));
    }
    VERIFY(pthread_mutex_lock(&fab_client_mutex)==0);

    if (!cl) {
      goto prim_fail;
    }

    printf("fab_client::invoke proc %x primary %s ret %d\n", proc, 
           primary.c_str(), ret);
    if (ret == fab_client_protocol::OK) {
      break;
    }
    if (ret == fab_client_protocol::BUSY) {
      printf("fab is busy %s\n", primary.c_str());
      sleep(3);
      continue;
    }
prim_fail:
    printf("primary %s failed ret %d\n", primary.c_str(), ret);
    primary_failure();
    printf ("fab_client::invoke: retry new primary %s\n", primary.c_str());
  }
  return ret;
}

bool
fab_client::init_members()
{
  printf("fab_client::init_members get members!\n");
  handle h(primary);
  std::vector<std::string> new_view;
  VERIFY(pthread_mutex_unlock(&fab_client_mutex)==0);
  int ret;
  rpcc *cl = h.safebind();
  if (cl) {
    ret = cl->call(fab_client_protocol::members, 0, new_view, 
                   rpcc::to(1000)); 
  }
  VERIFY(pthread_mutex_lock(&fab_client_mutex)==0);
  if (cl == 0 || ret != fab_protocol::OK)
    return false;
  if (new_view.size() < 1) {
    printf("fab_client::init_members do not know any members!\n");
    VERIFY(0);
  }
  known_mems = new_view;
  primary = known_mems.back();
  known_mems.pop_back();

  printf("fab_client::init_members: primary %s\n", primary.c_str());

  return true;
}

