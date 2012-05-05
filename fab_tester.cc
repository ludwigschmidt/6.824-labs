#include <cstdio>
#include "extent_client.h"

int main(int argc, char* argv[]) {

  if (argc != 2) {
    printf("wrong usage\n");
    exit(1);
  }

  printf("creating extent client for address %s ...\n", argv[1]);
  extent_client ec(argv[1]);
  printf("done");
  string s;
  printf("calling get for eid 0 ...\n");
  ec.get(0, s);
  printf("result: %s\n", s.c_str());

  return 0;
}
