#include <cstdio>
#include <string>
#include "extent_client.h"

using namespace std;

int main(int argc, char* argv[]) {

  if (argc != 2) {
    printf("wrong usage\n");
    exit(1);
  }

  printf("creating extent client for address %s ...\n", argv[1]);
  extent_client ec(argv[1]);
  printf("done");
  string s;
  printf("calling put for eid 10 foo ...\n");
  int r = ec.put(10, "foo");
  ec.flush(10);
  //int r = ec.get(10, s);
  printf("result: %d\n", r);

  return 0;
}
