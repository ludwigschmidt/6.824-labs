#include <cstdio>
#include <string>
#include "extent_client.h"

using namespace std;

int main(int argc, char* argv[]) {
  printf("creating extent client for address %s ...\n", argv[1]);
  extent_client ec(argv[1]);
  printf("done");

  if (strcmp(argv[2], "put") == 0) {
    printf("calling put for eid 10 with \"%s\" ...\n", argv[3]);
    int r = ec.put(10, argv[3]);
    ec.flush(10);
    printf("result: %d\n", r);
  } else if (strcmp(argv[2], "get") == 0) {
    printf("calling get for eid 10 ...\n");
    string s;
    int r = ec.get(10, s);
    printf("result: %d, val \"%s\"\n", r, s.c_str());
  } else {
    printf("unknown command: %s\n", argv[2]);
  }

  return 0;
}
