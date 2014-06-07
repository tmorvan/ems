// Create a binary file of random unsigned 32-bit integers
//

#include "Util.h"

#include <iostream>
#include <cstdlib>

using namespace std;
using namespace ems;

int main(int argc, char** argv)
{
  if (argc < 3) {
    cerr << "Too few arguments " << endl;
    if (argc != 0) cerr << "Syntax : " << argv[0] << " fileName numValues [chunkSize]" << endl;
    return 1;
  }

  string fileName = argv[1];
  long long numValues = atoll(argv[2]);
  long long chunkSize = 10000;
  if (argc >= 4) chunkSize = atoll(argv[3]);

  if (!Util::createRandomFile(fileName,numValues,chunkSize)) {
    cerr << "Error creating file " << fileName.c_str() << endl;
    return 1;
  }
  return 0;
}

