// Create a binary file of random unsigned 32-bit integers
//

#include "Util.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <functional>

using namespace std;
using namespace ems;

int main(int argc, char** argv)
{
  if (argc < 3) {
    cerr << "Too few arguments " << endl;
    if (argc != 0) cerr << "Syntax : " << argv[0] << " fileName numValues [keyType] [chunkSize]" << endl;
    return 1;
  }

  string fileName = argv[1];
  long long numValues = atoll(argv[2]);
  

  string keyType = "uint32";
  if (argc >= 4) keyType = argv[3];

  function<bool(string, long long, long long)> myCreateRandomFile;

  if (keyType == "uint8") myCreateRandomFile = createRandomFile<uint8_t>;
  else if (keyType == "uint16") myCreateRandomFile = createRandomFile<uint16_t>;
  else if (keyType == "uint32") myCreateRandomFile = createRandomFile<uint32_t>;
  else if (keyType == "uint64") myCreateRandomFile = createRandomFile<uint64_t>;
  else if (keyType == "int8") myCreateRandomFile = createRandomFile<int8_t>;
  else if (keyType == "int16") myCreateRandomFile = createRandomFile<int16_t>;
  else if (keyType == "int32") myCreateRandomFile = createRandomFile<int32_t>;
  else if (keyType == "int64") myCreateRandomFile = createRandomFile<int64_t>;
  else if (keyType == "float") myCreateRandomFile = createRandomFile<float>;
  else if (keyType == "double") myCreateRandomFile = createRandomFile<double>;
  else {
    cerr << "Invalid key type " << keyType.c_str() << endl;
    return 1;
  }

  long long chunkSize = 10000;
  if (argc >= 5) chunkSize = atoll(argv[4]);

  if (!myCreateRandomFile(fileName,numValues,chunkSize)) {
    cerr << "Error creating file " << fileName.c_str() << endl;
    return 1;
  }
  return 0;
}

