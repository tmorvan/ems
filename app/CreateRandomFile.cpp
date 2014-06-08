// Create a binary file of random unsigned 32-bit integers
//

#include "Util.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <memory>

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

  unique_ptr<UtilBase> myUtil;

  if (keyType == "uint8") myUtil = unique_ptr<UtilBase>(new Util<uint8_t>);
  else if (keyType == "uint16") myUtil = unique_ptr<UtilBase>(new Util<uint16_t>);
  else if (keyType == "uint32") myUtil = unique_ptr<UtilBase>(new Util<uint32_t>);
  else if (keyType == "uint64") myUtil = unique_ptr<UtilBase>(new Util<uint64_t>);
  else if (keyType == "int8") myUtil = unique_ptr<UtilBase>(new Util<int8_t>);
  else if (keyType == "int16") myUtil = unique_ptr<UtilBase>(new Util<int16_t>);
  else if (keyType == "int32") myUtil = unique_ptr<UtilBase>(new Util<int32_t>);
  else if (keyType == "int64") myUtil = unique_ptr<UtilBase>(new Util<int64_t>);
  else if (keyType == "float") myUtil = unique_ptr<UtilBase>(new Util<float>);
  else if (keyType == "double") myUtil = unique_ptr<UtilBase>(new Util<double>);
  else {
    cerr << "Invalid key type " << keyType.c_str() << endl;
    return 1;
  }

  long long chunkSize = 10000;
  if (argc >= 5) chunkSize = atoll(argv[4]);

  if (!myUtil->createRandomFile(fileName,numValues,chunkSize)) {
    cerr << "Error creating file " << fileName.c_str() << endl;
    return 1;
  }
  return 0;
}

