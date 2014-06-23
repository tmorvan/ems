// Create a binary file of random unsigned 32-bit integers
//

#include "Util.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <functional>

int main(int argc, char** argv)
{
  if (argc < 3) {
    std::cerr << "Too few arguments " << std::endl;
    if (argc != 0) std::cerr << "Syntax : " << argv[0] << " fileName numValues [keyType] [chunkSize]" << std::endl;
    return 1;
  }

  std::string fileName = argv[1];
  long long numValues = atoll(argv[2]);
  

  std::string keyType = "uint32";
  if (argc >= 4) keyType = argv[3];

  std::function<bool(std::string, long long, long long)> myCreateRandomFile;

  if (keyType == "uint8") myCreateRandomFile = ems::createRandomFile<uint8_t>;
  else if (keyType == "uint16") myCreateRandomFile = ems::createRandomFile<uint16_t>;
  else if (keyType == "uint32") myCreateRandomFile = ems::createRandomFile<uint32_t>;
  else if (keyType == "uint64") myCreateRandomFile = ems::createRandomFile<uint64_t>;
  else if (keyType == "int8") myCreateRandomFile = ems::createRandomFile<int8_t>;
  else if (keyType == "int16") myCreateRandomFile = ems::createRandomFile<int16_t>;
  else if (keyType == "int32") myCreateRandomFile = ems::createRandomFile<int32_t>;
  else if (keyType == "int64") myCreateRandomFile = ems::createRandomFile<int64_t>;
  else if (keyType == "float") myCreateRandomFile = ems::createRandomFile<float>;
  else if (keyType == "double") myCreateRandomFile = ems::createRandomFile<double>;
  else {
    std::cerr << "Invalid key type " << keyType.c_str() << std::endl;
    return 1;
  }

  long long chunkSize = 10000;
  if (argc >= 5) chunkSize = atoll(argv[4]);

  if (!myCreateRandomFile(fileName,numValues,chunkSize)) {
    std::cerr << "Error creating file " << fileName.c_str() << std::endl;
    return 1;
  }
  return 0;
}

