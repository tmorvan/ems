// Check if a binary file of unsigned 32-bit integers is sorted 
//

#include "Util.h"

#include <iostream>
#include <cstdint>
#include <memory>
#include <functional>


int main(int argc, char** argv)
{
  if (argc < 2) {
    std::cerr << "Too few arguments " << std::endl;
    if (argc != 0) std::cerr << "Syntax : " << argv[0] << " fileName [keyType]" << std::endl;
    return 1;
  }

  std::string fileName = argv[1];

  std::string keyType = "uint32";
  if (argc>=3) keyType = argv[2];
  
  std::function<bool(std::string)> myCheckSortedFile;

  if (keyType == "uint8") myCheckSortedFile = ems::checkSortedFile<uint8_t>;
  else if (keyType == "uint16") myCheckSortedFile = ems::checkSortedFile<uint16_t>;
  else if (keyType == "uint32") myCheckSortedFile = ems::checkSortedFile<uint32_t>;
  else if (keyType == "uint64") myCheckSortedFile = ems::checkSortedFile<uint64_t>;
  else if (keyType == "int8") myCheckSortedFile = ems::checkSortedFile<int8_t>;
  else if (keyType == "int16") myCheckSortedFile = ems::checkSortedFile<int16_t>;
  else if (keyType == "int32") myCheckSortedFile = ems::checkSortedFile<int32_t>;
  else if (keyType == "int64") myCheckSortedFile = ems::checkSortedFile<int64_t>;
  else if (keyType == "float") myCheckSortedFile = ems::checkSortedFile<float>;
  else if (keyType == "double") myCheckSortedFile = ems::checkSortedFile<double>;
  else {
    std::cerr << "Invalid key type " << keyType.c_str() << std::endl;
    return 1;
  }

  if (!myCheckSortedFile(fileName)) {
    std::cout << "File " << fileName.c_str() << " is not sorted " << std::endl;
    return 1;
  }
  std::cout << "File " << fileName.c_str() << " is sorted " << std::endl;
  return 0;
}

