// Check if a binary file of unsigned 32-bit integers is sorted 
//

#include "Util.h"

#include <iostream>
#include <cstdint>
#include <memory>
#include <functional>

using namespace std;
using namespace ems;

int main(int argc, char** argv)
{
  if (argc < 2) {
    cerr << "Too few arguments " << endl;
    if (argc != 0) cerr << "Syntax : " << argv[0] << " fileName [keyType]" << endl;
    return 1;
  }

  string fileName = argv[1];

  string keyType = "uint32";
  if (argc>=3) keyType = argv[2];
  
  function<bool(string)> myCheckSortedFile;

  if (keyType == "uint8") myCheckSortedFile = checkSortedFile<uint8_t>;
  else if (keyType == "uint16") myCheckSortedFile = checkSortedFile<uint16_t>;
  else if (keyType == "uint32") myCheckSortedFile = checkSortedFile<uint32_t>;
  else if (keyType == "uint64") myCheckSortedFile = checkSortedFile<uint64_t>;
  else if (keyType == "int8") myCheckSortedFile = checkSortedFile<int8_t>;
  else if (keyType == "int16") myCheckSortedFile = checkSortedFile<int16_t>;
  else if (keyType == "int32") myCheckSortedFile = checkSortedFile<int32_t>;
  else if (keyType == "int64") myCheckSortedFile = checkSortedFile<int64_t>;
  else if (keyType == "float") myCheckSortedFile = checkSortedFile<float>;
  else if (keyType == "double") myCheckSortedFile = checkSortedFile<double>;
  else {
    cerr << "Invalid key type " << keyType.c_str() << endl;
    return 1;
  }

  if (!myCheckSortedFile(fileName)) {
    cout << "File " << fileName.c_str() << " is not sorted " << endl;
    return 1;
  }
  cout << "File " << fileName.c_str() << " is sorted " << endl;
  return 0;
}

