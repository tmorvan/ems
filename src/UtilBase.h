//Base non-templated Utility class containing functions for ExternalMergeSort

#pragma once

#include <string>

namespace ems {

  class UtilBase
  {
  public:
    //Create a random file of keys, generating and writing the integers by chunks
    virtual bool createRandomFile(std::string fileName, long long numValues, long long chunkSize = 10000) = 0;

    //Check if a file contains sorted keys
    virtual bool checkSortedFile(std::string fileName) = 0;

    //Check if the file with desired filename exists
    //If not append integers to the name until a non-existent filename is found
    //Return empty string if all filenames are taken
    static std::string findAvailableFileName(std::string desiredFileName);
  };

} //namespace ems

#include "UtilBase-inl.h"