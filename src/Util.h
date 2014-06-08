//Templated Utility class containing functions for ExternalMergeSort

#pragma once

#include <UtilBase.h>

#include <string>

namespace ems {

  template<typename key>
  class Util : public UtilBase
  {
  public:
    //Create a random file of keys, generating and writing the integers by chunks
    virtual bool createRandomFile(std::string fileName, long long numValues, long long chunkSize = 10000);
    
    //Check if a file contains sorted keys
    virtual bool checkSortedFile(std::string fileName);
  };

} //namespace ems

#include "Util-inl.h"