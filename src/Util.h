//Templated Utility class containing functions for ExternalMergeSort

#pragma once

#include <string>

namespace ems {

  //Create a random file of keys, generating and writing the integers by chunks
  template<typename key>
  bool createRandomFile(std::string fileName, long long numValues, long long chunkSize = 10000);

  //Check if a file contains sorted keys
  template<typename key>
  bool checkSortedFile(std::string fileName);

  //Check if the file with desired filename exists
  //If not append integers to the name until a non-existent filename is found
  //Return empty string if all filenames are taken
  std::string findAvailableFileName(std::string desiredFileName);

} //namespace ems

#include "Util-inl.h"