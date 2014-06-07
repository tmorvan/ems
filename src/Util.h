//Utility class containing functions for ExternalMergeSort

#pragma once

#include <string>

namespace ems {

  class Util
  {
  public:
    //Create a random file of 32-bit unsigned integers, generating and writing the integers by chunks
    static bool createRandomFile(std::string fileName, long long numValues, long long chunkSize = 10000);
    
    //Check if a file contains sorted 32-bit unsigned integers
    static bool checkSortedFile(std::string fileName);

    //Check if the file with desired filename exists
    //If not append integers to the name until a non-existent filename is found
    //Return empty string if all filenames are taken
    static std::string findAvailableFileName(std::string desiredFileName);
  };

} //namespace ems

