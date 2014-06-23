//Templated Utility class containing functions for ExternalMergeSort

#pragma once

#include "ThreadPool.h"

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
  //If appendNumber is positive it is the first appended integer
  //Otherwise the first appended integer is 0
  //The final appended number is returned in appendNumber (-1 if no numbers were appended)
  //Return empty string if all filenames are taken
  std::string findAvailableFileName(std::string desiredFileName, int &appendNumber);

  //Check if the file with desired filename exists
  //If not append integers to the name until a non-existent filename is found
  //Return empty string if all filenames are taken
  inline std::string findAvailableFileName(std::string desiredFileName) {
    int appendNumber = -1;
    return findAvailableFileName(desiredFileName, appendNumber);
  }

  //Write a file containing the profiling information for a list of tasks completed by a thread pool
  //All durations are written in nanoseconds
  //The first line contains the number of threads and total duration (endTime - startTime)
  //Every subsequent pair of lines contains 
  //The task name in the first line
  //The id of the thread which processed the task, the start and end time of the task (relative to startTime) in the second line
  void writeProfilingFile(std::string profilingFileName, int numThreads, TimePoint startTime, TimePoint endTime, const std::vector<std::shared_ptr<Task>> &completedTasks);

} //namespace ems

#include "Util-inl.h"