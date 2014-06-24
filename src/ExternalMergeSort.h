//Templated class for ExternalMergeSort
//This class performs multithreaded external merge sort on a binary file
//The file is divided into chunks which are sorted then merged in parallel

#pragma once

#include "ExternalMergeSortBase.h"

namespace ems {
  template<typename key> using SortFunction = std::function < void(typename std::vector<key>::iterator, typename std::vector<key>::iterator) >;

  template<typename key>
  class ExternalMergeSort : public ExternalMergeSortBase
  {
  public:
    ExternalMergeSort();

    //Set the sort function for the given thread
    //If threadId is -1, set this function as default for all threads
    //Initially defaault sort function is std::sort
    void setSortFunction(SortFunction<key> sortFunc, int threadId = -1);

    //Reset the sort functions for the given thread
    //If threadId is -1, clear all thread-specific sort functions, keeping only the default one
    void clearSortFunction(int threadId = -1);

    //Perform the external merge sort
    //Returns true if successful
    virtual bool sort();

  protected:
    //Function to sort a chunk
    virtual void handleSortChunkTask(int threadId, Task *task, SortFunction<key> sortFunc);

    //Function to merge a chunl
    virtual void handleMergeFilesTask(int threadId, Task *task);

    //Allocate the data for the threads
    virtual void allocateData();

    //vector of keys for each thread
    std::vector< std::vector<key> > dataVec_;
  };

} //namespace ems

#include "ExternalMergeSort-inl.h"
