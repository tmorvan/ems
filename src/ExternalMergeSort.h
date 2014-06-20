//Templated class for ExternalMergeSort
//This class performs multithreaded external merge sort on a binary file
//The file is divided into chunks which are sorted then merged in parallel

#pragma once

#include "ExternalMergeSortBase.h"

namespace ems {

  template<typename key>
  class ExternalMergeSort : public ExternalMergeSortBase
  {
  public:
    ExternalMergeSort();

    //Perform the external merge sort
    //Returns true if successful
    virtual bool sort();

  protected:
    //Function to sort a chunk
    virtual void handleSortChunkTask(int threadId, Task *task) ;

    //Function to merge a chunl
    virtual void handleMergeFilesTask(int threadId, Task *task) ;

    //Allocate the data for the threads
    virtual void allocateData();

    //vector of keys for each thread
    std::vector< std::vector<key> > dataVec_;
  };

} //namespace ems

#include "ExternalMergeSort-inl.h"
