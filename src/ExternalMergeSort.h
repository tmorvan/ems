//Templat ed class for ExternalMergeSort
//This class performs multithreaded external merge sort on a file of unsigned 32 bit integers
//The file is divided into chunks which are sorted then merged in parallel

#pragma once

#include "ExternalMergeSortBase.h"

namespace ems {

  template<typename key>
  class ExternalMergeSort : public ExternalMergeSortBase
  {
  public:
    //Perform the external merge sort
    //Returns true if successful
    virtual bool sort();

  protected:
    //Function for sorting threads
    //Sort a chunk
    virtual void sortChunk(int threadId);

    //Function for merging threads
    //Perform a N-way merge of chunks into one merged chunk
    virtual void mergeChunks(int threadId);

    //Allocate the data for the threads
    virtual void allocateData();

    //vector of keys for each thread
    std::vector< std::vector<key> > dataVec_;
  };

} //namespace ems

#include "ExternalMergeSort-inl.h"
