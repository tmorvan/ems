//This class performs multithreaded external merge sort on a file of unsigned 32 bit integers
//The file is divided into chunks which are sorted then merged in parallel

#pragma once

#include <vector>
#include <fstream>
#include <mutex>
#include <string>
#include <atomic>
#include <cstdint>

namespace ems {

  class ExternalMergeSort
  {
  public:
    ExternalMergeSort();

    ~ExternalMergeSort();

    //Set/get the input file name
    inline void setInputFileName(const char *fileName) {
      inputFileName_ = fileName ? fileName : "";
    }
    inline const char *getInputFileName() const {
      return inputFileName_.c_str();
    }

    //Set/get the output file name
    inline void setOutputFileName(const char *fileName) {
      outputFileName_ = fileName ? fileName : "";
    }
    inline const char *getOutputFileName() const {
      return outputFileName_.c_str();
    }

    //Set/get the number of threads to use
    void setNumThreads(int nThreads);
    inline int getNumThreads() const {
      return numThreads_;
    };

    //Set/get the amount of data (ints) allocated to each thread
    void setDataSizePerThread(long long dataSize);
    inline long long getDataSizePerThread() const {
      return dataSizePerThread_;
    };

    //Set/get the maximum number of merges per thread
    void setNumMergesPerThread(long long numMerges);
    inline long long getNumMergesPerThread() const {
      return numMergesPerThread_;
    };

    //Perform the external merge sort
    //Returns true if successful
    bool sort();

  private:
    //Function for sorting threads
    //Sort a chunk
    void sortChunk(int threadId);

    //Function for merging threads
    //Perform a N-way merge of chunks into one merged chunk
    void mergeChunks(int threadId);

    //Allocate the data for the threads
    void allocateData();

    //Close the open files and remove the intermediate files
    void cleanup();

    //Input file name
    std::string inputFileName_;

    //Output file name
    std::string outputFileName_;

    //Number of threads to use (default 4)
    int numThreads_;

    //Amount of data (ints) allocated to each thread (default 10M)
    long long dataSizePerThread_;

    //Maximum amount of chunks merged per thread (default 10)
    long long numMergesPerThread_;

    //Number of chunks merged by the last thread
    long long numMergesLastThread_;

    //vector of ints for each thread
    std::vector< std::vector<uint32_t> > dataVec_;

    //The input file
    std::fstream inFile_;

    //Used to serialize access to the input file by the threads
    std::mutex inFileMutex_;

    //Temporary files corresponding to sorted chunks
    std::vector< std::pair<std::string, std::fstream> > chunkFiles_;

    //Unique id for each chunk file
    long long chunkFileId_ = 0;

    //Used by the sorting threads to keep track of the next chunk to process
    std::atomic<long long> currentChunk_;

    //Number of chunks to sort or merge
    long long numChunks_;

    //Size of each chunk
    long long chunkSize_;

    //Size of the last chunk
    long long lastChunkSize_;

    //Used by the sorting threads to keep track of the next chunks to merge
    std::atomic<long long> currentMergeChunk_;

    //Number of merged chunks
    long long numMergeChunks_;

    //Size of each merged chunk
    long long mergeChunkSize_;

    //Size of the last merged chunk
    long long lastMergeChunkSize_;

    //Indicate whether an error has occured in the threads
    bool threadError_;

  };

} //namespace ems

