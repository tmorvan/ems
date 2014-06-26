//Base non-templated class for external merge sort
//This class performs multithreaded external merge sort on a binary file
//The file is divided into chunks which are sorted then merged in parallel

#pragma once

#include <vector>
#include <fstream>
#include <mutex>
#include <string>
#include <atomic>
#include <algorithm>
#include <memory>

#include "ThreadPool.h"

namespace ems {
  //Tasks for sorting chunks
  struct SortChunkTask : public Task {
    long long startInd;
    long long numValues;
    std::string sortedFileName;
  };

  //Task for merging files
  struct MergeFilesTask : public Task {
    std::vector<std::pair<std::string,long long>> files;
    int level;
    std::string mergedFileName;
  };

  class ExternalMergeSortBase
  {
  public:
    ExternalMergeSortBase() :
      numThreads_(4),
      dataSizePerThread_(10000000),
      numMergesPerThread_(10)
    {
      if (std::thread::hardware_concurrency()) numThreads_ = std::thread::hardware_concurrency();
    }

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

    //Set/get the profiling file, if empty, profiling is turned off
    inline void setProfilingFileName(const char *fileName) {
      profilingFileName_ = fileName ? fileName : "";
    }
    inline const char *getProfilingFileName() const {
      return profilingFileName_.c_str();
    }

    //Set/get the number of threads to use
    inline void setNumThreads(int nThreads) {
      numThreads_ = std::max(1, nThreads);
      allocateData();
    }
    inline int getNumThreads() const {
      return numThreads_;
    };

    //Set/get the amount of data allocated to each thread
    inline void setDataSizePerThread(long long dataSize) {
      dataSizePerThread_ = std::max(3LL, dataSize);
      numMergesPerThread_ = std::min(dataSizePerThread_ - 1, numMergesPerThread_);
      allocateData();
    }
    inline long long getDataSizePerThread() const {
      return dataSizePerThread_;
    };

    //Set/get the maximum number of merges per thread
    inline void setNumMergesPerThread(long long numMerges) {
      numMergesPerThread_ = std::max(2LL, numMerges);
      numMergesPerThread_ = std::min(dataSizePerThread_ - 1, numMergesPerThread_);
    }
    inline long long getNumMergesPerThread() const {
      return numMergesPerThread_;
    };

    //Perform the external merge sort
    //Returns true if successful
    virtual bool sort() = 0;

  protected:
    //Allocate the data for the threads
    virtual void allocateData() = 0;

    //Close the open files and remove the intermediate files
    inline void cleanup() {
      pool_.stopHandlingTasks();
      pool_.join();

      if (inFile_.is_open()) inFile_.close();
      inFile_.clear();

      std::shared_ptr<Task> task;

      //Go through the tasks still in the pool and the ones stored and close and remove the temporary files
      do {
        task = pool_.getTask(false, false);
        if (!task) task = pool_.getCompletedTask(false, false);
        while (!task && storedTasks_.size()) {
          auto taskList = storedTasks_.back();
          if (!taskList.size()) storedTasks_.pop_back();
          else {
            task = taskList.back();
            taskList.pop_back();
          }
        }
        if (task) {
          SortChunkTask *sortTask = dynamic_cast<SortChunkTask *>(task.get());
          if (sortTask) {
            if (!sortTask->sortedFileName.empty()) remove(sortTask->sortedFileName.c_str());
          }
          else {
            MergeFilesTask *mergeTask = dynamic_cast<MergeFilesTask *>(task.get());
            if (mergeTask) {
              for (auto fileInfo : mergeTask->files) {
                if (!fileInfo.first.empty()) remove(fileInfo.first.c_str());
              }
              if (!mergeTask->mergedFileName.empty()) remove(mergeTask->mergedFileName.c_str());
            }
          }
        }
      } while (task);
    }

    //Input file name
    std::string inputFileName_;

    //Output file name
    std::string outputFileName_;
    
    //Profiling file name
    std::string profilingFileName_;

    //Number of threads to use
    int numThreads_;

    //Amount of keys allocated to each thread (default 10M)
    long long dataSizePerThread_;

    //Maximum amount of chunks merged per thread (default 10)
    long long numMergesPerThread_;

    //The pool containing the worker threads
    ThreadPool pool_;

    //Tasks stored by the main thread for future merge
    std::vector< std::vector< std::shared_ptr<Task> > > storedTasks_;

    //The input file
    std::fstream inFile_;

    //Used to serialize access to the input file by the threads
    std::mutex inFileMutex_;
  };

} //namespace ems

