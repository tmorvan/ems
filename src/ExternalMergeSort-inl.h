#pragma once

#include "Util.h"

#include <cstdio>
#include <queue>
#include <iostream>

namespace ems {

  template<typename key>
  ExternalMergeSort<key>::ExternalMergeSort() {
    //initial allocation
    allocateData();

    //Do not let thread throw exceptions since they will be recaptured by the main thread
    pool_.setThreadExceptionHandler([](int, std::exception_ptr) { return false; });

    //Set the default handlers for the sort and merge tasks
    pool_.addTaskHandler<SortChunkTask>(std::bind(&ExternalMergeSort<key>::handleSortChunkTask,this,std::placeholders::_1,std::placeholders::_2));
    pool_.addTaskHandler<MergeFilesTask>(std::bind(&ExternalMergeSort<key>::handleMergeFilesTask, this, std::placeholders::_1, std::placeholders::_2));
  }

  template<typename key>
  bool ExternalMergeSort<key>::sort() {
    try {
      if ((inputFileName_.empty()) || (outputFileName_.empty())) {
        std::cerr << "ExternalMergeSort::sort No input or output file specified" << std::endl;
        cleanup();
        return false;
      }

      //Open the input file
      inFile_.exceptions(std::fstream::failbit | std::fstream::badbit);
      inFile_.open(inputFileName_, std::ios::in | std::ios::binary);
      if (!inFile_.is_open()) {
        std::cerr << "ExternalMergeSort::sortCould not open file " << inputFileName_ << std::endl;
        cleanup();
        return false;
      }

      //Unique id used for temporary files
      int tmpFileId = 0;

      //Get the size of the input file
      inFile_.seekg(0, std::ios::end);
      long long dataLength = inFile_.tellg();
      inFile_.seekg(0, std::ios::beg);

      //File size should be a multiple of sizeof(key)
      if (dataLength % sizeof(key)) {
        std::cerr << "ExternalMergeSort::sort Invalid file size" << std::endl;
        cleanup();
        return false;
      }
      long long numValues = dataLength / sizeof(key);

      //Get the number of chunks
      long long numChunks = (numValues + dataSizePerThread_-1) / dataSizePerThread_;

      //Compute the number of levels of merge to apply after the sorting and the number of chunks at each level
      int numMergeLevels = 0;
      long long levelSize = numChunks;
      std::vector<long long> levelNumChunks;
      while (levelSize>1) {
        levelNumChunks.push_back(levelSize);
        numMergeLevels++;
        levelSize = (levelSize + numMergesPerThread_-1) / numMergesPerThread_;

      }

      //Clear the stored tasks
      storedTasks_.clear();
      storedTasks_.resize(numMergeLevels);

      //Clear all previous tasks in the pool
      pool_.clearTasks();
      pool_.clearCompletedTasks();

      //Set up profiling if the a profil9ng file has been specified
      std::vector<std::shared_ptr<Task>> completedTasks;
      pool_.setProfile(!profilingFileName_.empty());

      //Create the sort tasks for each chunk 
      for (long long i = 0; i < numChunks; i++) {
        std::shared_ptr<SortChunkTask> sortTask = std::make_shared<SortChunkTask>();
        sortTask->startInd = i*dataSizePerThread_;
        sortTask->numValues = std::min(dataSizePerThread_, numValues - sortTask->startInd);
        if (numChunks>1) {
          sortTask->sortedFileName = findAvailableFileName(outputFileName_,tmpFileId);
          tmpFileId++;
          if (sortTask->sortedFileName.empty()) {
            //No available name found, return
            std::cerr << "No available filename found " << std::endl;
            cleanup();
            return false;
          }
        }
        else sortTask->sortedFileName = outputFileName_;
        pool_.addTask(sortTask);
      }

      pool_.handleTasks(numThreads_);

      std::shared_ptr<MergeFilesTask> newMergeTask;
      std::shared_ptr<Task> completedTask = pool_.getCompletedTask();

      while (completedTask) {
        //If needed save the task for profiling information
        if (!profilingFileName_.empty()) completedTasks.push_back(completedTask);

        //Find out the type of the task
        SortChunkTask *sortTask = dynamic_cast<SortChunkTask *>(completedTask.get());
        MergeFilesTask *mergeTask = dynamic_cast<MergeFilesTask *>(completedTask.get());
        if (sortTask) {
          //Only one chunk, exit directly
          if (numChunks==1) break;
          //Store the task
          storedTasks_[0].push_back(completedTask);
          //Create a merge task if we have enough stored tasks
          if (storedTasks_[0].size() == std::min(numMergesPerThread_, levelNumChunks[0])) {
            //Create the merge task
            newMergeTask = std::make_shared<MergeFilesTask>();
            newMergeTask->level = 1;
            for (auto task : storedTasks_[0]) {
              SortChunkTask *storedSortTask = dynamic_cast<SortChunkTask *>(task.get());
              newMergeTask->files.push_back(std::make_pair(storedSortTask->sortedFileName,storedSortTask->numValues));
            }
            if (newMergeTask->level == numMergeLevels) {
              //Last merge level, write directly to output
              newMergeTask->mergedFileName = outputFileName_;
            }
            else {
              //Find a filename
              newMergeTask->mergedFileName = findAvailableFileName(outputFileName_, tmpFileId);
              tmpFileId++;
              if (newMergeTask->mergedFileName.empty()) {
                //No available name found, return
                std::cerr << "No available filename found " << std::endl;
                cleanup();
                return false;
              }
            }
            //Add the new merge task
            pool_.addTask(newMergeTask); // , newMergeTask->level);
            //Decrement the number of chunks for this level
            levelNumChunks[0] -= storedTasks_[0].size();
            //Clear the stored tasks
            storedTasks_[0].clear();
          }
        }
        else if (mergeTask) {         
          //If the last level has been reached, exit
          if (mergeTask->level >= numMergeLevels) break;

          //Store the task
          storedTasks_[mergeTask->level].push_back(completedTask);
          if (storedTasks_[mergeTask->level].size() == std::min(numMergesPerThread_, levelNumChunks[mergeTask->level])) {
            //Create the merge task
            newMergeTask = std::make_shared<MergeFilesTask>();
            newMergeTask->level = mergeTask->level+1;
            for (auto task : storedTasks_[mergeTask->level]) {
              MergeFilesTask *storedMergeTask = dynamic_cast<MergeFilesTask *>(task.get());
              long long taskNumValues = 0;
              //Get the number of values for this stored task
              for (auto fileInfo : storedMergeTask->files) {
                taskNumValues += fileInfo.second;
              }
              newMergeTask->files.push_back(std::make_pair(storedMergeTask->mergedFileName, taskNumValues));
            }
            if (newMergeTask->level == numMergeLevels) {
              //Last merge level, write directly to output
              newMergeTask->mergedFileName = outputFileName_;
            }
            else {
              //Find a filename
              newMergeTask->mergedFileName = findAvailableFileName(outputFileName_, tmpFileId);
              tmpFileId++;
              if (newMergeTask->mergedFileName.empty()) {
                //No available name found, return
                std::cerr << "No available filename found " << std::endl;
                cleanup();
                return false;
              }
            }
            //Add the new merge task
            pool_.addTask(newMergeTask); // , newMergeTask->level);
            //Decrement the number of chunks for this level
            levelNumChunks[mergeTask->level] -= storedTasks_[mergeTask->level].size();
            //Clear the stored tasks
            storedTasks_[mergeTask->level].clear();
          }
        }

        completedTask = pool_.getCompletedTask();
      }

      //Stop handling and join the threads
      cleanup();

      //Write profiling information
      if (!profilingFileName_.empty()) {
        writeProfilingFile(profilingFileName_, numThreads_, pool_.getStartTime(), pool_.getEndTime(), completedTasks);
      }

      return true;
    }
    catch (...) {
      //Make sure we cleanup before exiting
      std::cerr << "ExternalMergeSort::sort exception occured " << std::endl;
      cleanup();
      throw;
    }
  }

  template<typename key>
  void ExternalMergeSort<key>::handleSortChunkTask(int threadId, Task *task) {
    SortChunkTask *sortTask = dynamic_cast<SortChunkTask *>(task);
    if (!sortTask) return;
    std::fstream sortedFile;
    try {
      //Open the file for this chunk
      sortedFile.exceptions(std::fstream::failbit | std::fstream::badbit);
      sortedFile.open(sortTask->sortedFileName, std::ios::out | std::ios::binary);
      //Read the data in this thread data vector
      {
        //Lock
        std::lock_guard<std::mutex> lock(inFileMutex_);

        inFile_.seekg(sortTask->startInd * sizeof(key));
        inFile_.read(reinterpret_cast<char *>(&(dataVec_[threadId][0])), sizeof(key)*sortTask->numValues);
        //Release lock
      }

      //sort the chunk
      std::sort(dataVec_[threadId].begin(), dataVec_[threadId].begin() + sortTask->numValues);

      //Write the sorted chunk
      sortedFile.write(reinterpret_cast<char *>(&(dataVec_[threadId][0])), sizeof(key)*sortTask->numValues);
      //Close the sorted file
      sortedFile.close();
    }
    catch (...) {
      //close and remove the file if needed
      if (sortedFile.is_open()) sortedFile.close();
      remove(sortTask->sortedFileName.c_str());
      throw;
    }
  }

  //Each file is allocated an input buffer in the dataVec of this thread.
  //An output buffer for the merged file is also allocated
  template<typename key>
  void ExternalMergeSort<key>::handleMergeFilesTask(int threadId, Task *task) {
    MergeFilesTask *mergeTask = dynamic_cast<MergeFilesTask *>(task);
    if (!mergeTask) return;
    std::fstream mergedFile;
    std::vector<std::fstream> inputFiles;
    try {
      long long numMerges = mergeTask->files.size();
      if (!numMerges) return;

      //Compute the size allocated to each input file in this thread data array
      long long inputFileArraySize = dataSizePerThread_ / (numMerges+1);
      if (!inputFileArraySize) return;

      //Remaining size is allocated to the merged file
      long long mergedFileArraySize = dataSizePerThread_ - numMerges*inputFileArraySize;

      //Keep track of the current position in the input files and the array for each input file
      std::vector<long long> inputFilePos(numMerges, 0);
      std::vector<long long> inputFileArrayPos(numMerges);
      //Set the array position at the end so that data is loaded
      for (int i = 0; i < numMerges; i++) inputFileArrayPos[i] = (i + 1)*mergedFileArraySize;

      //Keep track of the current position in the array for the output file
      long long mergedFileArrayPos = numMerges*inputFileArraySize;

      //Open the input files in read mode
      inputFiles.resize(numMerges);
      for (int i = 0; i < numMerges; i++) {
        inputFiles[i].exceptions(std::fstream::failbit | std::fstream::badbit);
        inputFiles[i].open(mergeTask->files[i].first, std::ios::in | std::ios::binary);
      }

      //Open the merged file in write mode
      mergedFile.exceptions(std::fstream::failbit | std::fstream::badbit);
      mergedFile.open(mergeTask->mergedFileName, std::ios::out | std::ios::binary);

      //Priority queue keeping track of the values at the current pointers in the thread data vector
      std::priority_queue<std::pair<key, long long>, std::vector<std::pair<key, long long>>, std::greater<std::pair<key, long long>>> mergeQueue;

      //Perform N-way merge of the input files
      //Load the initial data and fill the priority queue
      for (long long i = 0; i < numMerges; i++) {
        //If the end for this chunk has been reached skip to the next chunk
        if (inputFilePos[i] == mergeTask->files[i].second) continue;

        //Load data for file i
        long long numRead = std::min<long long>(inputFileArraySize, mergeTask->files[i].second - inputFilePos[i]);
        if (numRead) {
          //Point to the beginning of the buffer for this chunk
          inputFileArrayPos[i] = i*inputFileArraySize;
          //Read the data
          inputFiles[i].read(reinterpret_cast<char *>(&(dataVec_[threadId][inputFileArrayPos[i]])), sizeof(key)* numRead);
          mergeQueue.push(std::make_pair(dataVec_[threadId][inputFileArrayPos[i]], i));
        }
      }

      //While the queue is not empty, dequeue the top element, add it to the result and try to fetch another value from the same input file
      while (!mergeQueue.empty()) {
        auto topPair = mergeQueue.top();
        mergeQueue.pop();
        dataVec_[threadId][mergedFileArrayPos++] = topPair.first;

        //Add data to the queue from the input file we just poped
        //Increment the pointers for this input file
        inputFileArrayPos[topPair.second]++;
        inputFilePos[topPair.second]++;
        //Check that the end of this input file has not been reached
        if (inputFilePos[topPair.second] != mergeTask->files[topPair.second].second) {
          //Read more data if necessary
          if (inputFileArrayPos[topPair.second] == (topPair.second + 1)*inputFileArraySize) {
            long long numRead = std::min<long long>(inputFileArraySize, mergeTask->files[topPair.second].second - inputFilePos[topPair.second]);
            if (numRead) {
              //Point to the beginning of the buffer for this input file
              inputFileArrayPos[topPair.second] = topPair.second*inputFileArraySize;
              //Read the data
              inputFiles[topPair.second].read(reinterpret_cast<char *>(&(dataVec_[threadId][inputFileArrayPos[topPair.second]])), sizeof(key)* numRead);
            }
          }
          //Add to the queue
          mergeQueue.push(std::make_pair(dataVec_[threadId][inputFileArrayPos[topPair.second]], topPair.second));
        }
        //Write the merged data if the output buffer is full or the queue is empty
        if ((mergedFileArrayPos == dataSizePerThread_) || mergeQueue.empty()) {
          long long numWrite = mergedFileArrayPos - numMerges*inputFileArraySize;
          mergedFile.write(reinterpret_cast<char *>(&(dataVec_[threadId][numMerges*inputFileArraySize])), sizeof(key)* numWrite);
          mergedFileArrayPos = numMerges*inputFileArraySize;
        }
      }
      //Close the merged file
      if (mergedFile.is_open()) mergedFile.close();

      //Close and remove the input files
      for (std::fstream &f : inputFiles) {
        if (f.is_open()) f.close();
      }
      for (auto fileInfo : mergeTask->files) {
        remove(fileInfo.first.c_str());
      }
    }
    catch (...) {
      //Close and remove the merged file
      if (mergedFile.is_open()) mergedFile.close();
      remove(mergeTask->mergedFileName.c_str());

      //Close and remove the input files
      for (std::fstream &f : inputFiles) {
        if (f.is_open()) f.close();
      }
      for (auto fileInfo : mergeTask->files) {
        remove(fileInfo.first.c_str());
      }
      throw;
    }
  }

  template<typename key>
  void ExternalMergeSort<key>::allocateData() {
    dataVec_.clear();
    dataVec_.resize(numThreads_);
    for (auto &vec : dataVec_) vec.resize(dataSizePerThread_);
  }

} //namespace ems
