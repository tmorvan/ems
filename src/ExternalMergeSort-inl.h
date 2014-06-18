#include <cstdio>
#include <thread>
#include <queue>

namespace ems {

  template<typename key>
  ExternalMergeSort<key>::ExternalMergeSort() {
    //initial allocation
    allocateData();
  }

  template<typename key>
  bool ExternalMergeSort<key>::sort() {
    try {
      if ((inputFileName_.empty()) || (outputFileName_.empty())) {
        cerr << "ExternalMergeSort::sort No input or output file specified" << endl;
        cleanup();
        return false;
      }

      //Open the input file
      inFile_.exceptions(fstream::failbit | fstream::badbit);
      inFile_.open(inputFileName_, ios::in | ios::binary);
      if (!inFile_.is_open()) {
        cerr << "ExternalMergeSort::sortCould not open file " << inputFileName_ << endl;
        cleanup();
        return false;
      }

      //The threads used for processing
      std::vector<thread> sortThreads(numThreads_);

      threadError_ = false;

      chunkSize_ = dataSizePerThread_;

      chunkFileId_ = 0;

      long long chunkSizeBytes = sizeof(key) * chunkSize_;

      //Get the size of the input file
      inFile_.seekg(0, ios::end);
      long long dataLength = inFile_.tellg();
      inFile_.seekg(0, ios::beg);

      //File size should be a multiple of sizeof(key)
      if (dataLength % sizeof(key)) {
        cerr << "ExternalMergeSort::sort Invalid file size" << endl;
        cleanup();
        return false;
      }

      //Get the number of chunks
      numChunks_ = dataLength / chunkSizeBytes;
      lastChunkSize_ = (dataLength%chunkSizeBytes) / sizeof(key);
      if (lastChunkSize_ == 0) lastChunkSize_ = chunkSize_;
      else numChunks_++;


      //Create the files for each chunk 
      for (long long i = 0; i < numChunks_; i++) {
        std::fstream chunkFile;
        std::string chunkFileName = outputFileName_;
        //If only one chunk, write directly to output file, otherwise create chunk files
        if (numChunks_ >1) {
          //Try to find a temporary name that does not exist
          chunkFileName = outputFileName_ + to_string(chunkFileId_++);
          chunkFile.open(chunkFileName.c_str(), ios::in | ios::binary);
          while (chunkFile.is_open()) {
            chunkFile.close();
            chunkFileName = outputFileName_ + to_string(chunkFileId_++);
            chunkFile.open(chunkFileName.c_str(), ios::in | ios::binary);
            if (chunkFileId_ == std::numeric_limits<long long>::max()) {
              cerr << "ExternalMergeSort::sort Ran out of temporary file names" << endl;
              cleanup();
              return false;
            }
          }
          chunkFile.clear();
        }
        chunkFile.exceptions(fstream::failbit | fstream::badbit);
        chunkFiles_.push_back(make_pair(chunkFileName, move(chunkFile)));
      }

      currentChunk_ = 0;

      //Spawn the threads to sort the chunks
      for (int i = 0; i < numThreads_; i++) sortThreads[i] = thread(&ExternalMergeSort<key>::sortChunk, this, i);
      //Synchronize
      for (int i = 0; i<numThreads_; i++) sortThreads[i].join();

      //If there was an io error cleanup and exit
      if (threadError_) {
        cerr << "ExternalMergeSort::sort Error encountered during sort" << endl;
        cleanup();
        return false;
      }

      //Successively merge chunks in parallel until only one is left
      while (numChunks_>1) {
        mergeChunkSize_ = numMergesPerThread_*chunkSize_;
        //Numbers of chunks once merged
        numMergeChunks_ = numChunks_ / numMergesPerThread_;
        numMergesLastThread_ = numChunks_ % numMergesPerThread_;
        if (numMergesLastThread_ == 0) numMergesLastThread_ = numMergesPerThread_;
        else numMergeChunks_++;
        lastMergeChunkSize_ = (numMergesLastThread_ - 1)*chunkSize_ + lastChunkSize_;

        //Create the files for each merged chunk 
        for (long long i = 0; i < numMergeChunks_; i++) {
          std::fstream chunkFile;
          std::string chunkFileName = outputFileName_;
          //If only one merged chunk, write directly to output file, otherwise create merged chunk files
          if (numMergeChunks_>1) {
            //Try to find a temporary name that does not exist
            chunkFileName = outputFileName_ + to_string(chunkFileId_++);
            chunkFile.open(chunkFileName.c_str(), ios::in | ios::binary);
            while (chunkFile.is_open()) {
              chunkFile.close();
              chunkFileName = outputFileName_ + to_string(chunkFileId_++);
              chunkFile.open(chunkFileName.c_str(), ios::in | ios::binary);
              if (chunkFileId_ == std::numeric_limits<long long>::max()) {
                cerr << "ExternalMergeSort::sort Ran out of temporary file names" << endl;
                cleanup();
                return false;
              }
            }
            chunkFile.clear();
          }
          chunkFile.exceptions(std::fstream::failbit | std::fstream::badbit);
          chunkFiles_.push_back(make_pair(chunkFileName, move(chunkFile)));
        }

        currentMergeChunk_ = 0;

        //Spawn the threads to merge the chunks
        for (int i = 0; i < numThreads_; i++) sortThreads[i] = std::thread(&ExternalMergeSort<key>::mergeChunks, this, i);
        //Synchronize
        for (int i = 0; i < numThreads_; i++) sortThreads[i].join();

        //If there was an io error cleanup and exit
        if (threadError_) {
          cerr << "ExternalMergeSort::sort Error encountered during merge" << endl;
          cleanup();
          return false;
        }

        //Erase the files of the chunks that have been merged
        chunkFiles_.erase(chunkFiles_.begin(), chunkFiles_.begin() + numChunks_);

        //Merged chunks become the new chunks
        numChunks_ = numMergeChunks_;
        chunkSize_ = mergeChunkSize_;
        lastChunkSize_ = lastMergeChunkSize_;
      }

      inFile_.close();
      inFile_.clear();
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
  void ExternalMergeSort<key>::sortChunk(int threadId) {
    try {
      long long threadCurrentChunk;
      long long threadChunkSize;
      while (!threadError_) {
        //Get the current chunk
        threadCurrentChunk = currentChunk_++;
        //If done, stop the thread
        if (threadCurrentChunk >= numChunks_) return;
        //Open the file for this chunk
        chunkFiles_[threadCurrentChunk].second.open(chunkFiles_[threadCurrentChunk].first.c_str(), std::ios::out | std::ios::binary);
        threadChunkSize = chunkSize_;
        if (threadCurrentChunk == numChunks_ - 1) threadChunkSize = lastChunkSize_;
        //Read the data in this thread data vector
        inFileMutex_.lock();
        try {
          inFile_.seekg(threadCurrentChunk*chunkSize_ * sizeof(key));
          inFile_.read(reinterpret_cast<char *>(&(dataVec_[threadId][0])), sizeof(key) * threadChunkSize);
        }
        catch (...) {
          //Unlock the mutex if an exception occured
          inFileMutex_.unlock();
          throw;
        }
        inFileMutex_.unlock();
        if (threadError_) return;
        //sort the chunk
        std::sort(dataVec_[threadId].begin(), dataVec_[threadId].begin() + threadChunkSize);
        //Write the chunk to the corresponding chunk file
        chunkFiles_[threadCurrentChunk].second.write(reinterpret_cast<char *>(&(dataVec_[threadId][0])), sizeof(key) * threadChunkSize);

        //Close the file for this chunk
        chunkFiles_[threadCurrentChunk].second.close();
        chunkFiles_[threadCurrentChunk].second.clear();
      }
    }
    catch (...) {
      //Stop the threads if an exception occured
      threadError_ = true;
      throw;
    }
  }

  //Each chunk file is allocated an input buffer in the dataVec of this thread.
  //An output buffer for the merged chunk file is also allocated
  template<typename key>
  void ExternalMergeSort<key>::mergeChunks(int threadId) {
    try {
      long long threadCurrentMergeChunk;
      long long threadNumMerges;
      long long threadMergeChunkSize;
      while (!threadError_) {
        threadCurrentMergeChunk = currentMergeChunk_++;
        //If done, stop the thread
        if (threadCurrentMergeChunk >= numMergeChunks_) return;
        threadNumMerges = numMergesPerThread_;
        threadMergeChunkSize = mergeChunkSize_;
        if (threadCurrentMergeChunk == (numMergeChunks_ - 1)) {
          threadNumMerges = numMergesLastThread_;
          threadMergeChunkSize = lastMergeChunkSize_;
        }

        //Open the files for the input chunks in read mode
        for (long long i = 0; i < threadNumMerges; i++) {
          long long indChunkFile = threadCurrentMergeChunk*numMergesPerThread_ + i;
          chunkFiles_[indChunkFile].second.open(chunkFiles_[indChunkFile].first.c_str(), std::ios::in | std::ios::binary);
          if (!chunkFiles_[indChunkFile].second.is_open()) {
            threadError_ = true;
            return;
          }
        }
        //Open the file for the output merged chunk in write mode
        long long indMergeChunkFile = numChunks_ + threadCurrentMergeChunk;
        chunkFiles_[indMergeChunkFile].second.open(chunkFiles_[indMergeChunkFile].first.c_str(), std::ios::out | std::ios::binary);

        //Pointer in the file and size for each chunk
        std::vector<long long> threadChunkStreamPos(threadNumMerges, 0);
        std::vector<long long> threadChunkSize(threadNumMerges, chunkSize_);

        if (threadCurrentMergeChunk == (numMergeChunks_ - 1)) threadChunkSize[threadNumMerges - 1] = lastChunkSize_;

        //Size of the buffer for each chunk in the data vector
        long long threadChunkDataSize = dataSizePerThread_ / (threadNumMerges + 1);
        //Size of the buffer for the merged chunk in the data vector
        long long threadMergeChunkDataSize = dataSizePerThread_ - threadNumMerges*threadChunkDataSize;

        //Pointers for each chunk in the thread data vector
        std::vector<long long> threadChunkDataInd(threadNumMerges);
        //Set the pointers for each chunk at the end of its buffer 
        for (long long i = 0; i < threadNumMerges; i++) threadChunkDataInd[i] = i*(threadChunkDataSize+1);

        //Pointer for the merged chunk in the thread data vector
        long long threadMergeChunkDataInd = threadNumMerges*threadChunkDataSize;

        //Priority queue keeping track of the values at the current pointers in the thread data vector
        std::priority_queue<std::pair<key,long long>, std::vector<std::pair<key,long long>>, std::greater<std::pair<key,long long>>> mergeQueue;

        //Perform N-way merge of the chunks
        //Load the initial data and fill the priority queue
        for (long long i = 0; i < threadNumMerges; i++) {
          //If the end for this chunk has been reached skip to the next chunk
          if (threadChunkStreamPos[i] == threadChunkSize[i]) continue;

          //Load data for chunk i
          long long numRead = min<long long>(threadChunkDataSize, threadChunkSize[i] - threadChunkStreamPos[i]);
          if (numRead) {
            long long indChunkFile = threadCurrentMergeChunk*numMergesPerThread_ + i;
            //Point to the beginning of the buffer for this chunk
            threadChunkDataInd[i] = i*threadChunkDataSize;
            //Read the data
            chunkFiles_[indChunkFile].second.read(reinterpret_cast<char *>(&(dataVec_[threadId][threadChunkDataInd[i]])), sizeof(key)* numRead);
            mergeQueue.push(std::make_pair(dataVec_[threadId][threadChunkDataInd[i]],i));
          }
        }

        //While the queue is not empty, dequeue the top element, add it to the result and try to fetch another value from the same chunk
        while (!mergeQueue.empty()) {
          auto topPair = mergeQueue.top();
          mergeQueue.pop();
          dataVec_[threadId][threadMergeChunkDataInd++] = topPair.first;

          //Add data to the queue from the chunk we just poped
          //Increment the pointers for this chunk
          threadChunkDataInd[topPair.second]++;
          threadChunkStreamPos[topPair.second]++;
          //Check that the end of the stream has not been reached
          if (threadChunkStreamPos[topPair.second] != threadChunkSize[topPair.second]) {
            //Read more data if necessary
            if (threadChunkDataInd[topPair.second] == (topPair.second + 1)*threadChunkDataSize) {
              long long numRead = min<long long>(threadChunkDataSize, threadChunkSize[topPair.second] - threadChunkStreamPos[topPair.second]);
              if (numRead) {
                long long indChunkFile = threadCurrentMergeChunk*numMergesPerThread_ + topPair.second;
                //Point to the beginning of the buffer for this chunk
                threadChunkDataInd[topPair.second] = topPair.second*threadChunkDataSize;
                //Read the data
                chunkFiles_[indChunkFile].second.read(reinterpret_cast<char *>(&(dataVec_[threadId][threadChunkDataInd[topPair.second]])), sizeof(key)* numRead);
              }
            }
            //Add to the queue
            mergeQueue.push(std::make_pair(dataVec_[threadId][threadChunkDataInd[topPair.second]],topPair.second));
          }
          //Write the merged data if the output buffer is full or the queue is empty
          if ((threadMergeChunkDataInd == dataSizePerThread_) || mergeQueue.empty()) {
            long long numWrite = threadMergeChunkDataInd - threadNumMerges*threadChunkDataSize;
            chunkFiles_[indMergeChunkFile].second.write(reinterpret_cast<char *>(&(dataVec_[threadId][threadNumMerges*threadChunkDataSize])), sizeof(key)* numWrite);
            threadMergeChunkDataInd = threadNumMerges*threadChunkDataSize;
          }
        }

        //Close the file for the output merged chunk
        chunkFiles_[indMergeChunkFile].second.close();
        chunkFiles_[indMergeChunkFile].second.clear();

        //Close and remove the files for the input chunks
        for (long long i = 0; i < threadNumMerges; i++) {
          long long indChunkFile = threadCurrentMergeChunk*numMergesPerThread_ + i;
          chunkFiles_[indChunkFile].second.close();
          if (remove(chunkFiles_[indChunkFile].first.c_str())) {
            threadError_ = true;
            return;
          }
        }
      }
    }
    catch (...) {
      //Stop the threads if an exception occured
      threadError_ = true;
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
