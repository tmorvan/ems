#pragma once

#include <vector>
#include <random>
#include <limits>
#include <cstdio>
#include <fstream>
#include <algorithm>
#include <iostream>

namespace ems {

  template<typename key>
  bool createRandomFile<key>(std::string fileName, long long numValues, long long chunkSize) {
    if ((numValues <= 0) || (chunkSize <= 0) || (fileName.empty())) return false;
    
    std::fstream outFile;

    //Random number generation
    double minDouble = std::numeric_limits<double>::min();
    double minKey = static_cast<double>(std::numeric_limits<key>::min());
    double distMin = std::max(minDouble, minKey);
    double maxDouble = std::numeric_limits<double>::max();
    double maxKey = static_cast<double>(std::numeric_limits<key>::max());
    double distMax = std::min(maxDouble, maxKey);
    std::uniform_real_distribution<double> dist(distMin, distMax);
    std::random_device rd;

    //Write dataSize random integer values
    outFile.open(fileName, std::ios::out | std::ios::binary);
    if (!outFile.is_open()) return false;

    long long numChunks = numValues / chunkSize;
    long long lastChunkSize = numValues % chunkSize;
    if (lastChunkSize != 0) numChunks++;
    else lastChunkSize = chunkSize;

    std::vector<key> chunk(chunkSize);

    for (long long i = 0; i < numChunks ; i++) {
      long long currentChunkSize = (i == (numChunks - 1)) ? lastChunkSize : chunkSize;
      for (int i = 0; i < currentChunkSize; i++) chunk[i] = static_cast<key>(dist(rd));
      outFile.write(reinterpret_cast<char *>(&chunk[0]), sizeof(key) * currentChunkSize);
      if (outFile.fail()) {
        remove(fileName.c_str());
        return false;
      }
    }

    return true;

  }

  template<typename key>
  bool checkSortedFile<key>(std::string fileName) {
    std::fstream inFile;
    inFile.open(fileName, std::ios::in | std::ios::binary);
    if (!inFile.is_open()) return false;

    //Get the size of the input file
    inFile.seekg(0, std::ios::end);
    long long dataLength = inFile.tellg();
    inFile.seekg(0, std::ios::beg);

    //Check that the file is of the right size
    if (dataLength % sizeof(key)) return false;

    long long numValues = dataLength / sizeof(key);

    //File must contain at least one value
    if (numValues == 0) return false;

    key val;
    key previousVal = std::numeric_limits<key>::min();

    for (long long i = 0; i < numValues; i++) {
      //Read the value and compare it to the previous
      inFile.read(reinterpret_cast<char *>(&val), sizeof(key));
      if (inFile.fail()) return false;
      if (val < previousVal) return false;
      previousVal = val;
    }
    return true;
  }

  std::string findAvailableFileName(std::string desiredFileName, int &appendNumber) {
    //Try to open the file
    std::string testFileName;
    std::fstream testFile;
    if (appendNumber < 0) {
      testFileName = desiredFileName;
      appendNumber = -1;
    }
    else {
      testFileName = desiredFileName + std::to_string(appendNumber);
    }
    testFile.open(testFileName, std::ios::in | std::ios::binary);
    while (testFile.is_open() && (appendNumber < std::numeric_limits<int>::max())) {
      appendNumber++;
      //file exists try another file
      testFile.close();
      testFileName = desiredFileName + std::to_string(appendNumber);
      testFile.open(testFileName, std::ios::in | std::ios::binary);
    }
    if (testFile.is_open()) {
      testFile.close();
      appendNumber = -1;
      return std::string();
    }
    return testFileName;
  }

  void writeProfilingFile(std::string profilingFileName, int numThreads, TimePoint startTime, TimePoint endTime, const std::vector<std::shared_ptr<Task>> &completedTasks) {
    std::fstream profilingFile;
    profilingFile.open(profilingFileName,std::ios_base::out);
    if (!profilingFile.is_open()) {
      std::cerr << "writeProfilingFile: Could not open file " << profilingFileName << std::endl;
    }
    profilingFile << numThreads << ' ' << std::chrono::duration_cast<std::chrono::nanoseconds>(endTime-startTime).count() << std::endl;
    for (auto task : completedTasks) {
      if (!task) continue;
      profilingFile << typeid(*task).name() << std::endl;
      profilingFile << task->handlingThreadId;
      profilingFile << ' ';
      profilingFile << std::chrono::duration_cast<std::chrono::nanoseconds>(task->startTime - startTime).count();
      profilingFile << ' ';
      profilingFile << std::chrono::duration_cast<std::chrono::nanoseconds>(task->endTime - startTime).count();
      profilingFile << std::endl;
    }
  }


} //namespace ems
