#include "Util.h"

#include <vector>
#include <random>
#include <limits>
#include <cstdint>
#include <cstdio>
#include <fstream>

using namespace std;

namespace ems {

  bool Util::createRandomFile(std::string fileName, long long numValues, long long chunkSize) {
    if ((numValues <= 0) || (chunkSize <= 0) || (fileName.empty())) return false;
    
    fstream outFile;

    //Random number generation
    uniform_int_distribution<uint32_t> dist(numeric_limits<uint32_t>::min(), numeric_limits<uint32_t>::max());
    random_device rd;

    //Write dataSize random integer values
    outFile.open(fileName, ios::out | ios::binary);
    if (!outFile.is_open()) return false;

    long long numChunks = numValues / chunkSize;
    long long lastChunkSize = numValues % chunkSize;
    if (lastChunkSize != 0) numChunks++;
    else lastChunkSize = chunkSize;

    vector<uint32_t> chunk(chunkSize);

    for (long long i = 0; i < numChunks ; i++) {
      long long currentChunkSize = (i == (numChunks - 1)) ? lastChunkSize : chunkSize;
      for (int i = 0; i < currentChunkSize; i++) chunk[i] = dist(rd);
      outFile.write(reinterpret_cast<char *>(&chunk[0]), 4 * currentChunkSize);
      if (outFile.fail()) {
        remove(fileName.c_str());
        return false;
      }
    }

    return true;

  }

  bool Util::checkSortedFile(std::string fileName) {
    fstream inFile;
    inFile.open(fileName, ios::in | ios::binary);
    if (!inFile.is_open()) return false;

    //Get the size of the input file
    inFile.seekg(0, ios::end);
    long long dataLength = inFile.tellg();
    inFile.seekg(0, ios::beg);

    //Check that the file is of the right size
    if (dataLength % 4) return false;

    long long numValues = dataLength / 4;

    //File must contain at least one value
    if (numValues == 0) return false;

    uint32_t val;
    uint32_t previousVal = numeric_limits<uint32_t>::min();

    for (long long i = 0; i < numValues; i++) {
      //Read the value and compare it to the previous
      inFile.read(reinterpret_cast<char *>(&val), 4);
      if (inFile.fail()) return false;
      if (val < previousVal) return false;
      previousVal = val;
    }
    return true;
  }

  std::string Util::findAvailableFileName(std::string desiredFileName) {
    //Try to open the file
    string testFileName = desiredFileName;
    fstream testFile;
    testFile.open(testFileName, ios::in | ios::binary);
    int nameInd = 0;
    while (testFile.is_open() && (nameInd < std::numeric_limits<int>::max())) {
      //file exists try another file
      testFile.close();
      testFileName = desiredFileName + to_string(nameInd);
      testFile.open(testFileName, ios::in | ios::binary);
      nameInd++;
    }
    if (testFile.is_open()) {
      testFile.close();
      return string();
    }
    return testFileName;
  }

} //namespace ems
