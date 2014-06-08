#include <vector>
#include <random>
#include <limits>
#include <cstdio>
#include <fstream>
#include <algorithm>

namespace ems {

  template<typename key>
  bool Util<key>::createRandomFile(std::string fileName, long long numValues, long long chunkSize) {
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
    outFile.open(fileName, ios::out | ios::binary);
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
  bool Util<key>::checkSortedFile(std::string fileName) {
    std::fstream inFile;
    inFile.open(fileName, ios::in | ios::binary);
    if (!inFile.is_open()) return false;

    //Get the size of the input file
    inFile.seekg(0, ios::end);
    long long dataLength = inFile.tellg();
    inFile.seekg(0, ios::beg);

    //Check that the file is of the right size
    if (dataLength % sizeof(key)) return false;

    long long numValues = dataLength / sizeof(key);

    //File must contain at least one value
    if (numValues == 0) return false;

    key val;
    key previousVal = numeric_limits<key>::min();

    for (long long i = 0; i < numValues; i++) {
      //Read the value and compare it to the previous
      inFile.read(reinterpret_cast<char *>(&val), sizeof(key));
      if (inFile.fail()) return false;
      if (val < previousVal) return false;
      previousVal = val;
    }
    return true;
  }

} //namespace ems
