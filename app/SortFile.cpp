// Sort a binary file of unsigned 32-bit integers using external merge sort
//

#include "ExternalMergeSort.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <memory>

int main(int argc, char** argv)
{
  if (argc < 3) {
    std::cerr << "Too few arguments " << std::endl;
    if (argc != 0) std::cerr << "Syntax : " << argv[0] << " inputFileName outputFileName [keyType] [numThreads] [dataSizePerThread] [numMergesPerThread] [profilingFileName]" << std::endl;
    return 1;
  }

  std::string keyType = "uint32";
  if (argc >= 4) keyType = argv[3];

  std::unique_ptr<ems::ExternalMergeSortBase> mergeSort;

  if (keyType == "uint8") mergeSort = std::make_unique<ems::ExternalMergeSort<uint8_t>>();
  else if (keyType == "uint16") mergeSort = std::make_unique<ems::ExternalMergeSort<uint16_t>>();
  else if (keyType == "uint32") mergeSort = std::make_unique<ems::ExternalMergeSort<uint32_t>>();
  else if (keyType == "uint64") mergeSort = std::make_unique<ems::ExternalMergeSort<uint64_t>>();
  else if (keyType == "int8") mergeSort = std::make_unique<ems::ExternalMergeSort<int8_t>>();
  else if (keyType == "int16") mergeSort = std::make_unique<ems::ExternalMergeSort<int16_t>>();
  else if (keyType == "int32") mergeSort = std::make_unique<ems::ExternalMergeSort<int32_t>>();
  else if (keyType == "int64") mergeSort = std::make_unique<ems::ExternalMergeSort<int64_t>>();
  else if (keyType == "float") mergeSort = std::make_unique<ems::ExternalMergeSort<float>>();
  else if (keyType == "double") mergeSort = std::make_unique<ems::ExternalMergeSort<double>>();
  else {
    std::cerr << "Invalid key type " << keyType.c_str() << std::endl;
    return 1;
  }

  mergeSort->setInputFileName(argv[1]);
  mergeSort->setOutputFileName(argv[2]);
  if (argc >= 5) mergeSort->setNumThreads(atoi(argv[4]));
  if (argc >= 6) mergeSort->setDataSizePerThread(atoll(argv[5]));
  if (argc >= 7) mergeSort->setNumMergesPerThread(atoll(argv[6]));
  if (argc >= 8) mergeSort->setProfilingFileName(argv[7]);

  if (!mergeSort->sort()) {
    std::cerr << "Error during merge sort" << std::endl;
    return 1;
  }
  return 0;
}

