// Sort a binary file of unsigned 32-bit integers using external merge sort
//

#include "ExternalMergeSort.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <memory>

#ifdef WITH_CUDA
#include "SortFileCuda.h"
#endif //WITH_CUDA

int numThreads;
long long dataSizePerThread;
long long numMergesPerThread;
std::string inputFileName;
std::string outputFileName;
std::string profilingFileName;

#ifdef WITH_CUDA
int numGpuThreads;
#endif //WITH_CUDA

template<typename key>
int sortFile() {
  ems::ExternalMergeSort<key> mergeSort;
  mergeSort.setInputFileName(inputFileName.c_str());
  mergeSort.setOutputFileName(outputFileName.c_str());
#ifdef WITH_CUDA
  if (numGpuThreads > 0) numThreads += numGpuThreads;
#endif //WITH_CUDA
  mergeSort.setNumThreads(numThreads);
#ifdef WITH_CUDA
  //Use thrust (radix) as default for now
  ems::SortFunction<key> thrustSortFunc = thrust::sort<typename std::vector<key>::iterator>;
  mergeSort.setSortFunction(thrustSortFunc);
  ems::SortFunction<key> cudaSortFunc = sortCuda<key>;
  for (int i = 0; i < numGpuThreads; i++) mergeSort.setSortFunction(cudaSortFunc, i);
#endif //WITH_CUDA
  mergeSort.setDataSizePerThread(dataSizePerThread);
  mergeSort.setNumMergesPerThread(numMergesPerThread);
  if(!profilingFileName.empty()) mergeSort.setProfilingFileName(profilingFileName.c_str());



  if (!mergeSort.sort()) {
    std::cerr << "SortFile: Sort failed" << std::endl;
    return 1;
  }
  return 0;
}

int main(int argc, char** argv)
{
  if (argc < 3) {
    std::cerr << "Too few arguments " << std::endl;
    if (argc != 0) std::cerr << "Syntax : " << argv[0] << " inputFileName outputFileName [keyType] [numThreads] [dataSizePerThread] [numMergesPerThread] [numGpuThreads] [profilingFileName]" << std::endl;
    return 1;
  }

  inputFileName = argv[1];
  outputFileName = argv[2];
  numThreads = std::max(1u, std::thread::hardware_concurrency());
  if (argc > 4) numThreads = atoi(argv[4]);


  std::string keyType = "uint32";
  if (argc > 3) keyType = argv[3];
  dataSizePerThread = 10000000LL;
  if (argc > 5) dataSizePerThread = atoll(argv[5]);
  numMergesPerThread = 10LL;
  if (argc > 6) numMergesPerThread = atoll(argv[6]);

  int lastArg = 7;

#ifdef WITH_CUDA
  numGpuThreads = 0;
  if (argc > lastArg) {
    numGpuThreads = atoi(argv[lastArg++]);
  }
#endif //WITH_CUDA

  if (argc > lastArg) profilingFileName = argv[lastArg];

  if (keyType == "uint8") return sortFile<uint8_t>();
  else if (keyType == "uint16") return sortFile<uint16_t>();
  else if (keyType == "uint32") return sortFile<uint32_t>();
  else if (keyType == "uint64") return sortFile<uint64_t>();
  else if (keyType == "int8") return sortFile<int8_t>();
  else if (keyType == "int16") return sortFile<int16_t>();
  else if (keyType == "int32") return sortFile<int32_t>();
  else if (keyType == "int64") return sortFile<int64_t>();
  else if (keyType == "float") return sortFile<float>();
  else if (keyType == "double") return sortFile<double>();

  std::cerr << "Invalid key type " << keyType.c_str() << std::endl;
  return 1;
}

