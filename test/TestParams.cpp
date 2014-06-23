// Test input parameter validity of ExternalMergeSort
// Check that we always have numThreads>=1 dataSizePerThreads>=3 and dataSizePerThreads-1 >= numMergesPerThread >= 2

#include "ExternalMergeSort.h"

#include <cstdint>
#include <memory>



int main(int argc, char** argv)
{
  ems::ExternalMergeSort<uint32_t> mergeSort;

  mergeSort.setNumThreads(0);
  if (mergeSort.getNumThreads() != 1) return 1;

  mergeSort.setDataSizePerThread(0);
  if (mergeSort.getDataSizePerThread() != 3LL) return 1;

  mergeSort.setNumMergesPerThread(0);
  if (mergeSort.getNumMergesPerThread() != 2LL) return 1;

  mergeSort.setNumMergesPerThread(mergeSort.getDataSizePerThread() + 1);
  if ((mergeSort.getDataSizePerThread() - 1) != mergeSort.getNumMergesPerThread()) return 1;

  return 0;
}

