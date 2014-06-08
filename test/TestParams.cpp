// Test input parameter validity of ExternalMergeSort
// Check that we always have numThreads>=1 dataSizePerThreads>=3 and dataSizePerThreads-1 >= numMergesPerThread >= 2

#include "ExternalMergeSort.h"

#include <cstdint>
#include <memory>

using namespace std;
using namespace ems;

template<typename key>
bool testParam() {
  ExternalMergeSort<key> mergeSort;

  mergeSort.setNumThreads(0);
  if (mergeSort.getNumThreads() != 1) return false;

  mergeSort.setDataSizePerThread(0);
  if (mergeSort.getDataSizePerThread() != 3LL) return false;

  mergeSort.setNumMergesPerThread(0);
  if (mergeSort.getNumMergesPerThread() != 2LL) return false;

  mergeSort.setNumMergesPerThread(mergeSort.getDataSizePerThread() + 1);
  if ((mergeSort.getDataSizePerThread() - 1) != mergeSort.getNumMergesPerThread()) return false;
  return true;
}

int main(int argc, char** argv)
{
  //Test with all basic types
  if (!testParam<uint8_t>()) return 1;
  if (!testParam<uint16_t>()) return 1;
  if (!testParam<uint32_t>()) return 1;
  if (!testParam<uint64_t>()) return 1;
  if (!testParam<int8_t>()) return 1;
  if (!testParam<int16_t>()) return 1;
  if (!testParam<int32_t>()) return 1;
  if (!testParam<int64_t>()) return 1;
  if (!testParam<float>()) return 1;
  if (!testParam<double>()) return 1;

  return 0;
}

