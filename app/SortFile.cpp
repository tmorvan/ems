// Sort a binary file of unsigned 32-bit integers using external merge sort
//

#include "ExternalMergeSort.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <memory>

using namespace std;
using namespace ems;

int main(int argc, char** argv)
{
  if (argc < 3) {
    cerr << "Too few arguments " << endl;
    if (argc != 0) cerr << "Syntax : " << argv[0] << " inputFileName outputFileName [keyType] [numThreads] [dataSizePerThread] [numMergesPerThread]" << endl;
    return 1;
  }

  string keyType = "uint32";
  if (argc >= 4) keyType = argv[3];

  unique_ptr<ExternalMergeSortBase> mergeSort;

  if (keyType == "uint8") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<uint8_t>);
  else if (keyType == "uint16") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<uint16_t>);
  else if (keyType == "uint32") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<uint32_t>);
  else if (keyType == "uint64") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<uint64_t>);
  else if (keyType == "int8") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<int8_t>);
  else if (keyType == "int16") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<int16_t>);
  else if (keyType == "int32") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<int32_t>);
  else if (keyType == "int64") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<int64_t>);
  else if (keyType == "float") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<float>);
  else if (keyType == "double") mergeSort = unique_ptr<ExternalMergeSortBase>(new ExternalMergeSort<double>);
  else {
    cerr << "Invalid key type " << keyType.c_str() << endl;
    return 1;
  }

  mergeSort->setInputFileName(argv[1]);
  mergeSort->setOutputFileName(argv[2]);
  if (argc >= 5) mergeSort->setNumThreads(atoi(argv[4]));
  if (argc >= 6) mergeSort->setDataSizePerThread(atoll(argv[5]));
  if (argc >= 7) mergeSort->setNumMergesPerThread(atoll(argv[6]));

  if (!mergeSort->sort()) {
    cerr << "Error during merge sort" << endl;
    return 1;
  }
  return 0;
}

