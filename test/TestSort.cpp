// Test ExternalMergeSort using a randomly generated file

#include "Util.h"
#include "ExternalMergeSort.h"

#include <iostream>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <fstream>
#include <memory>

using namespace std;
using namespace ems;

//Input and output files generated by the test
string inputFileName;
string outputFileName;

//Remove all temporary files
void cleanup() {
  if (!inputFileName.empty()) remove(inputFileName.c_str());
  inputFileName.clear();
  if (!outputFileName.empty()) remove(outputFileName.c_str());
  outputFileName.clear();
}

template<typename key>
bool testSort() {
  Util<key> util;
  ExternalMergeSort<key> mergeSort;

  try {
    //Find an available input filename
    inputFileName = util.findAvailableFileName("testsort_input");
    if (inputFileName.empty()) return false;

    //Find an available output filename
    outputFileName = util.findAvailableFileName("testsort_output");
    if (outputFileName.empty()) return false;

    //Create the input file
    if (!util.createRandomFile(inputFileName, 1000, 1000)) {
      cleanup();
      return false;
    }

    //Perform the sort
    mergeSort.setInputFileName(inputFileName.c_str());
    mergeSort.setOutputFileName(outputFileName.c_str());
    mergeSort.setDataSizePerThread(100);
    mergeSort.setNumMergesPerThread(4);
    if (!mergeSort.sort()) {
      cleanup();
      return false;
    }

    //Check the result
    if (!util.checkSortedFile(outputFileName)) {
      cleanup();
      return false;
    }

    cleanup();
    return true;
  }
  catch (...) {
    cleanup();
    throw;
  }
}

int main(int argc, char** argv)
{
  //Test with all basic types
  if (!testSort<uint8_t>()) return 1;
  if (!testSort<uint16_t>()) return 1;
  if (!testSort<uint32_t>()) return 1;
  if (!testSort<uint64_t>()) return 1;
  if (!testSort<int8_t>()) return 1;
  if (!testSort<int16_t>()) return 1;
  if (!testSort<int32_t>()) return 1;
  if (!testSort<int64_t>()) return 1;
  if (!testSort<float>()) return 1;
  if (!testSort<double>()) return 1;

  return 0;
}

