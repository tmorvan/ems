// Sort a binary file of unsigned 32-bit integers using external merge sort
//

#include "ExternalMergeSort.h"

#include <iostream>
#include <cstdlib>

using namespace std;
using namespace ems;

int main(int argc, char** argv)
{
  if (argc < 3) {
    cerr << "Too few arguments " << endl;
    if (argc != 0) cerr << "Syntax : " << argv[0] << " inputFileName outputFileName [numThreads] [dataSizePerThread] [numMergesPerThread]" << endl;
    return 1;
  }

  ExternalMergeSort mergeSort;

  mergeSort.setInputFileName(argv[1]);
  mergeSort.setOutputFileName(argv[2]);
  if (argc >= 4) mergeSort.setNumThreads(atoi(argv[3]));
  if (argc >= 5) mergeSort.setDataSizePerThread(atoll(argv[4]));
  if (argc >= 6) mergeSort.setNumMergesPerThread(atoll(argv[5]));

  if (!mergeSort.sort()) {
    cerr << "Error during merge sort" << endl;
    return 1;
  }
  return 0;
}

