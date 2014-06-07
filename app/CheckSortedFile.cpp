// Check if a binary file of unsigned 32-bit integers is sorted 
//

#include "Util.h"

#include <iostream>

using namespace std;
using namespace ems;

int main(int argc, char** argv)
{
  if (argc < 2) {
    cerr << "Too few arguments " << endl;
    if (argc != 0) cerr << "Syntax : " << argv[0] << " fileName" << endl;
    return 1;
  }

  string fileName = argv[1];

  if (!Util::checkSortedFile(fileName)) {
    cout << "File " << fileName.c_str() << " is not sorted " << endl;
    return 1;
  }
  cout << "File " << fileName.c_str() << " is sorted " << endl;
  return 0;
}

