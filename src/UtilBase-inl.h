#include <limits>
#include <fstream>

namespace ems {
  std::string UtilBase::findAvailableFileName(std::string desiredFileName) {
    //Try to open the file
    std::string testFileName = desiredFileName;
    std::fstream testFile;
    testFile.open(testFileName, std::ios::in | std::ios::binary);
    int nameInd = 0;
    while (testFile.is_open() && (nameInd < std::numeric_limits<int>::max())) {
      //file exists try another file
      testFile.close();
      testFileName = desiredFileName + std::to_string(nameInd);
      testFile.open(testFileName, std::ios::in | std::ios::binary);
      nameInd++;
    }
    if (testFile.is_open()) {
      testFile.close();
      return std::string();
    }
    return testFileName;
  }

} //namespace ems
