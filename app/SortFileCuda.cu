// Sort a binary file of unsigned 32-bit integers using external merge sort
//

#include "SortFileCuda.h"

#include <thrust/copy.h>
#include <thrust/device_vector.h>
#include <iostream>

template<typename key> void sortCudaDevice(typename std::vector<key>::iterator beginIt, typename std::vector<key>::iterator endIt) {
  // transfer data to the device
  thrust::device_vector<key> dVec(beginIt, endIt);

  // sort data on the device
  thrust::sort(dVec.begin(), dVec.end());

  // transfer data back to host
  thrust::copy(dVec.begin(), dVec.end(), beginIt);
  
}

template<> void sortCuda<uint8_t>(std::vector<uint8_t>::iterator beginIt, std::vector<uint8_t>::iterator endIt) {
  sortCudaDevice<uint8_t>(beginIt,endIt);
}
template<> void sortCuda<uint16_t>(std::vector<uint16_t>::iterator beginIt, std::vector<uint16_t>::iterator endIt) {
  sortCudaDevice<uint16_t>(beginIt,endIt);
}
template<> void sortCuda<uint32_t>(std::vector<uint32_t>::iterator beginIt, std::vector<uint32_t>::iterator endIt) {
  sortCudaDevice<uint32_t>(beginIt,endIt);
}
template<> void sortCuda<uint64_t>(std::vector<uint64_t>::iterator beginIt, std::vector<uint64_t>::iterator endIt) {
  sortCudaDevice<uint64_t>(beginIt,endIt);
}
template<> void sortCuda<int8_t>(std::vector<int8_t>::iterator beginIt, std::vector<int8_t>::iterator endIt) {
  sortCudaDevice<int8_t>(beginIt,endIt);
}
template<> void sortCuda<int16_t>(std::vector<int16_t>::iterator beginIt, std::vector<int16_t>::iterator endIt) {
  sortCudaDevice<int16_t>(beginIt,endIt);
}
template<> void sortCuda<int32_t>(std::vector<int32_t>::iterator beginIt, std::vector<int32_t>::iterator endIt) {
  sortCudaDevice<int32_t>(beginIt,endIt);
}
template<> void sortCuda<int64_t>(std::vector<int64_t>::iterator beginIt, std::vector<int64_t>::iterator endIt) {
  sortCudaDevice<int64_t>(beginIt,endIt);
}
template<> void sortCuda<float>(std::vector<float>::iterator beginIt, std::vector<float>::iterator endIt) {
  sortCudaDevice<float>(beginIt,endIt);
}
template<> void sortCuda<double>(std::vector<double>::iterator beginIt, std::vector<double>::iterator endIt) {
  sortCudaDevice<double>(beginIt,endIt);
}