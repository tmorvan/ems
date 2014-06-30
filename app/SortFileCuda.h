// Sort a binary file of unsigned 32-bit integers using external merge sort
//

#pragma once

#include <thrust/sort.h>
#include <vector>
#include <stdint.h>

template<typename key>
void sortCuda(typename std::vector<key>::iterator beginIt, typename std::vector<key>::iterator endIt) {
  thrust::sort(beginIt,endIt);
}

//Specialization for types supported on the GPU
template<> void sortCuda<uint8_t>(std::vector<uint8_t>::iterator beginIt, std::vector<uint8_t>::iterator endIt);
template<> void sortCuda<uint16_t>(std::vector<uint16_t>::iterator beginIt, std::vector<uint16_t>::iterator endIt);
template<> void sortCuda<uint32_t>(std::vector<uint32_t>::iterator beginIt, std::vector<uint32_t>::iterator endIt);
template<> void sortCuda<uint64_t>(std::vector<uint64_t>::iterator beginIt, std::vector<uint64_t>::iterator endIt);
template<> void sortCuda<int8_t>(std::vector<int8_t>::iterator beginIt, std::vector<int8_t>::iterator endIt);
template<> void sortCuda<int16_t>(std::vector<int16_t>::iterator beginIt, std::vector<int16_t>::iterator endIt);
template<> void sortCuda<int32_t>(std::vector<int32_t>::iterator beginIt, std::vector<int32_t>::iterator endIt);
template<> void sortCuda<int64_t>(std::vector<int64_t>::iterator beginIt, std::vector<int64_t>::iterator endIt);
template<> void sortCuda<float>(std::vector<float>::iterator beginIt, std::vector<float>::iterator endIt);
template<> void sortCuda<double>(std::vector<double>::iterator beginIt, std::vector<double>::iterator endIt);
