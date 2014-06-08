This repository contains a multi-threaded implementation of external merge sort for large binary files.

The merge sort implementation is provided in the ExternalMergeSort class.

sortfile is an executable to perform the sorting from the command line:
sortfile inputFileName outputFileName [keyType] [numThreads] [dataSizePerThread] [numMergesPerThread]

Test files can be created using the createrandomfile utility:
createrandomfile fileName numValues [keyType] [chunkSize]

Result files can be checked using the checksortedfile utility:
checksortedfile fileName [keyType]

keyType can be one of the following (default uint32):
uint8 uint16 uint32 uint64 int8 int16 int32 int64 float double 

Licensed under the MIT license (see LICENSE.txt for details).

