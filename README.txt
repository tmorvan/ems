This repository contains a multi-threaded implementation of external merge sort for large binary files.

The current implementation is restricted to 32-bits integers.

The external merge sort implementation is provided in the ems library.

sortfile is an executable to perform the sorting from the command line:
sortfile inputFileName outputFileName [numThreads] [dataSizePerThread] [numMergesPerThread]

Test files can be created using the createrandomfile utility:
createrandomfile fileName numValues [chunkSize]

Result files can be checked using the checksortedfile utility:
checksortedfile fileName

Licensed under the MIT license (see LICENSE.txt for details).

