#!/usr/bin/python

import sys
import matplotlib.pyplot as plt
import matplotlib
import numpy

if (len(sys.argv)<2):
    print("Error no filename given")
    sys.exit(1)
    
f = open(sys.argv[1], 'r')

#Read the duration and number of threads info
line = f.readline()
readVal = line.split()
numThreads = int(readVal[0])
totalDuration = long(readVal[1])/1000000.0

basecolors = "bgrcmykw"
indColor = 0

#Read the tasks
taskList = []
#Unique task names
taskColors = {}
taskName = f.readline()
while taskName:
    if taskName not in taskColors:
        if (indColor<9):
            taskColors[taskName] = matplotlib.colors.ColorConverter.colors[basecolors[indColor]]
            indColor +=1
        else:
            taskColors[taskName] = numpy.random.rand(3,1)
    line = f.readline()
    readVal = line.split()
    taskList.append([taskName, int(readVal[0]), long(readVal[1])/1000000.0, long(readVal[2])/1000000.0])
    taskName = f.readline()

#plot
for task in taskList:
    plt.barh(task[1],task[3]-task[2],1,task[2],color=taskColors[task[0]])
    
if (len(sys.argv)>2):
    plt.savefig(sys.argv[2])

plt.show()
    
