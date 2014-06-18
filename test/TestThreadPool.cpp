// Test ThreadPool using a simple addition

#include "ThreadPool.h"

#include <iostream>
#include <atomic>

using namespace std;
using namespace ems;

//Used to store the atomic sum
atomic<long long> atomicSum ;

struct AtomicAddTask : public Task {
  long long number;
};

void atomicAddTaskHandler(int threadId, Task *task) {
  AtomicAddTask *addTask = dynamic_cast<AtomicAddTask *>(task);
  if (!addTask) return;
  atomicSum += addTask->number;
}

//Test using simple atomic addition, stops when the task queue is empty
bool atomicAddTest() {
  long long numValues = 1000;

  atomicSum = 0;

  //The tasks containing numbers to be added
  vector<shared_ptr<AtomicAddTask>> addTasks(numValues);

  for (auto i = 0; i < addTasks.size(); i++) {
    addTasks[i] = make_shared<AtomicAddTask>();
    addTasks[i]->number = (i + 1);
  }

  ThreadPool pool;

  for (auto &addTask : addTasks) pool.addTask(addTask);

  pool.addTaskHandler<AtomicAddTask>(atomicAddTaskHandler);

  //Perform the sum
  pool.handleTasks(4, true);
  //Join the threads
  pool.join();

  //Check that no exception occured
  if (pool.getThreadException()) return false;

  //Check the sum
  if (atomicSum != ((numValues*(numValues + 1)) / 2)) return false;

  return true;
}

struct ParallelAddTask : public Task {
  long long level;
  long long number[2];
};

void parallelAddTaskHandler(int threadId, Task *task) {
  ParallelAddTask *addTask = dynamic_cast<ParallelAddTask *>(task);
  if (!addTask) return;
  addTask->number[0] += addTask->number[1];
  addTask->level++;
}

//Test using additive parallel reduction, main threads adds new tasks to the queue until the top level is reached
bool parallelAddTest() {
  //Must be power of two
  long long numValues = 1024;

  //The tasks containing numbers to be added
  vector<shared_ptr<ParallelAddTask>> addTasks(numValues);

  long long numLevels = 0;
  long long levelSize = numValues;
  while (levelSize >>= 1) ++numLevels;



  for (long long i = 0; i < numValues/2; i++) {
    addTasks[i] = make_shared<ParallelAddTask>();
    addTasks[i]->number[0] = (i + 1);
    addTasks[i]->number[1] = numValues/2 + (i + 1);
    addTasks[i]->level = 0;
  }

  ThreadPool pool;

  for (auto &addTask : addTasks) pool.addTask(addTask);

  pool.addTaskHandler<ParallelAddTask>(parallelAddTaskHandler);

  //Perform the sum
  pool.handleTasks(4, false);

  long long parallelSum = 0;

  //Used to store completed tasks
  vector<shared_ptr<ParallelAddTask>> storedTasks(numLevels,nullptr);

  while (true) {
    std::shared_ptr<ParallelAddTask> completedTask = std::dynamic_pointer_cast<ParallelAddTask>(pool.getCompletedTask());
    if (!completedTask) return false;
    //Last level reached, set the result
    if (completedTask->level == numLevels) {
      parallelSum = completedTask->number[0];
      break;
    }
    //If we have stored another task at the same level, create a new task
   if (storedTasks[completedTask->level]) {
      completedTask->number[1] = storedTasks[completedTask->level]->number[0];
      storedTasks[completedTask->level] = nullptr;
      pool.addTask(completedTask);
    }
    else {
      //Store the task for now
      storedTasks[completedTask->level] = completedTask;
    }
  }

  //Stop the threads
  pool.stopHandlingTasks();

  //Join the threads
  pool.join();

  //Check that no exception occured
  if (pool.getThreadException()) return false;

  //Check the sum
  if (parallelSum != ((numValues*(numValues + 1)) / 2)) return false;

  return true;
}

int main(int argc, char** argv)
{
  if (!atomicAddTest()) return 1;

  if (!parallelAddTest()) return 1;

  return 0;
}

