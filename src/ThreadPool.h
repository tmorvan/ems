// A pool of thread working concurrently on a task list

#pragma once

#include <unordered_map>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <functional>

namespace ems {

  //Base polymorphic base struct for tasks
  struct Task {
    Task() : handled(false) {};
    virtual ~Task() {}; // for polymorphism

    bool handled;
  };

  typedef std::function<void(int, Task *)> TaskHandler;
  typedef std::function<bool(int, std::exception_ptr)> ThreadExceptionHandler;

  class ThreadPool 
  {

  public:
    //Default constructor
    ThreadPool();

    //Start handling the tasks (spawn numWorker threads)
    //If stopWhenEmpty is true the handling will stop once the taskList is empty
    void handleTasks(int numWorkers=std::max(std::thread::hardware_concurrency(),1u), bool stopWhenEmpty=false);

    //Stop handling the tasks (stop the threads)
    void stopHandlingTasks();

    //Join the threads
    void join();

    //Enqueue a task for processing
    void addTask(std::shared_ptr<Task> task);

    //Get a task and remove it from the queue
    //If block is true and the task queue is empty the thread will block
    //until a new task is added or stopHandlingTasks is called
    //If block is false the function will return immediately
    //Returns a null pointer if the task list is empty 
    //If an exception occured in the threads, rethrows it if retrhrowException is true, otherwise returns null
    std::shared_ptr<Task> getTask(bool block = true, bool rethrowException = true);

    //Clear all tasks
    void clearTasks();

    //Get a completed task and remove it from the queue
    //If block is true and the completed task queue is empty the thread will block
    //until a new task is completed or stopHandlingTasks is called
    //If block is false the function will return immediately
    //Returns a null pointer if the task list is empty 
    //If an exception occured in the threads, rethrows it if retrhrowException is true, otherwise returns null
    std::shared_ptr<Task> getCompletedTask(bool block=true, bool rethrowException=true);

    //Clear all completed tasks
    void clearCompletedTasks();

    //Add a handler for a task type described by its type_info hashcode and a specific thread
    //If no handler exists for this task type or if threadId is -1, set this handler as default
    void addTaskHandler(size_t typeHash, TaskHandler handler, int threadId = -1);

    //Add a handler for a task type and a specific thread
    //If no handler exists for this task type or if threadId is -1, set this handler as default
    template<typename TaskType>
    void addTaskHandler(TaskHandler handler, int threadId=-1);

    //Get the handler for a task type described by its type_info hashcode and a specific thread
    //Returns the default handler if no specific handler exist for this thread
    //Return an empty handler if no handlers exist for this type
    TaskHandler getTaskHandler(size_t typeHash, int threadId = -1);

    //Get the handler for a task type and a specific thread
    //Returns the default handler if no specific handler exist for this thread
    //Return an empty handler if no handlers exist for this type
    template<typename TaskType>
    TaskHandler getTaskHandler(int threadId=-1);

    //Remove the handler for a task type described by its type_info hashcode and a specific thread
    //If threadId is -1 remove all handlers for this task
    void removeTaskHandler(size_t typeHash, int threadId = -1);

    //Remove the handler for a task type and a specific thread
    //If threadId is -1 remove all handlers for this task
    template<typename TaskType>
    void removeTaskHandler(int threadId=-1);

    //Clear the handlers
    void clearTaskHandlers();

    //Get the latest exception thorwn by the worker threads
    inline std::exception_ptr getThreadException() {
      return workerException_;
    }

    //Set a custom exception handler for the threads
    //The exception handler takes the thread id and exception pointer as parameters
    //If handler returns true, the exception is rethrown by the thread
    inline void setThreadExceptionHandler(ThreadExceptionHandler handler) {
      threadExceptionHandler_ = handler;
    }

  private:
    //Worker threads
    std::vector<std::thread> workers_;

    //Queue of tasks to be processed
    std::deque<std::shared_ptr<Task>> tasks_;

    //Queue of completed tasks
    std::deque<std::shared_ptr<Task>> completedTasks_;

    //Mutex for accessing the tasks
    std::mutex tasksMutex_;

    //Condition variable to notify threads when new tasks have been added or when task handling has been terminated
    std::condition_variable tasksCondition_;

    //Condition variable to notify threads when new task has been completed or when task handling has been terminated
    std::condition_variable completedTasksCondition_;

    //Handlers for each task type and thread
    std::unordered_map<size_t, std::unordered_map<int, TaskHandler>> taskHandlers_;

    //Mutex for accessing the handlers
    std::mutex taskHandlersMutex_;

    //Indicates whether threads should stop when the task list is empty
    bool stopWhenEmpty_;

    //Indicates when the threads are active
    std::atomic<bool> isHandlingTasks_;

    //keep track of exceptions occuring in threads
    std::atomic<std::exception_ptr> workerException_;

    //Custom exception handler for the threads
    ThreadExceptionHandler threadExceptionHandler_;

    //Function called by individual worker threads
    void workerFunc(int threadId);
  };

} //namespace ems

#include "ThreadPool-inl.h"