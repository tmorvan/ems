#pragma once

namespace ems {

  ThreadPool::ThreadPool() :
    isHandlingTasks_(false),
    stopWhenEmpty_(false),
    profile_(false)
  {  
  }

  void ThreadPool::handleTasks(int numWorkers, bool stopWhenEmpty) {
    //Make sure the previous threads are stopped and joined
    stopHandlingTasks();
    join();

    if (numWorkers <= 0) return;

    {
      //Acquire lock
      std::lock_guard<std::mutex> lock(tasksMutex_);
      stopWhenEmpty_ = stopWhenEmpty;
      isHandlingTasks_ = true;
      //Release lock
    }

    if (profile_) startTime_ = std::chrono::high_resolution_clock::now();

    //Spawn the worker threads
    workers_.clear();
    for (int i = 0; i < numWorkers; i++) {
      workers_.push_back(std::thread(&ThreadPool::workerFunc, this, i));
    }
  }

  void ThreadPool::stopHandlingTasks() {
    //Stop the threads if needed
    {
      // Acquire lock
      std::lock_guard<std::mutex> lock(tasksMutex_);
      if (isHandlingTasks_) isHandlingTasks_ = false;
      else return;
      //Release lock
    }

    //Notify all threads
    tasksCondition_.notify_all();

    //Notify threads waiting for new completed task
    completedTasksCondition_.notify_all();
  }

  void ThreadPool::join() {
    for (int i = 0; i < workers_.size(); i++) {
      if (workers_[i].joinable()) workers_[i].join();
    }
    if (profile_) endTime_ = std::chrono::high_resolution_clock::now();
  }

  void ThreadPool::addTask(std::shared_ptr<Task> task, int priority) {
    if (!task) return;

    task->handlingThreadId = -1;

    {
      //Acquire lock
      std::lock_guard<std::mutex> lock(tasksMutex_);

      //Enqueue the task
      tasks_.emplace(priority,task);

      //Release lock
    } 

    //Notify the threads that a task has been added
    tasksCondition_.notify_one();
  }

  std::shared_ptr<Task> ThreadPool::getTask(bool block, bool rethrowException) {
    //Acquire lock
    std::unique_lock<std::mutex> lock(tasksMutex_);

    while (block && tasks_.empty() && isHandlingTasks_) {
      tasksCondition_.wait(lock);
    }

    //If an exception occured in one of the thread, rethrow it or return null
    if (workerException_ != nullptr) {
      if (rethrowException) std::rethrow_exception(workerException_);
      else return nullptr;
    }

    if (tasks_.empty()) return nullptr;

    std::shared_ptr<Task> res = tasks_.top().second;
    tasks_.pop();

    return res;

    //Release lock
  }

  void ThreadPool::clearTasks() {
    //Acquire lock
    std::unique_lock<std::mutex> lock(tasksMutex_);

    //Clear the tasks
    tasks_ = {};

    // Stop the threads if stopWhenEmpty_ is true
    if (stopWhenEmpty_) {
      //Release lock
      lock.unlock();

      //Stop the threads
      stopHandlingTasks();
    }

    //Release lock
  }

  std::shared_ptr<Task> ThreadPool::getCompletedTask(bool block,bool rethrowException) {
    //Acquire lock
    std::unique_lock<std::mutex> lock(tasksMutex_);

    while (block && completedTasks_.empty() && isHandlingTasks_) {
      completedTasksCondition_.wait(lock);
    }

    //If an exception occured in one of the thread, rethrow it or return null
    if (workerException_ != nullptr) {
      if (rethrowException) std::rethrow_exception(workerException_);
      else return nullptr;
    }

    if (completedTasks_.empty()) return nullptr;

    std::shared_ptr<Task> res = completedTasks_.front();
    completedTasks_.pop_front();

    return res;

    //Release lock
  }

  void ThreadPool::clearCompletedTasks() {
    //Acquire lock
    std::lock_guard<std::mutex> lock(tasksMutex_);

    completedTasks_.clear();

    //Release lock
  }

  void ThreadPool::addTaskHandler(size_t typeHash, TaskHandler handler, int threadId) {
    //Acquire lock
    std::lock_guard<std::mutex> lock(taskHandlersMutex_);

    //Set the handler
    taskHandlers_[typeHash][threadId] = handler;

    //If this task type does not hae a default handler, set it
    auto it = taskHandlers_[typeHash].find(-1);
    if (it == taskHandlers_[typeHash].end()) taskHandlers_[typeHash][-1] = handler;

    //Release lock
  }

  template<typename TaskType>
  void ThreadPool::addTaskHandler(TaskHandler handler, int threadId) {
    addTaskHandler(typeid(TaskType).hash_code(), handler, threadId);
  }

  TaskHandler ThreadPool::getTaskHandler(size_t typeHash, int threadId) {
    //Acquire lock
    std::lock_guard<std::mutex> lock(taskHandlersMutex_);

    TaskHandler res;
    auto typeIt = taskHandlers_.find(typeHash);
    if (typeIt != taskHandlers_.end()) {
      auto threadIt = typeIt->second.find(threadId);
      if (threadIt != typeIt->second.end()) res = threadIt->second;
      else {
        threadIt = typeIt->second.find(-1);
        if (threadIt != typeIt->second.end()) res = threadIt->second;
      }
    }

    return res;

    //Release lock
  }

  template<typename TaskType>
  TaskHandler ThreadPool::getTaskHandler(int threadId) {
    return getTaskHandler(typeid(TaskType).hash_code(),threadId);
  }

  void ThreadPool::removeTaskHandler(size_t typeHash, int threadId) {
    //Acquire lock
    std::lock_guard<std::mutex> lock(taskHandlersMutex_);

    if (threadId == -1) taskHandlers_.erase(typeHash);
    else {
      auto typeIt = taskHandlers_.find(typeHash);
      if (typeIt != taskHandlers_.end()) typeIt->second.erase(threadId);
    }
    //Release lock
  }

  template<typename TaskType>
  void ThreadPool::removeTaskHandler(int threadId) {
    removeTaskHandler(typeid(TaskType).hash_code(), threadId);
  }
  

  void ThreadPool::clearTaskHandlers() {
    //Acquire lock
    std::lock_guard<std::mutex> lock(taskHandlersMutex_);

    taskHandlers_.clear();

    //Release lock
  }

  void ThreadPool::workerFunc(int threadId) {
    std::shared_ptr<Task> threadCurrentTask = nullptr;
    try {
      while (true) {
        //Get the next task
        std::shared_ptr<Task> threadCurrentTask = getTask(!stopWhenEmpty_);

        //Stop the thread if we could not get a new task
        if (!threadCurrentTask) return;
        else if (!isHandlingTasks_) {
          //Not handling tasks anymore, readd the task to the queue and exit
          addTask(threadCurrentTask);
          return;
        }

        //Get the task handler for this task
        TaskHandler handler;

        size_t typeHash = typeid(*threadCurrentTask).hash_code();
          
        handler = getTaskHandler(typeHash, threadId);

        //Execute the handler
        if (handler) {
          threadCurrentTask->handlingThreadId = threadId;

          if (profile_) threadCurrentTask->startTime = std::chrono::high_resolution_clock::now();
          handler(threadId, threadCurrentTask.get());
          if (profile_) threadCurrentTask->endTime = std::chrono::high_resolution_clock::now();
        }
        else threadCurrentTask->handlingThreadId = -1;

        //Push the task to the list of completed tasks
        {
          //Acquire lock
          std::lock_guard<std::mutex> lock(tasksMutex_);

          completedTasks_.push_back(threadCurrentTask);
          //Release lock
        }

        //Free the current task pointer
        threadCurrentTask.reset();

        //Notify threads waiting for new completed task
        completedTasksCondition_.notify_one();
      }
    }
    catch (std::exception e) {
      //If there is a current task try to readd it to the queue
      if (threadCurrentTask) addTask(threadCurrentTask);

      //Set the exception pointer
      workerException_ = std::current_exception();
      
      bool rethrowException = true;

      //Run the custom exception handler if there is one
      if (threadExceptionHandler_) {
        rethrowException = threadExceptionHandler_(threadId, workerException_);
      }

      //Stop the threads
      stopHandlingTasks();

      //Rethrow the exception
      if (rethrowException) std::rethrow_exception(workerException_);
    }
  }

} //namespace ems