/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <chrono>
#include <queue>

#include "bucket_logger.h"
#include "common.h"
#include "executorpool.h"
#include "executorthread.h"
#include "globaltask.h"
#include "taskqueue.h"

#include <platform/timeutils.h>
#include <sstream>

extern "C" {
    static void launch_executor_thread(void *arg) {
        ExecutorThread *executor = (ExecutorThread*) arg;
        executor->run();
    }
}

ExecutorThread::~ExecutorThread() {
    EP_LOG_INFO("Executor killing {}", name);
}

void ExecutorThread::start() {
    std::string thread_name("mc:" + getName());
    // Only permitted 15 characters of name; therefore abbreviate thread names.
    std::string worker("_worker");
    std::string::size_type pos = thread_name.find(worker);
    if (pos != std::string::npos) {
        thread_name.replace(pos, worker.size(), "");
    }
    thread_name.resize(15);
    if (cb_create_named_thread(&thread, launch_executor_thread, this, 0,
                               thread_name.c_str()) != 0) {
        std::stringstream ss;
        ss << name.c_str() << ": Initialization error!!!";
        throw std::runtime_error(ss.str().c_str());
    }

    EP_LOG_DEBUG("{}: Started", name);
}

void ExecutorThread::stop(bool wait) {
    if (!wait && (state == EXECUTOR_SHUTDOWN || state == EXECUTOR_DEAD)) {
        return;
    }

    state = EXECUTOR_SHUTDOWN;

    if (!wait) {
        EP_LOG_INFO("{}: Stopping", name);
        return;
    }
    cb_join_thread(thread);
    EP_LOG_INFO("{}: Stopped", name);
}

void ExecutorThread::run() {
    EP_LOG_DEBUG("Thread {} running..", getName());

    for (uint8_t tick = 1;; tick++) {
        resetCurrentTask();

        if (state != EXECUTOR_RUNNING) {
            break;
        }

        updateCurrentTime();
        if (TaskQueue *q = manager->nextTask(*this, tick)) {
            manager->startWork(taskType);
            EventuallyPersistentEngine *engine = currentTask->getEngine();

            // Not all tasks are associated with an engine, only switch
            // for those that do.
            if (engine) {
                ObjectRegistry::onSwitchThread(engine);
            }

            if (currentTask->isdead()) {
                manager->doneWork(taskType);
                // Always switch to the no-engine context when the task is dead
                // this is to guarantee that if a shutdown is occuring TLS does
                // not store an engine which is on its way down. Critically do
                // the switch before the cancel as if a shutdown is happening
                // the live engine is waiting for all tasks to cancel before
                // actually moving to delete, without this, this thread may
                // try and deference the engine in other onSwitchThread calls
                ObjectRegistry::onSwitchThread(nullptr);
                manager->cancel(currentTask->uid, true);
                continue;
            }

            // Measure scheduling overhead as difference between the time
            // that the task wanted to wake up and the current time
            const std::chrono::steady_clock::time_point woketime =
                    currentTask->getWaketime();

            auto scheduleOverhead = getCurTime() - woketime;
            // scheduleOverhead can be a negative number if the task has been
            // woken up before we expected it too be. In this case this means
            // that we have no schedule overhead and thus need to set it too 0.
            if (scheduleOverhead.count() < 0) {
                scheduleOverhead = std::chrono::steady_clock::duration::zero();
            }

            currentTask->getTaskable().logQTime(currentTask->getTaskId(),
                                                scheduleOverhead);
            // MB-25822: It could be useful to have the exact datetime of long
            // schedule times, in the same way we have for long runtimes.
            // It is more difficult to estimate the expected schedule time than
            // the runtime for a task, because the schedule times depends on
            // things "external" to the task itself (e.g., how many tasks are
            // in queue in the same priority-group).
            // Also, the schedule time depends on the runtime of the previous
            // run. That means that for Read/Write/AuxIO tasks it is even more
            // difficult to predict because that do IO.
            // So, for now we log long schedule times only for NON_IO tasks,
            // which is the task type for the ConnManager and
            // ConnNotifierCallback tasks involved in MB-25822 and that we aim
            // to debug. We consider 1 second a sensible schedule overhead
            // limit for NON_IO tasks.
            if (GlobalTask::getTaskType(currentTask->getTaskId()) ==
                        task_type_t::NONIO_TASK_IDX &&
                scheduleOverhead > std::chrono::seconds(1)) {
                auto description = currentTask->getDescription();
                EP_LOG_WARN(
                        "Slow scheduling for NON_IO task '{}' on thread {}. "
                        "Schedule overhead: {}",
                        description,
                        getName(),
                        cb::time2text(scheduleOverhead));
            }
            updateTaskStart();

            const auto curTaskDescr = currentTask->getDescription();
            EP_LOG_DEBUG("{}: Run task \"{}\" id {}",
                         getName(),
                         curTaskDescr,
                         currentTask->getId());

            // Now Run the Task ....
            currentTask->setState(TASK_RUNNING, TASK_SNOOZED);
            bool again = currentTask->run();

            // Task done, log it ...
            const std::chrono::steady_clock::duration runtime(
                    std::chrono::steady_clock::now() - getTaskStart());
            currentTask->getTaskable().logRunTime(currentTask->getTaskId(),
                                                  runtime);
            currentTask->updateRuntime(runtime);

            // Check if exceeded expected duration; and if so log.
            // Note: This is done before we call onSwitchThread(NULL)
            // so the bucket name is included in the Log message.
            if (runtime > currentTask->maxExpectedDuration()) {
                auto description = currentTask->getDescription();
                EP_LOG_WARN("Slow runtime for '{}' on thread {}: {}",
                            description,
                            getName(),
                            cb::time2text(runtime));
            }

            // See above comment, but must switch to null before cancelling.
            // However we do this here to cover both the if and else blocks
            // below, we can enter the else block with a task which is about to
            // cancel (become dead), when that happens another thread of the
            // same class may process the cancel, making the engine* vulnerable
            // to deletion.
            ObjectRegistry::onSwitchThread(nullptr);

            // Check if task is run once or needs to be rescheduled..
            if (!again || currentTask->isdead()) {
                manager->cancel(currentTask->uid, true);
            } else {
                // if a task has not set snooze, update its waketime to now
                // before rescheduling for more accurate timing histograms
                currentTask->updateWaketimeIfLessThan(getCurTime());

                // reschedule this task back into the queue it was fetched from
                const std::chrono::steady_clock::time_point new_waketime =
                        q->reschedule(currentTask);
                // record min waketime ...
                if (new_waketime < getWaketime()) {
                    setWaketime(new_waketime);
                }
                EP_LOG_DEBUG(
                        "{}: Reschedule a task"
                        " \"{}\" id {}[{} {} |{}]",
                        name,
                        curTaskDescr,
                        currentTask->getId(),
                        to_ns_since_epoch(new_waketime).count(),
                        to_ns_since_epoch(currentTask->getWaketime()).count(),
                        to_ns_since_epoch(getWaketime()).count());
            }
            manager->doneWork(taskType);
        }
    }
    // Thread is about to terminate - disassociate it from any engine.
    ObjectRegistry::onSwitchThread(nullptr);

    state = EXECUTOR_DEAD;
}

void ExecutorThread::setCurrentTask(ExTask newTask) {
    LockHolder lh(currentTaskMutex);
    currentTask = newTask;
}

// MB-24394: reset currentTask, however we will perform the actual shared_ptr
// reset without the lock. This is because the task *can* re-enter the
// executorthread/pool code from it's destructor path, specifically if the task
// owns a VBucketPtr which is marked as "deferred-delete". Doing this std::move
// and lockless reset prevents a lock inversion.
void ExecutorThread::resetCurrentTask() {
    ExTask resetThisObject;
    {
        LockHolder lh(currentTaskMutex);
        // move currentTask so we 'steal' the pointer and ensure currentTask
        // owns nothing.
        resetThisObject = std::move(currentTask);
    }
    resetThisObject.reset();
}

std::string ExecutorThread::getTaskName() {
    LockHolder lh(currentTaskMutex);
    if (currentTask) {
        return currentTask->getDescription();
    } else {
        return "Not currently running any task";
    }
}

const std::string ExecutorThread::getTaskableName() {
    LockHolder lh(currentTaskMutex);
    if (currentTask) {
        return currentTask->getTaskable().getName();
    } else {
        return std::string();
    }
}

const std::string ExecutorThread::getStateName() {
    switch (state.load()) {
    case EXECUTOR_RUNNING:
        return std::string("running");
    case EXECUTOR_WAITING:
        return std::string("waiting");
    case EXECUTOR_SLEEPING:
        return std::string("sleeping");
    case EXECUTOR_SHUTDOWN:
        return std::string("shutdown");
    default:
        return std::string("dead");
    }
}
