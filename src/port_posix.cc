/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/port_posix.h"
#include "pdlfs-common/pdlfs_platform.h"

namespace pdlfs {
namespace port {

Mutex::Mutex() {
  PthreadCall("pthread_mutex_init", pthread_mutex_init(&mu_, NULL));
}

Mutex::~Mutex() {
  PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mu_));
}

void Mutex::Lock() {
  PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mu_));
}

void Mutex::Unlock() {
  PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mu_));
}

CondVar::CondVar(Mutex* mu) : mu_(mu) {
  PthreadCall("pthread_cond_init", pthread_cond_init(&cv_, NULL));
}

CondVar::~CondVar() {
  PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&cv_));
}

void CondVar::Wait() {
  PthreadCall("pthread_cond_wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

void CondVar::TimedWait(uint64_t micro) {
  struct timespec abstime;
  clock_gettime(CLOCK_REALTIME_COARSE, &abstime);
  uint64_t nano = abstime.tv_nsec;
  nano += 1000 * micro;
  abstime.tv_nsec = nano % 1000000000LLU;
  abstime.tv_sec += nano / 1000000000LLU;
  PthreadCall("pthread_cond_timedwait",
              pthread_cond_timedwait(&cv_, &mu_->mu_, &abstime));
}

void CondVar::Signal() {
  PthreadCall("pthread_cond_signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
  PthreadCall("pthread_cond_broadcast", pthread_cond_broadcast(&cv_));
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("pthread_once", pthread_once(once, initializer));
}

uint64_t PthreadId() {
  pthread_t tid = pthread_self();
  uint64_t thread_id = 0;
  memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
  return thread_id;
}

}  // namespace port
}  // namespace pdlfs
