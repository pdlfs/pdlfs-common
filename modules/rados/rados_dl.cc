/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/logging.h"
#include "pdlfs-common/map.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include "rados_api.h"
#include "rados_dl.h"

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(RADOS)
typedef pdlfs::rados::RadosOptions options_t;
typedef pdlfs::rados::RadosConn conn_t;
typedef pdlfs::port::Mutex mutex_t;

static pdlfs::HashMap<conn_t> conn_table;
static mutex_t mutex;
static conn_t* OpenRadosConn(const char* conf_str) {
  pdlfs::MutexLock ml(&mutex);
  conn_t* conn = conn_table.Lookup(".");
  if (conn == NULL) {
    conn = new conn_t;
    options_t options;
    pdlfs::Status s = conn->Open(options);
    if (!s.ok()) {
      pdlfs::Error(__LOG_ARGS__, "Fail to open connection to rados: %s",
                   s.ToString().c_str());
      delete conn;
      return NULL;
    } else {
      conn_table.Insert(".", conn);
      return conn;
    }
  } else {
    return conn;
  }
}
#endif

void* pdlfs_load_rados_env(const char* conf_str) {
#if defined(RADOS)
  pdlfs::Env* env = NULL;
  conn_t* conn = OpenRadosConn(conf_str);
  if (conn != NULL) {
    pdlfs::Status s = conn->OpenEnv(&env);
    if (!s.ok()) {
      pdlfs::Error(__LOG_ARGS__, "Fail to open rados env: %s",
                   s.ToString().c_str());
      env = NULL;
    }
  }

  return env;
#else
  return NULL;
#endif
}

void* pdlfs_load_rados_fio(const char* conf_str) {
#if defined(RADOS)
  pdlfs::Fio* fio = NULL;
  conn_t* conn = OpenRadosConn(conf_str);
  if (conn != NULL) {
    pdlfs::Status s = conn->OpenFio(&fio);
    if (!s.ok()) {
      pdlfs::Error(__LOG_ARGS__, "Fail to open rados fio: %s",
                   s.ToString().c_str());
      fio = NULL;
    }
  }

  return fio;
#else
  return NULL;
#endif
}

#if defined(__cplusplus)
}
#endif
