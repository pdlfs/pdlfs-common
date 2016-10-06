#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <stdio.h>

#include "pdlfs-common/leveldb/db/options.h"

namespace pdlfs {

// A DualDB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
class DualDB {
 protected:
  typedef DBOptions Options;

 public:
  // Open a db instance on a named image with full access.
  // Stores a pointer to the db in *dbptr and returns OK on success.
  // Otherwise, stores NULL in *dbptr and returns a non-OK status.
  // Only a single db instance can be opened on a specified db image.
  // Caller should delete *dbptr when it is no longer needed.
  static Status Open(const Options& options, const std::string& name,
                     DualDB** dbptr);

  DualDB() {}
  virtual ~DualDB() {}

  // Set the database entry for "key" to "value".  Returns OK on success,
  // and a non-OK status on error.
  // Note: consider setting options.sync = true.
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;

  // If the database contains an entry for "key", store the
  // corresponding value in *value and return OK.
  //
  // Consider setting "options.size_limit" to force a maximum
  // on the number of bytes to be copied into *value. This
  // is useful when the caller only needs a prefix of the value.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

 private:
  // No copying allowed
  void operator=(const DualDB&);
  DualDB(const DualDB&);
 protected:
  DB* dualdb_[2];
};

}  // namespace pdlfs
