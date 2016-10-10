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

#include <deque>
#include <set>

#include "options_internal.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/dualdb.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/log_writer.h"

namespace pdlfs {

class DualDBImpl : public DualDB {
 public:
  DualDBImpl(const Options& options, const std::string& dbname);
  virtual ~DualDBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Get(const ReadOptions&, const Slice& key, std::string* value);
  virtual Status Delete(const WriteOptions& options, const Slice& key);

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover();

  // Extra methods (for testing) that are not in the public DB interface

 private:
  friend class DualDB;
  Env* const env_;
  const Options options_;
  const std::string superdbname_;



  // No copying allowed
  void operator=(const DualDBImpl&);
  DualDBImpl(const DualDBImpl&);

};

}  // namespace pdlfs
