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
#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "dualdb_impl.h"

// Maybe not
#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/dbformat.h"
#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

DualDBImpl::DualDBImpl(const Options& raw_options, const std::string& superdbname)
    : env_(raw_options.env),
      options_(raw_options),
      superdbname_(superdbname) {
}

DualDBImpl::~DualDBImpl() {
  delete dualdb_[0];
  delete dualdb_[1];
}

// TODO:
// Status DestroyDB(const std::string& dbname, const DBOptions& options)


Status DualDBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  std::string left_value;
  std::string right_value;
  Status s = dualdb_[0]->Get(options, key, &left_value);
  if (!s.ok()) {
    return s;
  }
  s = dualdb_[1]->Get(options, key, &right_value);
  if (!s.ok()) {
    return s;
  }
  // TODO: combine left_value and right_value
  value->assign(left_value + right_value);
  return s;
}

Status DualDBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
    // TODO: separate values func
    // TODO: tryPut func in case of half-insert on left table
  std::string str = val.ToString();
  size_t len = str.size();
  Status s = dualdb_[0]->Put(o, key, Slice(str.substr(0, len)));
  if (!s.ok()) {
      return s;
  }
  s = dualdb_[1]->Put(o, key, Slice(str.substr(len)));
  if (!s.ok()) {
      // TODO: should undo insert on left table
      return s;
  }
  return s;
}

Status DualDBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DualDB::Delete(options, key);
}

Status DualDB::Open(const Options& options, const std::string& superdbname, DualDB** dualdbptr) {
  *dualdbptr = NULL;
  // TODO: need a macro to define the left and right dir name
  std::string leftname = superdbname + DUALDBLEFSUF; // "/db_left";
  std::string rightname = superdbname + DUALDBRIGHTSUF; // "/db_right";

  DualDBImpl* impl = new DualDBImpl(options, superdbname);
  impl->Recover();
  // TODO: more suitable status code..
  Status s = DB::Open(options, leftname, &impl->dualdb_[0]);
  if (!s.ok()) {
      delete impl->dualdb_[0];
      delete impl;
      *dualdbptr = NULL;
      return s;
  }
  Options right_uncompacted_option(options);
  right_uncompacted_option.disable_compaction = true;
  s = DB::Open(right_uncompacted_option, rightname, &impl->dualdb_[1]);
  if (!s.ok()) {
      delete impl->dualdb_[0];
      delete impl->dualdb_[1];
      delete impl;
      *dualdbptr = NULL;
      return s;
  }
  *dualdbptr = impl;
  return s;
}

Status DualDBImpl::Recover() {
  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  return env_->CreateDir(superdbname_);
}


}  // namespace pdlfs
