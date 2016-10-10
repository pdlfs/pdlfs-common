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
  dualdb_[1]->Get(options, key, &right_value);
  // It's possible that no value is stored in the right table
  // if the whole value is short.
  /*
  if (!s.ok()) {
    return s;
  }
  */
  value->assign(left_value + right_value);
  return s;
}

Status DualDBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  // TODO: separate values func
  // TODO: tryPut func in case of half-insert on left table
  size_t len = val.size();
  size_t left_len = std::min(len, 128lu);
  Slice left_val(val);
  size_t right_len = left_len < 128lu ? 0 : len - 128lu;
  left_val.remove_suffix(right_len);

  Status s = dualdb_[0]->Put(o, key, left_val);
  if (!s.ok()) {
      return s;
  }
  if (right_len > 0) {
	  Slice right_val(val);
	  right_val.remove_prefix(left_len);
	  s = dualdb_[1]->Put(o, key, right_val);
	  if (!s.ok()) {
		  // TODO: should undo insert on left table
		  return s;
	  }
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
