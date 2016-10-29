/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

// TODO: clean header files
#include "vlog_column_impl.h"

#include <pdlfs-common/coding.h>
#include <pdlfs-common/dbfiles.h>
#include <pdlfs-common/env.h>
#include <pdlfs-common/leveldb/db/dbformat.h>
#include <pdlfs-common/leveldb/iterator.h>
#include <pdlfs-common/log_writer.h>
#include <pdlfs-common/slice.h>
#include <pdlfs-common/status.h>
// #include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace pdlfs {

VLogColumnImpl::~VLogColumnImpl() {
  delete db_;
  // TODO: and ??
}

Status VLogColumnImpl::WriteTableStart() {
  // TODO
  return Status::OK();
}

Status VLogColumnImpl::WriteTableEnd() {
  // TODO
  return Status::OK();
}

Status VLogColumnImpl::BulkInsertVLog(KeyValOffVec* const kvoff_vec,
                                      const std::string& fname,
                                      Iterator* iter) {
  WritableFile* file;
  Status s = env_->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  log::Writer vlog(file);
  iter->SeekToFirst();
  if (iter->Valid()) {
    for (; iter->Valid(); iter->Next()) {
      uint64_t off = vlog.CurrentOffset();
      std::string buffer;
      PutVarint64(&buffer, iter->key().size());
      PutVarint64(&buffer, iter->value().size());
      buffer.append(iter->key().data());
      buffer.append(iter->value().data());
      s = vlog.AddRecord(buffer);
      // If error, skip this one?
      if (!s.ok()) {
        continue;
      }
      std::string encoded_pos;
      PutFixed64(&encoded_pos, off);
      PutFixed64(&encoded_pos, vlogfile_number_);

      // Keep pair<key, offest>0
      kvoff_vec->push_back(std::make_pair(iter->key(), encoded_pos));
    }
  }
  file->Close();
  delete file;
  return Status::OK();
}

Status VLogColumnImpl::WriteTable(Iterator* contents) {
  // 1. bulk insert to vlog, and store tmp files in a vector
  // Always create a new file
  const std::string fname = VLogFileName(vlogname_, vlogfile_number_);
  KeyValOffVec kvoff_vec;
  Status s = BulkInsertVLog(&kvoff_vec, fname, contents);
  if (!s.ok()) {
    return s;
  }

  // 2. bulk insert to left table.
  KeyValOffsetIterator* vec_iter = new KeyValOffsetIterator(&kvoff_vec);
  s = db_->BulkInsert(vec_iter);
  // What's typical strategy?
  if (!s.ok()) {
    return s;
  }
  // If leveldb bulkinsert fails, vlogfile_number stays the same.
  vlogfile_number_++;
  return Status::OK();
}

Iterator* VLogColumnImpl::NewInternalIterator(const ReadOptions& options) {
  // TODO

  SequenceNumber ignored_last_sequence;
  uint32_t ignored_seed;
  return db_->NewInternalIterator(options, &ignored_last_sequence,
                                  &ignored_seed);
}

Status VLogColumnImpl::Get(const ReadOptions& options, const LookupKey& lkey,
                           Buffer* result) {
  // TODO
  return db_->Get(options, lkey, result);
}

Status VLogColumnImpl::Recover() {
  Status s;
  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(vlogname_);
  // TODO: No lock file
  if (!env_->FileExists(vlogname_)) {
    if (!options_.create_if_missing) {
      return Status::InvalidArgument(vlogname_, "does not exist");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(vlogname_, "exists");
    }
  }
  return Status::OK();
}
}  // namespace pdlfs
