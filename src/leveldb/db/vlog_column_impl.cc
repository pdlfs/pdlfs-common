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
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include "pdlfs-common/log_reader.h"
#include "pdlfs-common/mutexlock.h"
#include "version_edit.h"
#include "version_set.h"

namespace pdlfs {

VLogColumnImpl::~VLogColumnImpl() {
  delete db_;
  // TODO: and ??
}

<<<<<<< HEAD
=======
bool VLogColumnImpl::ShouldSlowdownWrites() {
  return db_->ShouldSlowdownWrites();
}

bool VLogColumnImpl::ShouldBlockWrites() { return db_->ShouldBlockWrites(); }

>>>>>>> htfs
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
      PutVarint32(&buffer, static_cast<uint32_t>(iter->key().size()));
      buffer.append(iter->key().data(), iter->key().size());
      PutVarint32(&buffer, static_cast<uint32_t>(iter->value().size()));
      buffer.append(iter->value().data(), iter->value().size());
      s = vlog.AddRecord(Slice(buffer));

      // If error occurs, return. Inserted entries in VLog does not
      // matter since they will be eliminated during garbage collection.
      if (!s.ok()) {
        file->Close();
        delete file;
        return Status::IOError("BulkInsertVLog error");
      }

      // Record pair<key, offset>
      std::string encoded_pos;
      PutFixed64(&encoded_pos, vlogfile_number_);
      PutFixed64(&encoded_pos, off);
      // Allocate new space by std::string to prevent from
      // iter becoming invalid.
      kvoff_vec->push_back(std::make_pair(iter->key().ToString(), encoded_pos));
    }
  }
  file->Close();
  delete file;
  return Status::OK();
}

Status VLogColumnImpl::WriteTable(Iterator* contents) {
  // Currently, always create a new file for each invocation
  // Step 1. Bulk insert to vlog, and store pos info in a vector
  const std::string fname = VLogFileName(vlogname_, vlogfile_number_);
  KeyValOffVec kvoff_vec;
  Status s = BulkInsertVLog(&kvoff_vec, fname, contents);
  if (!s.ok()) {
    return s;
  }

  // Step 2. Bulk insert pos info to leveldb
  KeyValOffsetIterator* vec_iter = new KeyValOffsetIterator(&kvoff_vec);
  s = db_->BulkInsert(vec_iter);
  delete vec_iter;
  if (!s.ok()) {
    return s;
  }
  // If leveldb bulkinsert fails, vlogfile_number stays the same.
  vlogfile_number_++;
  return Status::OK();
}

Iterator* VLogColumnImpl::NewInternalIterator(const ReadOptions& options) {
  SequenceNumber ignored_last_sequence;
  uint32_t ignored_seed;
  Iterator* leveldb_iter =
      db_->NewInternalIterator(options, &ignored_last_sequence, &ignored_seed);
  return new VLogColumnIterator(leveldb_iter, vlogname_, env_);
}

Status VLogColumnImpl::Get(const ReadOptions& options, const LookupKey& lkey,
                           Buffer* result) {
  // Get position from leveldb
  std::string pos;
  buffer::StringBuf buf(&pos);
  Status s = db_->Get(options, lkey, &buf);
  if (!s.ok()) {
    return s;
  }

  uint64_t vlog_num = DecodeFixed64(pos.data());
  uint64_t vlog_offset = DecodeFixed64(pos.data() + 8);
  const std::string vlogname = VLogFileName(vlogname_, vlog_num);

  // Read value from vlog
  SequentialFile* file;
  s = env_->NewSequentialFile(vlogname, &file);
  if (!s.ok()) {
    return s;
  }

  // Create the log reader, ignore LogReporter for now.
  log::Reader reader(file, NULL, true /*checksum*/,
                     vlog_offset /*initial_offset*/);
  Slice record;
  std::string scratch;
  if (reader.ReadRecord(&record, &scratch)) {
    const char* p = record.data();
    const char* limit = p + record.size();
    std::string key_str;
    std::string value_str;
    Slice key(key_str);
    Slice value(value_str);
    if (!(p = GetLengthPrefixedSlice(p, limit, &key))) {
      delete file;
      return Status::Corruption(columnname_,
                                "VLog ReadRecord record corrupted");
    }
    // TODO: assert lkey == key
    if (!(p = GetLengthPrefixedSlice(p, limit, &value))) {
      delete file;
      return Status::Corruption(columnname_,
                                "VLog ReadRecord record corrupted");
    }
    result->Fill(value.data(), value.size());
  } else {
    delete file;
    return Status::IOError(columnname_, "VLog ReadRecord failed");
  }
  delete file;
  return s;
}

Status VLogColumnImpl::Recover() {
  Status s;

  // TODO: Add lock file
  if (!env_->FileExists(vlogname_)) {
    if (!options_.create_if_missing) {
      return Status::InvalidArgument(vlogname_, "does not exist");
    } else {
      // Sematically equivalent to NewDB().
      // Ignore error from CreateDir since the creation of the DB is
      // committed only when the descriptor is created, and this directory
      // may already exist from a previous failed creation attempt.
      env_->CreateDir(vlogname_);
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(vlogname_, "exists");
    }
  }

  // Recover from leveldb
  VersionEdit edit;
  MutexLock l(&db_->mutex_);

  s = db_->Recover(&edit);  // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    s = db_->versions_->LogAndApply(&edit, &db_->mutex_);
  }
  if (s.ok()) {
    db_->DeleteObsoleteFiles();
    db_->MaybeScheduleCompaction();
  }

  return s;
}
}  // namespace pdlfs
