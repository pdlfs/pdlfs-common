#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "columnar_impl.h"
#include "db_impl.h"

namespace pdlfs {

typedef std::vector<std::pair<Slice, std::string> > KeyValOffVec;
// TODO: borrowed from memtable.cc
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

class KeyValOffsetIterator : public Iterator {
 public:
  explicit KeyValOffsetIterator(KeyValOffVec* k_voff)
      : vec_(k_voff), iter_(k_voff->begin()) {}
  virtual bool Valid() const { return iter_ != vec_->end(); }
  virtual void Seek(const Slice& k) {}
  virtual void SeekToFirst() { iter_ = vec_->begin(); }
  virtual void SeekToLast() {}
  virtual void Next() { ++iter_; }
  virtual void Prev() {}
  virtual Slice key() const {
    // return GetLengthPrefixedSlice(iter_->first.data());
    return iter_->first;
  }
  virtual Slice value() const {
    return GetLengthPrefixedSlice(iter_->second.data());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  KeyValOffVec* vec_;
  KeyValOffVec::iterator iter_;

  // No copying allowed
  KeyValOffsetIterator(const KeyValOffsetIterator&);
  void operator=(const KeyValOffsetIterator&);
};

class VLogColumnImpl : public Column {
 public:
  explicit VLogColumnImpl(const Options& raw_options,
                          const std::string& columnname)
      : env_(raw_options.env),
        options_(raw_options),
        columnname_(columnname),
        leveldbname_(columnname + "leveldb"),
        vlogname_(columnname + "vlog"),
        vlogfile_number_(0) {
    db_ = new DBImpl(raw_options, leveldbname_);
  }
  ~VLogColumnImpl();

  Status WriteTableStart();
  Status WriteTable(Iterator* contents);
  Status WriteTableEnd();

  Iterator* NewInternalIterator(const ReadOptions& options);
  Status Get(const ReadOptions& options, const LookupKey& lkey, Buffer* result);

  Status BulkInsertVLog(KeyValOffVec* const kvoff_vec, const std::string& fname,
                        Iterator* iter);

  // TODO: What's this?
  Status PreRecover(RecoverMethod method) { return Status::OK(); }

  Status Recover();

 private:
  Env* const env_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const std::string columnname_;
  const std::string leveldbname_;
  const std::string vlogname_;
  DBImpl* db_;
  uint64_t vlogfile_number_;

  // VLogImpl* vlog_;
};

}  // namespace pdlfs
