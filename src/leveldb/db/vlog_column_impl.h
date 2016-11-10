#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <pdlfs-common/dbfiles.h>
#include "columnar_impl.h"
#include "db_impl.h"
#include "pdlfs-common/coding.h"
#include "pdlfs-common/log_reader.h"

#include <iostream>
#define LOG(...)                                                         \
  std::cout << std::dec << __FILE__ << ":" << __LINE__ << ":" << __func__ \
            << " | " << __VA_ARGS__ << std::endl;

namespace pdlfs {

typedef std::vector<std::pair<std::string, std::string> > KeyValOffVec;

class VLogColumnIterator : public Iterator {
 public:
  VLogColumnIterator(Iterator* leveldb_iter, const std::string& vlogname,
                     Env* env)
      : leveldb_iter_(leveldb_iter),
        vlogname_(vlogname),
        value_fetched_(false),
        env_(env) {}
  ~VLogColumnIterator() { delete leveldb_iter_; }
  bool Valid() const { return leveldb_iter_->Valid(); }
  virtual void Seek(const Slice& k) {
    leveldb_iter_->Seek(k);
    value_fetched_ = false;
    value_str_.empty();
  }
  // Need optimized, such as memorized by is_first;
  virtual void SeekToFirst() {
    leveldb_iter_->SeekToFirst();
    value_fetched_ = false;
    value_str_.empty();
  }
  virtual void SeekToLast() {
    leveldb_iter_->SeekToLast();
    value_fetched_ = false;
    value_str_.empty();
  }
  virtual void Next() {
    leveldb_iter_->Next();
    value_fetched_ = false;
    value_str_.empty();
  }
  virtual void Prev() {
    leveldb_iter_->Prev();
    value_fetched_ = false;
    value_str_.empty();
  }
  virtual Slice key() const { return leveldb_iter_->key(); }
  virtual Slice value() const {
    if (value_fetched_) {
      return Slice(value_str_);
    }
    Slice position = leveldb_iter_->value();
    uint64_t vlog_num = DecodeFixed64(position.data());
    uint64_t vlog_offset = DecodeFixed64(position.data() + 8);
    const std::string vlogname = VLogFileName(vlogname_, vlog_num);

    // Read value from vlog
    SequentialFile* file;
    Status s = env_->NewSequentialFile(vlogname, &file);

    if (!s.ok()) {
      // TODO
    }

    // Create the log reader, ignore LogReporter for now.
    log::Reader reader(file, NULL, true /*checksum*/,
                       vlog_offset /*initial_offset*/);
    Slice record;
    std::string scratch;
    bool ret = reader.ReadRecord(&record, &scratch);
    if (!ret) {
      // TODO
    }
    const char* p = record.data();
    const char* limit = p + record.size();
    std::string key_str;
    Slice key(key_str);
    Slice ret_value;
    if (!(p = GetLengthPrefixedSlice(p, limit, &key))) {
      // TODO
    }
    // TODO: assert lkey == key
    if (!(p = GetLengthPrefixedSlice(p, limit, &ret_value))) {
      // TODO
    }

    value_str_ = ret_value.ToString();
    ret_value = Slice(value_str_);
    value_fetched_ = true;
    delete file;
    return ret_value;
  }

  virtual Status status() const { return Status::OK(); }

 private:
  Iterator* leveldb_iter_;
  const std::string vlogname_;
  mutable bool value_fetched_;
  Env* const env_;
  mutable std::string value_str_;

  // No copying allowed
  VLogColumnIterator(const VLogColumnIterator&);
  void operator=(const VLogColumnIterator&);
};

// Iterator for a KeyValOffVec, currently only used
// in VLogColumn::WriteTable. Some functions are not implemented
// since they will not be used.
class KeyValOffsetIterator : public Iterator {
 public:
  explicit KeyValOffsetIterator(KeyValOffVec* k_voff)
      : vec_(k_voff), iter_(k_voff->begin()) {}
  bool Valid() const { return iter_ != vec_->end(); }
  void Seek(const Slice& k) {}
  void SeekToFirst() { iter_ = vec_->begin(); }
  void SeekToLast() {}
  void Next() { ++iter_; }
  void Prev() {}
  Slice key() const { return Slice(iter_->first); }
  Slice value() const { return Slice(iter_->second); }

  Status status() const { return Status::OK(); }

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
        leveldbname_(columnname + LEVELDB_SUBDIR),
        vlogname_(columnname + VLOG_SUBDIR),
        vlogfile_number_(1) {
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

};

}  // namespace pdlfs
