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
#include "pdlfs-common/coding.h"
#include <pdlfs-common/dbfiles.h>
#include "pdlfs-common/log_reader.h"

#include <iostream>
#define LOG(...) std::cout << std::dec << __FILE__ << ":" << __LINE__ << \
                   ":" << __func__ << " | " << __VA_ARGS__ << std::endl;

namespace pdlfs {

typedef std::vector<std::pair<Slice, std::string> > KeyValOffVec;


class VLogColumnIterator : public Iterator {
 public:
  VLogColumnIterator(Iterator* leveldb_iter, const std::string& vlogname, Env* env) :
  	leveldb_iter_(leveldb_iter), vlogname_(vlogname), value_fetched_(false), env_(env) {
  }
  ~VLogColumnIterator() {
  	delete leveldb_iter_;
  }
  bool Valid() const {
  	return leveldb_iter_->Valid();
  }
  virtual void Seek(const Slice& k) { leveldb_iter_->Seek(k); value_fetched_ = false; value_str_.empty();}
  // Need optimized, such as memorized by is_first;
  virtual void SeekToFirst() {
  	leveldb_iter_->SeekToFirst();
  	value_fetched_ = false;
  	value_str_.empty();
  }
  virtual void SeekToLast() { leveldb_iter_->SeekToLast(); value_fetched_ = false; value_str_.empty(); }
  virtual void Next() { leveldb_iter_->Next(); value_fetched_ = false; value_str_.empty();}
  virtual void Prev() { leveldb_iter_->Prev(); value_fetched_ = false; value_str_.empty();}
  virtual Slice key() const { return leveldb_iter_->key(); }
  virtual Slice value() const {
  	if (value_fetched_) {
  		return value_;
  	}
    Slice value = leveldb_iter_->value();

		uint64_t vlog_num = DecodeFixed64(value.data());
		uint64_t vlog_offset = DecodeFixed64(value.data() + 8);
		const std::string vlogname = VLogFileName(vlogname_, vlog_num);

		// Read value from vlog
		SequentialFile* file;
		Status s = env_->NewSequentialFile(vlogname, &file);

		if (!s.ok()) {
			// ???
		}

		// Create the log reader, ignore LogReporter for now.
		log::Reader reader(file, NULL, true /*checksum*/,
											 vlog_offset /*initial_offset*/);
		Slice record;
		std::string scratch;
		//if (reader.ReadRecord(&record, &scratch)) {
			reader.ReadRecord(&record, &scratch);
			const char* p = record.data();
			const char* limit = p + 5;  // VarInt32 takes no more than 5 bytes
			std::string key_str;
			Slice key(key_str);
			value_ = Slice(value_str_);
			if (!(p = GetLengthPrefixedSliceLite(p, limit, &key))) {
				//??
			}
			// TODO: assert lkey == key
			limit = p + 5;
			if (!(p = GetLengthPrefixedSliceLite(p, limit, &value_))) {
				//???
			}
			value_fetched_ = true;
			delete file;
			return value_;
		//}

  }

  virtual Status status() const { return Status::OK(); }

 private:
  Iterator* leveldb_iter_;
  const std::string vlogname_;
  mutable bool value_fetched_;
  Env* const env_;
  mutable std::string value_str_;
  mutable Slice value_;

  // No copying allowed
  VLogColumnIterator(const VLogColumnIterator&);
  void operator=(const VLogColumnIterator&);

};

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
  Slice key() const {
    // return GetLengthPrefixedSlice(iter_->first.data());ter->value().data(
    return iter_->first;
  }
  Slice value() const { return Slice(iter_->second);}

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




