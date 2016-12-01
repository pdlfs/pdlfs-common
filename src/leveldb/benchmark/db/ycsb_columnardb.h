//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#pragma once

#include <iostream>
#include <mutex>
#include <string>
#include "../../db/columnar_impl.h"
#include "../../db/db_impl.h"
#include "../core/db.h"
#include "../core/properties.h"
#include "pdlfs-common/leveldb/db/columnar_db.h"
#include "pdlfs-common/status.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

using pdlfs::Status;
using pdlfs::Slice;

namespace ycsbc {

class TestColumnSelector : public pdlfs::ColumnSelector {
 public:
  virtual const char* Name() const { return "leveldb.TestColumnSelector"; }

  virtual size_t Select(const pdlfs::Slice& k) const { return 0; }
};

static const std::string FIELDNAME = "FIELDNAME";

class YCSBColumnarDB : public DB {
 public:
  typedef pdlfs::DBOptions Options;
  ~YCSBColumnarDB() { delete db_; }

  explicit YCSBColumnarDB(pdlfs::ColumnStyle style)
      : dbname_("/m/ycsb_bench") {
    pdlfs::DestroyDB(dbname_, Options());
    options_.create_if_missing = true;
    options_.skip_lock_file = true;
    options_.compaction_pool = pdlfs::ThreadPool::NewFixed(4);
    options_.compression = pdlfs::kNoCompression;
    pdlfs::ColumnStyle styles[1];
    styles[0] = style;
    // pdlfs::kLSMStyle;
    // styles[0] = kLSMKeyStyle;
    Status s = pdlfs::ColumnarDB::Open(options_, dbname_, &column_selector_,
                                       styles, 1, &db_);
    ASSERT_OK(s);
  }

  explicit YCSBColumnarDB() : dbname_("/m/ycsb_bench") {
    pdlfs::DestroyDB(dbname_, Options());
    options_.create_if_missing = true;
    options_.skip_lock_file = true;
    options_.compaction_pool = pdlfs::ThreadPool::NewFixed(4);
    options_.compression = pdlfs::kNoCompression;
    // pdlfs::kLSMStyle;
    // styles[0] = kLSMKeyStyle;

    options_.block_size = 1 << 14;           // 16KB
    options_.table_file_size = 8 * 1048576;  // 8MBcolumn_ocolumn_optionption

    Status s = pdlfs::DB::Open(options_, dbname_, &db_);
    ASSERT_OK(s);
  }

  std::string Get(const Slice& key) {
    std::string value;
    Status s = db_->Get(pdlfs::ReadOptions(), key, &value);
    if (!s.ok()) {
      return "NOT_FOUND";
    } else {
      return value;
    }
  }

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(pdlfs::WriteOptions(), k, v);
  }

  int Read(const std::string& table, const std::string& key,
           const std::vector<std::string>* fields,
           std::vector<KVPair>& result) {
    Slice key_slice(key);
    result.clear();
    std::string value = Get(Slice(key));
    if (value == "NOT_FOUND") {
      return DB::kErrorNoData;
    } else {
      result.push_back(std::make_pair(FIELDNAME, value));
      return DB::kOK;
    }
  }

  int Scan(const std::string& table, const std::string& key, int len,
           const std::vector<std::string>* fields,
           std::vector<std::vector<KVPair>>& result) {
    pdlfs::Iterator* iter = db_->NewIterator(pdlfs::ReadOptions());
    int i = 0;
    result.clear();
    // !! Read from beginning instead of key!
    for (iter->SeekToFirst(); i < len && iter->Valid(); iter->Next(), ++i) {
      result.push_back(std::vector<KVPair>());
      result[result.size() - 1].push_back(
          std::make_pair(FIELDNAME, iter->value().ToString()));
    }
    delete iter;
    return DB::kOK;
  }

  int Update(const std::string& table, const std::string& key,
             std::vector<KVPair>& values) {
    return Put(key, values[0].second).ok() ? 0 : -1;
  }

  int Insert(const std::string& table, const std::string& key,
             std::vector<KVPair>& values) {
    return Put(key, values[0].second).ok() ? 0 : -1;
  }

  int Delete(const std::string& table, const std::string& key) {
    return db_->Delete(pdlfs::WriteOptions(), key).ok() ? 0 : -1;
  }

 private:
  pdlfs::DB* db_;
  std::string dbname_;
  Options options_;
  TestColumnSelector column_selector_;
};

}  // namspace ycsbc
