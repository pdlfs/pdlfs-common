/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "dualdb_test.h"
#include "dualdb_impl.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/leveldb/db/dualdb.h"
#include "pdlfs-common/leveldb/filter_policy.h"
#include "pdlfs-common/leveldb/table.h"
#include "pdlfs-common/strutil.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class DualDBTest {
 protected:
  typedef DBOptions Options;

 private:
  const FilterPolicy* filter_policy_;

  // Sequence of option configurations to try
  enum OptionConfig { kDefault, kFilter, kUncompressed, kEnd };
  int option_config_;

 public:
  std::string dualdbname_;
  DualDB* dualdb_;


  DualDBTest() : option_config_(kDefault) {
    filter_policy_ = NewBloomFilterPolicy(10);
    dualdbname_ = test::TmpDir() + "/dualdb_test";
    DestroyDB(dualdbname_, Options());
    dualdb_ = NULL;
    Reopen();
  }

  ~DualDBTest() {
    delete dualdb_;
    DestroyDB(dualdbname_, Options());
    delete filter_policy_;
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    switch (option_config_) {
      case kFilter:
        options.filter_policy = filter_policy_;
        break;
      case kUncompressed:
        options.compression = kNoCompression;
        break;
      default:
        break;
    }
    return options;
  }

  
  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions() {
    option_config_++;
    if (option_config_ >= kEnd) {
      return false;
    } else {
      DestroyAndReopen();
      return true;
    }
  }


  void Reopen(Options* options = NULL) { ASSERT_OK(TryReopen(options)); }

  void Close() {
    delete dualdb_;
    dualdb_ = NULL;
  }

  void DestroyAndReopen(Options* options = NULL) {
    delete dualdb_;
    dualdb_ = NULL;
    DestroyDB(dualdbname_, Options());
    ASSERT_OK(TryReopen(options));
  }

  Status TryReopen(Options* options) {
    delete dualdb_;
    dualdb_ = NULL;
    Options opts;
    if (options != NULL) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }

    return DualDB::Open(opts, dualdbname_, &dualdb_);
  }

  Status Put(const std::string& k, const std::string& v) {
    return dualdb_->Put(WriteOptions(), k, v);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = NULL) {
    ReadOptions options;
    std::string result;
    Status s = dualdb_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

};

TEST(DualDBTest, Empty) {
  do {
    ASSERT_TRUE(dualdb_ != NULL);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

TEST(DualDBTest, ReadWrite) {
  do {
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(Put("bar", "v2"));
    ASSERT_OK(Put("foo", "v3"));
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
  } while (ChangeOptions());
}


}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
