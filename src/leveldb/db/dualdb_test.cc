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

#include <random>

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

    DBOptions options;
    options.is_dualdb = true;
    DestroyDB(dualdbname_, options);
    // DestroyDB(dualdbname_ + "/db_left", Options());
    // DestroyDB(dualdbname_ + "/db_right", Options());
    dualdb_ = NULL;
    Reopen();
  }

  ~DualDBTest() {
    delete dualdb_;
    DBOptions options;
    options.is_dualdb = true;
    DestroyDB(dualdbname_, options);
    // DestroyDB(dualdbname_ + "/db_left", Options());
    // DestroyDB(dualdbname_ + "/db_right", Options());
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

  Status Delete(const std::string& k) { return dualdb_->Delete(WriteOptions(), k); }

  void DestroyAndReopen(Options* options = NULL) {
    delete dualdb_;
    dualdb_ = NULL;

    DBOptions del_options;
    del_options.is_dualdb = true;
    DestroyDB(dualdbname_, del_options);

    // DestroyDB(dualdbname_ + "/db_left", Options());
    // DestroyDB(dualdbname_ + "/db_right", Options());
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

TEST(DualDBTest, PutDeleteGet) {
  do {
    ASSERT_OK(dualdb_->Put(WriteOptions(), "foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_OK(dualdb_->Put(WriteOptions(), "foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    ASSERT_OK(dualdb_->Delete(WriteOptions(), "foo"));
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

}  // namespace pdlfs


namespace {

constexpr int ascii_max = 255;

std::string MakeVariableLenString(::pdlfs::Random* rnd, uint32_t min_len, uint32_t max_len) {
  uint32_t len = rnd->Uniform(max_len - min_len + 1) + min_len;
  char* chars = new char[len+1];
  for (uint32_t i = 0; i < len; ++i) {
    chars[i] = rnd->Uniform(ascii_max) + 1;
  }
  chars[len] = 0;
  std::string res(chars);
  delete chars;
  return res;
}

std::string MakeFixedLenString(::pdlfs::Random* rnd, uint32_t len) {
  char* chars = new char[len+1];
  for (uint32_t i = 0; i < len; ++i) {
    chars[i] = rnd->Uniform(ascii_max) + 1;
  }
  chars[len] = 0;
  std::string res(chars);
  delete chars;
  return res;
}


void BM_DualDBPut(int iters) {
  typedef ::pdlfs::DBOptions Options;
  std::string dbname = ::pdlfs::test::TmpDir() + "/leveldb_test_benchmark";
  DestroyDB(dbname, Options());

  ::pdlfs::DualDB* dualdb = NULL;
  Options opts;
  opts.create_if_missing = true;
  ::pdlfs::Status s = ::pdlfs::DualDB::Open(opts, dbname, &dualdb);
  ASSERT_OK(s);
  ASSERT_TRUE(dualdb != NULL);

  uint32_t key_len = 16u;
  uint32_t value_min_len = 256u;
  uint32_t value_max_len = 4096u;
  ::pdlfs::Random rnd(::pdlfs::test::RandomSeed());
  ::pdlfs::WriteOptions write_opt;

  ::pdlfs::Env* env = ::pdlfs::Env::Default();
  uint64_t start_micros = env->NowMicros();

  for (int i = 0; i < iters; i++) {
    dualdb->Put(write_opt, MakeFixedLenString(&rnd, key_len), MakeVariableLenString(&rnd, value_min_len, value_max_len));
  }
  uint64_t stop_micros = env->NowMicros();
  unsigned int us = stop_micros - start_micros;
  fprintf(stderr,
          "BM_DualDBPut/%8d iters : %9u us (%7.0f us / iter)\n",
          iters, us, ((float)us) / iters);

  delete dualdb;
  dualdb = NULL;
}
}

int main(int argc, char** argv) {
  if (argc > 1 && std::string(argv[1]) == "--benchmark") {
	BM_DualDBPut(1000);
	return 0;
  }

  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
