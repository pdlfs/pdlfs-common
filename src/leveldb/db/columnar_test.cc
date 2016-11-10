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

#include "pdlfs-common/cache.h"
#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class TestColumnSelector : public ColumnSelector {
 public:
  virtual const char* Name() const { return "leveldb.TestColumnSelector"; }

  virtual size_t Select(const Slice& k) const { return 0; }
};

class ColumnarTest {
 typedef DBOptions Options;
 public:
  ColumnarTest() {
    dbname_ = test::TmpDir() + "/columnar_test";
    DestroyDB(ColumnName(dbname_, 0), Options());
    DestroyDB(dbname_, Options());
    options_.disable_write_ahead_log = false;
    options_.create_if_missing = true;
    options_.skip_lock_file = true;
    ColumnStyle styles[1];
    styles[0] = kLSMStyle;
    Status s =
        ColumnarDB::Open(options_, dbname_, &column_selector_, styles, 1, &db_);
    ASSERT_OK(s);
  }

  ~ColumnarTest() {
    delete db_;  //
  }

  std::string Get(const Slice& key) {
    std::string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    if (!s.ok()) {
      return "NOT_FOUND";
    } else {
      return value;
    }
  }

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  void CompactMemTable() {
    Status s =
        reinterpret_cast<ColumnarDBWrapper*>(db_)->TEST_CompactMemTable();
    ASSERT_OK(s);
  }

  std::string IterStatus(Iterator* iter) {
    std::string result;
    if (iter->Valid()) {
      result = iter->key().ToString();
      result = result + "->" + iter->value().ToString();
    } else {
      result = "(invalid)";
    }
    return result;
  }

  void Reopen(Options* options = NULL) { ASSERT_OK(TryReopen(options)); }

  Status TryReopen(Options* options) {
    delete db_;
    db_ = NULL;
    Options opts;
    if (options != NULL) {
      opts = *options;
    } else {
      opts = Options();
      opts.create_if_missing = true;
    }
    return DB::Open(opts, dbname_, &db_);
  }


  TestColumnSelector column_selector_;
  std::string dbname_;
  Options options_;
  DB* db_;
};

TEST(ColumnarTest, Empty) {
  // Empty
}

TEST(ColumnarTest, Put) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v2"));
  ASSERT_EQ(Get("foo"), "v1");
  ASSERT_EQ(Get("bar"), "v2");

  CompactMemTable();
  ASSERT_EQ(Get("foo"), "v1");
  ASSERT_EQ(Get("bar"), "v2");
}

TEST(ColumnarTest, IterMultiWithDelete) {
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Delete("b"));
  ASSERT_EQ("NOT_FOUND", Get("b"));

  CompactMemTable();
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek("c");
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  delete iter;
}

TEST(ColumnarTest, Recover) {
	ASSERT_OK(Put("foo", "v1"));
	ASSERT_OK(Put("baz", "v5"));

	Reopen();
	ASSERT_EQ("v1", Get("foo"));

	ASSERT_EQ("v1", Get("foo"));
	ASSERT_EQ("v5", Get("baz"));
	ASSERT_OK(Put("bar", "v2"));
	ASSERT_OK(Put("foo", "v3"));

	Reopen();
	ASSERT_EQ("v3", Get("foo"));
	ASSERT_OK(Put("foo", "v4"));
	ASSERT_EQ("v4", Get("foo"));
	ASSERT_EQ("v2", Get("bar"));
	ASSERT_EQ("v5", Get("baz"));
}


}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
