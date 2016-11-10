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

class TestVLogColumnSelector : public ColumnSelector {
 public:
  virtual const char* Name() const { return "leveldb.TestVLogColumnSelector"; }

  virtual size_t Select(const Slice& k) const { return 0; }
};

class VLogColumnTest {
 public:
 typedef DBOptions Options;
  VLogColumnTest() {
    dbname_ = test::TmpDir() + "/vlogcolumn_test";
    DestroyDB(dbname_, Options());
    options_.create_if_missing = true;
    options_.skip_lock_file = true;
    ColumnStyle styles[1];
    styles[0] = kLSMKeyStyle;
    Status s =
        ColumnarDB::Open(options_, dbname_, &column_selector_, styles, 1, &db_);
    ASSERT_OK(s);
  }

  ~VLogColumnTest() { delete db_; }

  std::string Get(const Slice& key) {
    std::string value;
    Status s = db_->Get(ReadOptions(), key, &value);
    if (!s.ok()) {
      return "NOT_FOUND";
    } else {
      return value;
    }
  }

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

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
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

  int NumTableFilesAtLevel(int level) {
    std::string property;
    ASSERT_TRUE(db_->GetProperty(
        "leveldb.num-files-at-level" + NumberToString(level), &property));
    return atoi(property.c_str());
  }

  TestVLogColumnSelector column_selector_;
  std::string dbname_;
  Options options_;
  DB* db_;
};

TEST(VLogColumnTest, Empty) {
  // Empty
}

TEST(VLogColumnTest, Put) {
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), "bar", "v2"));
  ASSERT_EQ(Get("foo"), "v1");
  ASSERT_EQ(Get("bar"), "v2");
  ASSERT_OK(Put("a", "va"));
  CompactMemTable();

  ASSERT_EQ(Get("foo"), "v1");
  ASSERT_EQ(Get("bar"), "v2");
  ASSERT_EQ(Get("a"), "va");
}

TEST(VLogColumnTest, Iterator) {
  Iterator* iter = db_->NewIterator(ReadOptions());

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("foo");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST(VLogColumnTest, IterSingle) {
  ASSERT_OK(Put("a", "va"));
  Iterator* iter = db_->NewIterator(ReadOptions());

  CompactMemTable();
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("a");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("b");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST(VLogColumnTest, IterMulti) {
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  Iterator* iter = db_->NewIterator(ReadOptions());

  CompactMemTable();
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("a");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("ax");
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Seek("b");
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Seek("z");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  // Switch from reverse to forward
  iter->SeekToLast();
  iter->Prev();
  iter->Prev();
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");

  // Switch from forward to reverse
  iter->SeekToFirst();
  iter->Next();
  iter->Next();
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->vb");

  // Make sure iter stays at snapshot
  ASSERT_OK(Put("a", "va2"));
  ASSERT_OK(Put("a2", "va3"));
  ASSERT_OK(Put("b", "vb2"));
  ASSERT_OK(Put("c", "vc2"));
  ASSERT_OK(Delete("b"));

  CompactMemTable();

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST(VLogColumnTest, IterSmallAndLargeMix) {
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", std::string(100000, 'b')));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put("d", std::string(100000, 'd')));
  ASSERT_OK(Put("e", std::string(100000, 'e')));

  Iterator* iter = db_->NewIterator(ReadOptions());

  CompactMemTable();

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST(VLogColumnTest, IterMultiWithDelete) {
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

TEST(VLogColumnTest, MultiCompaction) {
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  Iterator* iter = db_->NewIterator(ReadOptions());

  CompactMemTable();
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");

  ASSERT_OK(Put("c", "vc"));

  CompactMemTable();
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;

  ASSERT_OK(Delete("b"));
  CompactMemTable();
  iter = db_->NewIterator(ReadOptions());
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");

  delete iter;
}

TEST(VLogColumnTest, Recover) {
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


TEST(VLogColumnTest, RecoveryWithEmptyLog) {
	ASSERT_OK(Put("foo", "v1"));
	ASSERT_OK(Put("foo", "v2"));
	Reopen();
	Reopen();
	ASSERT_OK(Put("foo", "v3"));
	Reopen();
	ASSERT_EQ("v3", Get("foo"));
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST(VLogColumnTest, RecoverDuringMemtableCompaction) {
    Options options = options_;
    options.write_buffer_size = 1000000;
    Reopen(&options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_OK(Put("foo", "v1"));                         // Goes to 1st log file
    ASSERT_OK(Put("big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_OK(Put("big2", std::string(1000, 'y')));      // Triggers compaction
    ASSERT_OK(Put("bar", "v2"));                         // Goes to new log file

    Reopen(&options);
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get("big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get("big2"));
}

TEST(VLogColumnTest, RecoverWithLargeLog) {
  {
    Reopen(&options_);
    ASSERT_OK(Put("big1", std::string(200000, '1')));
    ASSERT_OK(Put("big2", std::string(200000, '2')));
    ASSERT_OK(Put("small3", std::string(10, '3')));
    ASSERT_OK(Put("small4", std::string(10, '4')));
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  }

  // Make sure that if we re-open with a small write buffer size that
  // we flush table files in the middle of a large log file.
  Options options = options_;
  options.write_buffer_size = 100000;
  Reopen(&options);
  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  ASSERT_EQ(std::string(200000, '1'), Get("big1"));
  ASSERT_EQ(std::string(200000, '2'), Get("big2"));
  ASSERT_EQ(std::string(10, '3'), Get("small3"));
  ASSERT_EQ(std::string(10, '4'), Get("small4"));
  ASSERT_GT(NumTableFilesAtLevel(0), 1);
}

TEST(VLogColumnTest, NoLog) {
  Options options = options_;
  options.disable_write_ahead_log = true;
  Reopen(&options);

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("baz", "v5"));

  Reopen(&options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("NOT_FOUND", Get("baz"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));

  Reopen();
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_OK(Put("foo", "v4"));
  ASSERT_EQ("v4", Get("foo"));
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ("NOT_FOUND", Get("baz"));
}

/*
TEST(VLogColumnTest, Destroy) { DestroyDB(dbname_, Options()); }
*/

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
