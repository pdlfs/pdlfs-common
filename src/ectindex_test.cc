/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <string>
#include <vector>

#include "pdlfs-common/random.h"
#include "pdlfs-common/slice.h"
#include "pdlfs-common/testharness.h"

#include "ectindex.h"

namespace pdlfs {

class ECTTest {};

class TrieWrapper {
 private:
  const size_t key_len_;
  std::vector<std::string> key_buffer_;
  std::vector<Slice> pending_keys_;
  ECT* ect_;

 public:
  TrieWrapper(size_t key_len) : key_len_(key_len), ect_(NULL) {}

  ~TrieWrapper() { delete ect_; }

  size_t Locate(const Slice& key) { return ect_->Find(key); }

  void Insert(const Slice& key) {
    key_buffer_.push_back(key.ToString());
    pending_keys_.push_back(key_buffer_.back());
  }

  void Flush() {
    if (ect_ != NULL) {
      delete ect_;
    }
    ect_ = ECT::Create(key_len_, pending_keys_.size(), pending_keys_.data());
    pending_keys_.clear();
    key_buffer_.clear();
  }
};

TEST(ECTTest, EmptyTrie) {
  TrieWrapper trie(1);
  trie.Flush();
  ASSERT_GE(trie.Locate("x"), 0);
  ASSERT_GE(trie.Locate("z"), 0);
}

TEST(ECTTest, Trie_1) {
  TrieWrapper trie(1);
  trie.Insert("y");
  trie.Flush();
  ASSERT_EQ(trie.Locate("x"), 0);
  ASSERT_EQ(trie.Locate("y"), 0);
  ASSERT_GE(trie.Locate("z"), 0);
}

TEST(ECTTest, Trie_3) {
  TrieWrapper trie(3);
  trie.Insert("abc");
  trie.Insert("efg");
  trie.Insert("hij");
  trie.Flush();
  ASSERT_EQ(trie.Locate("aaa"), 0);
  ASSERT_EQ(trie.Locate("abc"), 0);
  ASSERT_EQ(trie.Locate("azz"), 0);
  ASSERT_EQ(trie.Locate("eaa"), 1);
  ASSERT_EQ(trie.Locate("efg"), 1);
  ASSERT_EQ(trie.Locate("ezz"), 1);
  ASSERT_EQ(trie.Locate("haa"), 2);
  ASSERT_EQ(trie.Locate("hij"), 2);
  ASSERT_GE(trie.Locate("hzz"), 2);
  ASSERT_GE(trie.Locate("zzz"), 2);
}

TEST(ECTTest, Trie_5) {
  TrieWrapper trie(5);
  trie.Insert("fghdc");
  trie.Insert("fzhdc");
  trie.Insert("zdfgr");
  trie.Insert("zzfgr");
  trie.Insert("zzzgr");
  trie.Flush();
  ASSERT_EQ(trie.Locate("fffff"), 0);
  ASSERT_EQ(trie.Locate("fghdc"), 0);
  ASSERT_EQ(trie.Locate("fgxxx"), 0);
  ASSERT_EQ(trie.Locate("fzbbb"), 1);
  ASSERT_EQ(trie.Locate("fzhdc"), 1);
  ASSERT_EQ(trie.Locate("fzyyy"), 1);
  ASSERT_EQ(trie.Locate("zabcd"), 2);
  ASSERT_EQ(trie.Locate("zdfgr"), 2);
  ASSERT_GE(trie.Locate("zezzz"), 2);
  ASSERT_LE(trie.Locate("zezzz"), 3);
  ASSERT_GE(trie.Locate("zfzzz"), 2);
  ASSERT_LE(trie.Locate("zfzzz"), 3);
  ASSERT_GE(trie.Locate("zyzzz"), 2);
  ASSERT_LE(trie.Locate("zyzzz"), 3);
  ASSERT_EQ(trie.Locate("zzfgr"), 3);
  ASSERT_EQ(trie.Locate("zzzgr"), 4);
  ASSERT_GE(trie.Locate("zzzzz"), 4);
}

TEST(ECTTest, Trie_8) {
  TrieWrapper trie(8);
  trie.Insert("12345678");
  trie.Insert("23456789");
  trie.Insert("34567891");
  trie.Insert("45678912");
  trie.Insert("56789123");
  trie.Insert("67891234");
  trie.Insert("78912345");
  trie.Insert("89123456");
  trie.Flush();
  ASSERT_EQ(trie.Locate("11111111"), 0);
  ASSERT_EQ(trie.Locate("12345678"), 0);
  ASSERT_EQ(trie.Locate("23456789"), 1);
  ASSERT_EQ(trie.Locate("34567891"), 2);
  ASSERT_EQ(trie.Locate("45678912"), 3);
  ASSERT_EQ(trie.Locate("56789123"), 4);
  ASSERT_EQ(trie.Locate("67891234"), 5);
  ASSERT_EQ(trie.Locate("78912345"), 6);
  ASSERT_EQ(trie.Locate("89123456"), 7);
  ASSERT_GE(trie.Locate("99999999"), 7);
}

}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
