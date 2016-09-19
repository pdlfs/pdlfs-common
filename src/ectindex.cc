/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "ectindex.h"

#include "ectrie/bit_vector.h"
#include "ectrie/trie.h"
#include "ectrie/twolevel_bucketing.h"

#include <vector>

namespace pdlfs {

ECT::~ECT() {}

namespace {

class EntropyCodedTrie : public ECT {
 private:
  typedef ectrie::bit_vector<> buffer_type;
  buffer_type bitvector_;
  typedef ectrie::trie<> trie_type;
  trie_type trie_;
  size_t key_len_;
  size_t n_;

 public:
  EntropyCodedTrie(size_t l) : key_len_(l), n_(0) {}
  virtual ~EntropyCodedTrie() {}

  virtual size_t MemUsage() const { return bitvector_.size(); }

  virtual size_t Locate(const uint8_t* key) const {
    size_t iter = 0;
    size_t rank = trie_.locate(bitvector_, iter, key, key_len_, 0, n_);
    return rank;
  }

  virtual size_t Find(const Slice& key) const {
    return Locate(reinterpret_cast<const uint8_t*>(key.data()));
  }

  size_t InsertKeys(size_t n, const uint8_t** keys) {
    if (n_ == 0) {
      trie_.encode(bitvector_, keys, key_len_, 0, n);
      bitvector_.compact();
      n_ = n;
      return n_;
    } else {
      return 0;
    }
  }
};

}  // anonymous namespace

ECT* ECT::Create(size_t key_len, size_t n, const Slice* keys) {
  EntropyCodedTrie* ect = new EntropyCodedTrie(key_len);
  std::vector<const uint8_t*> ikeys;
  ikeys.reserve(n);
  for (size_t i = 0; i < n; i++) {
    ikeys.push_back(reinterpret_cast<const uint8_t*>(keys[i].data()));
  }
  ect->InsertKeys(ikeys.size(), ikeys.data());
  return ect;
}

}  // namespace pdlfs
