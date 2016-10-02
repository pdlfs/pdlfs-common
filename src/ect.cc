/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <vector>

#include "ectrie/bit_vector.h"
#include "ectrie/trie.h"
#include "ectrie/twolevel_bucketing.h"

#include "pdlfs-common/ect.h"

namespace pdlfs {

ECT::~ECT() {}

namespace {

class ECTImpl : public ECT {
 private:
  typedef ectrie::huffman_buffer<> huffbuf_t;
  huffbuf_t huffbuf_;
  typedef ectrie::trie<> trie_t;
  trie_t trie_;
  typedef ectrie::bit_vector<> bitvec_t;
  bitvec_t bitvector_;
  const size_t key_len_;
  size_t n_;

 public:
  ECTImpl(size_t k_len) : trie_(&huffbuf_), key_len_(k_len), n_(0) {}
  virtual ~ECTImpl() {}

  virtual size_t MemUsage() const { return bitvector_.size(); }

  virtual size_t Locate(const uint8_t* key) const {
    size_t iter = 0;
    size_t rank = trie_.locate(bitvector_, iter, key, key_len_, 0, n_);
    return rank;
  }

  virtual size_t Find(const Slice& key) const {
    return Locate(reinterpret_cast<const uint8_t*>(key.data()));
  }

  virtual void InsertKeys(size_t n, const uint8_t** keys) {
    assert(n_ == 0);
    trie_.encode(bitvector_, keys, key_len_, 0, n);
    bitvector_.compact();
    n_ = n;
  }
};

}  // anonymous namespace

void ECT::InitTrie(ECT* ect, size_t n, const Slice* keys) {
  std::vector<const uint8_t*> ukeys;
  ukeys.resize(n);
  for (size_t i = 0; i < n; i++) {
    ukeys[i] = reinterpret_cast<const uint8_t*>(&keys[i][0]);
  }
  ect->InsertKeys(ukeys.size(), &ukeys[0]);
}

ECT* ECT::Default(size_t key_len, size_t n, const Slice* keys) {
  ECT* ect = new ECTImpl(key_len);
  ECT::InitTrie(ect, n, keys);
  return ect;
}

}  // namespace pdlfs
