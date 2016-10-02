/*
 * Copyright (c) 2013 The RocksDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "index_builder.h"
#include "block_builder.h"

namespace pdlfs {

IndexBuilder::~IndexBuilder() {}

class FullkeyIndexBuilder : public IndexBuilder {
 public:
  FullkeyIndexBuilder(const Options* options)
      : index_block_builder_(options->index_block_restart_interval,
                             options->comparator),
        cmp_(options->comparator) {
    if (cmp_ == NULL) {
      cmp_ = BytewiseComparator();
    }
  }

  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block_handle) {
    if (next_key != NULL) {
      cmp_->FindShortestSeparator(last_key, *next_key);
    } else {
      cmp_->FindShortSuccessor(last_key);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key, handle_encoding);
  }

  virtual Slice Finish() { return index_block_builder_.Finish(); }

  virtual size_t CurrentSizeEstimate() const {
    return index_block_builder_.CurrentSizeEstimate();
  }

  virtual void ChangeOptions(const Options* options) {
    index_block_builder_.ChangeRestartInterval(
        options->index_block_restart_interval);
  }

  virtual void OnKeyAdded(const Slice& key) {
    // empty
  }

 private:
  BlockBuilder index_block_builder_;
  const Comparator* cmp_;
};

IndexBuilder* IndexBuilder::Create(const Options* options) {
  return new FullkeyIndexBuilder(options);
}

}  // namespace pdlfs
