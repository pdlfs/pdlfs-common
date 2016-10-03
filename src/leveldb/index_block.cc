/*
 * Copyright (c) 2013 The RocksDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "index_block.h"
#include "block.h"
#include "block_builder.h"
#include "format.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/ect.h"

namespace pdlfs {

IndexBuilder::~IndexBuilder() {}

IndexReader::~IndexReader() {}

class DefaultIndexBuilder : public IndexBuilder {
 public:
  DefaultIndexBuilder(const Options* options)
      : index_block_builder_(options->index_block_restart_interval,
                             options->comparator) {}

  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block_handle) {
    const Comparator* const comparator = index_block_builder_.comparator();
    if (next_key != NULL) {
      comparator->FindShortestSeparator(last_key, *next_key);
    } else {
      comparator->FindShortSuccessor(last_key);
    }

    std::string encoding;
    block_handle.EncodeTo(&encoding);
    index_block_builder_.Add(*last_key, encoding);
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
};

class DefaultIndexReader : public IndexReader {
 public:
  DefaultIndexReader(const BlockContents& contents, const Options* options)
      : cmp_(options->comparator), block_(contents) {}

  virtual size_t ApproximateMemoryUsage() const { return block_.size(); }

  virtual Iterator* NewIterator() { return block_.NewIterator(cmp_); }

 private:
  const Comparator* cmp_;
  Block block_;
};

class ThreeLevelCompactIndexBuilder : public IndexBuilder {
  // Indexing information for each prefix group
  struct PgInfo {
    size_t first_prefix_start;  // Start offset of the first prefix key in this
                                // prefix group
    size_t last_prefix_start;   // Start offset of the last prefix key in this
                                // prefix group
    size_t first_block;         // Rank of the first block
  };

  void Flush() {
    seen_new_block_ = false;
    // A prefix group may contain only a single prefix; in such cases,
    // we set the ending prefix to be empty, which reduces the size of the
    // on-disk index representation.
    assert(starting_prefix_.size() != 0);
    if (starting_prefix_.compare(ending_prefix_) == 0) {
      ending_prefix_.clear();
    }

    PgInfo pg;
    pg.first_prefix_start = buffer_.size();
    buffer_.append(starting_prefix_);
    starting_prefix_.clear();
    pg.last_prefix_start = buffer_.size();
    buffer_.append(ending_prefix_);
    ending_prefix_.clear();
    pg.first_block = starting_block_;
    starting_block_ = n_blocks_;
    pg_info_.push_back(pg);
  }

 public:
  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block_handle) {
    // Any block generated must be non-empty
    assert(last_prefix_.size() != 0);
    if (starting_prefix_.empty()) {
      starting_prefix_ = last_prefix_;
    }

    if (next_key != NULL) {
      std::string last_prefix =
          prefix_extractor_->Transform(*last_key, &prefix_storage_).ToString();
      std::string next_prefix =
          prefix_extractor_->Transform(*next_key, &prefix_storage_).ToString();
      assert(last_prefix == last_prefix_);

      // Force splitting the current prefix group if the last prefix
      // is going to span cross a block boundary
      if (last_prefix == next_prefix && last_prefix != starting_prefix_) {
        Flush();
      }
    } else {
      // Last block
      ending_prefix_.swap(last_prefix_);
      last_prefix_.clear();
      Flush();
    }

    seen_new_block_ = true;
    n_blocks_++;
  }

  virtual void OnKeyAdded(const Slice& key) {
    Slice prefix = prefix_extractor_->Transform(key, &prefix_storage_);
    assert(prefix.size() != 0);

    if (last_prefix_.empty()) {
      starting_prefix_ = last_prefix_ = prefix.ToString();
      starting_block_ = 0;
    } else {
      // Prefix must be pre-sorted in the raw byte order
      assert(prefix.compare(last_prefix_) >= 0);
      if (starting_prefix_.empty()) starting_prefix_ = last_prefix_;
      if (prefix.compare(last_prefix_) != 0) {
        ending_prefix_.swap(last_prefix_);
        last_prefix_ = prefix.ToString();
        if (seen_new_block_) {
          Flush();
        }
      }
    }

    n_keys_++;
  }

  virtual Slice Finish() {
    PgInfo pg;  // Add a dummy prefix group to serve as a sentinel
    pg.first_prefix_start = buffer_.size();
    pg.last_prefix_start = buffer_.size();
    pg.first_block = starting_block_;
    pg_info_.push_back(pg);
  }

 private:
  bool seen_new_block_;
  size_t starting_block_;
  std::string starting_prefix_;
  std::string ending_prefix_;
  std::string last_prefix_;
  const SliceTransform* prefix_extractor_;
  std::string prefix_storage_;
  const SliceTransform* suffix_extractor_;
  std::string suffix_storage_;
  size_t n_blocks_;
  size_t n_keys_;
  std::vector<PgInfo> pg_info_;
  std::string buffer_;
};

IndexBuilder* IndexBuilder::Create(const Options* options) {
  return new DefaultIndexBuilder(options);
}

IndexReader* IndexReader::Create(const BlockContents& contents,
                                 const Options* options) {
  return new DefaultIndexReader(contents, options);
}

}  // namespace pdlfs
