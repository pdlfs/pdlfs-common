#pragma once

/*
 * Copyright (c) 2013 The RocksDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "format.h"

namespace pdlfs {

// An internal interface used to build the index block for each Table file.
class IndexBuilder {
 public:
  IndexBuilder() {}
  virtual ~IndexBuilder();

  typedef DBOptions Options;

  // Create an index builder according to the specified options.
  static IndexBuilder* Create(const Options* options);

  // This method will be called whenever a new data block
  // is generated.
  // REQUIRES: Finish() has not been called.
  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block) = 0;

  // This method will be called whenever a new key/value pair
  // has been inserted into the table.
  // REQUIRES: Finish() has not been called.
  virtual void OnKeyAdded(const Slice& key) = 0;

  // Update options.
  virtual void ChangeOptions(const Options* options) = 0;

  // Returns an estimate of the current size of the index
  // block we are building.
  virtual size_t CurrentSizeEstimate() const = 0;

  // Finish building the block and return a slice that refers to the
  // block contents.
  virtual Slice Finish() = 0;
};

}  // namespace pdlfs
