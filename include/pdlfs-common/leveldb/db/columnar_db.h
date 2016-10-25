#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/db/db.h"

namespace pdlfs {

enum ColumnStyle {
  kLSMStyle,    // Both keys and values are stores as LSM
  kLSMKeyStyle  // Only keys are stores as LSM
};

class ColumnarDB : public DB {
 public:
  static Status Open(const Options& options, const std::string& dbname,
                     ColumnStyle*, size_t num_columns, DB**);

  ColumnarDB() {}
  virtual ~ColumnarDB();

  // The following operations are currently not supported
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual Status AddL0Tables(const InsertOptions&, const std::string& dir);
  virtual Status Dump(const DumpOptions&, const Range& range,
                      const std::string& dir, SequenceNumber* min_seq,
                      SequenceNumber* max_seq);
};

}  // namespace pdlfs
