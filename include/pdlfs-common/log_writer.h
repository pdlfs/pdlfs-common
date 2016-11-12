#pragma once

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stddef.h>
#include <stdint.h>

#include "pdlfs-common/log_format.h"
#include "pdlfs-common/status.h"

namespace pdlfs {

class WritableFile;

namespace log {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest, uint64_t dest_length);

  ~Writer();

  // Return the position of the writing cursor.
  uint64_t CurrentOffset() const { return offset_; }

  Status AddRecord(const Slice& slice);

  // Call Sync() on the destination file.
  Status Sync();

 private:
  WritableFile* dest_;
  int block_offset_;  // Offset in the block currently being written
  uint64_t offset_;   // Current offset in file

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // No copying allowed
  void operator=(const Writer&);
  Writer(const Writer&);
};

}  // namespace log
}  // namespace pdlfs
