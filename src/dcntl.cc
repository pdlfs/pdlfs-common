/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/dcntl.h"
#include "pdlfs-common/coding.h"

namespace pdlfs {

bool DirInfo::DecodeFrom(Slice* input) {
  uint64_t tmp64;
  uint32_t tmp32;
  if (!GetVarint64(input, &tmp64) || !GetVarint32(input, &tmp32)) {
    return false;
  } else {
    mtime = tmp64;
    size = tmp32;
    assert(size >= 0);
    return true;
  }
}

Slice DirInfo::EncodeTo(char* scratch) const {
  char* p = scratch;
  p = EncodeVarint64(p, mtime);
  assert(size >= 0);
  p = EncodeVarint32(p, size);
  return Slice(scratch, p - scratch);
}

}  // namespace pdlfs
