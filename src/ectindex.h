#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stddef.h>
#include <stdint.h>

#include "pdlfs-common/slice.h"

namespace pdlfs {

class ECT {
 public:
  static ECT* Default(size_t key_len, size_t n, const Slice* keys);

  static ECT* TwoLevel(size_t bucket_size, size_t key_len, size_t n,
                       const Slice* keys);

  // Return the internal memory usage in bytes.
  virtual size_t MemUsage() const = 0;

  // Return the rank of a given key.
  virtual size_t Find(const Slice& key) const = 0;

  virtual ~ECT();
  ECT() {}

 private:
  virtual void InsertKeys(size_t n, const uint8_t** keys) = 0;
  static void Init(ECT* ect, size_t n, const Slice* keys);

  // No copying allowed
  void operator=(const ECT&);
  ECT(const ECT&);
};

}  // namespace pdlfs
