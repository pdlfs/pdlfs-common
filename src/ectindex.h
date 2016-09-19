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
  ECT() {}

  static ECT* Create(size_t key_len, size_t n, const Slice* keys);

  // Destroy all internal index structures.
  virtual ~ECT();

  // Return the internal memory usage in bytes.
  virtual size_t MemUsage() const = 0;

  // Return the rank of a given key.
  virtual size_t Find(const Slice& key) const = 0;

 private:
  // No copying allowed
  void operator=(const ECT&);
  ECT(const ECT&);
};

}  // namespace pdlfs
