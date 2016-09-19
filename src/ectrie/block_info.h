#pragma once

/*
 * Copyright (c) 2015 The SILT Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stddef.h>
#include <stdint.h>

namespace pdlfs {

template <size_t n>
struct choose_initial_n {
  static const bool c = (size_t(1) << n << n) != 0;
  static const size_t value = !c * n + choose_initial_n<2 * c * n>::value;
};

template <>
struct choose_initial_n<0> {
  static const size_t value = 0;
};
const size_t initial_n = choose_initial_n<16>::value;

template <size_t x, size_t n = initial_n>
struct static_log2 {
  static const bool c = (x >> n) > 0;
  static const size_t value = c * n + (static_log2<(x >> c * n), n / 2>::value);
};

template <>
struct static_log2<1, 0> {
  static const size_t value = 0;
};

namespace ectrie {

template <typename BlockType>
class block_info {
 public:
  typedef BlockType block_type;

  static const size_t bytes_per_block = sizeof(block_type);
  static const size_t bits_per_block = bytes_per_block * 8;
  static const size_t bit_mask = bits_per_block - 1;
  static const size_t log_bits_per_block = static_log2<bits_per_block>::value;

  static size_t block_count(size_t n) {
    return (n + bits_per_block - 1) / bits_per_block;
  }

  static size_t size(size_t n) { return block_count(n) * bytes_per_block; }
};

}  // namespace ectrie
}  // namespace pdlfs
