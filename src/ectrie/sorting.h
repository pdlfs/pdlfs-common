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

#include <assert.h>
#include <string.h>
#include <vector>

#include "bit_access.h"

namespace pdlfs {
namespace ectrie {

class quick_sort {
 public:
  template <typename KeyArrayType>
  static void sort(KeyArrayType& arr, std::size_t off, std::size_t n) {
    assert(off + n <= arr.size());
    sort_rec(arr, off, off + n - 1);
  }

 protected:
  template <typename KeyArrayType>
  static void swap(KeyArrayType& arr, std::size_t i, std::size_t j) {
    assert(i < arr.size() && j < arr.size());

    uint8_t temp[arr.key_len()];
    memcpy(temp, arr[i], sizeof(temp));
    memcpy(arr[i], arr[j], sizeof(temp));
    memcpy(arr[j], temp, sizeof(temp));
  }

  template <typename KeyArrayType>
  static int cmp(const KeyArrayType& arr, std::size_t i, std::size_t j) {
    assert(i < arr.size() && j < arr.size());
    return memcmp(arr[i], arr[j], arr.key_len());
  }

  template <typename KeyArrayType>
  static void sort_rec(KeyArrayType& arr, std::size_t left, std::size_t right) {
    assert(left <= arr.size());
    assert(right < arr.size());

    if (left < right) {
      std::size_t p = left;
      std::size_t i = left;
      std::size_t j = right + 1;
      while (i < j) {
        while (i < right && cmp(arr, ++i, p) <= 0)
          ;
        while (left < j && cmp(arr, p, --j) <= 0)
          ;
        if (i < j) swap(arr, i, j);
      }
      if (p != j) swap(arr, p, j);

      if (j > 0)  // we are using unsigned type, so let's avoid negative numbers
        sort_rec(arr, left, j - 1);
      sort_rec(arr, j + 1, right);
    }
  }
};

}  // namespace ectrie
}  // namespace pdlfs
