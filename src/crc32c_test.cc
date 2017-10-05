/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/crc32c.h"
#include "pdlfs-common/testharness.h"

#include "crc32c_internal.h"

namespace pdlfs {
namespace crc32c {

class CRC {
 public:
  CRC() : hw_(CanAccelerateCrc32c()) {}
  int hw_;
};

TEST(CRC, HW) {
  if (hw_) {
    fprintf(stderr, "crc32c hardware acceleration is available");
  } else {
    fprintf(stderr, "crc32c hardware acceleration is off");
  }
  fprintf(stderr, "\n");
}

TEST(CRC, StandardResults) {
  // From rfc3720 section B.4.
  char buf[32];

  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0x8a9136aa, ValueSW(buf, sizeof(buf)));
  if (hw_) ASSERT_EQ(0x8a9136aa, ValueHW(buf, sizeof(buf)));

  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ(0x62a8ab43, ValueSW(buf, sizeof(buf)));
  if (hw_) ASSERT_EQ(0x62a8ab43, ValueHW(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = static_cast<char>(i);
  }
  ASSERT_EQ(0x46dd794e, ValueSW(buf, sizeof(buf)));
  if (hw_) ASSERT_EQ(0x46dd794e, ValueHW(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = static_cast<char>(31 - i);
  }
  ASSERT_EQ(0x113fdb5c, ValueSW(buf, sizeof(buf)));
  if (hw_) ASSERT_EQ(0x113fdb5c, ValueHW(buf, sizeof(buf)));

  unsigned char data[48] = {
      0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  ASSERT_EQ(0xd9963a56, ValueSW(reinterpret_cast<char*>(data), sizeof(data)));
  if (hw_)
    ASSERT_EQ(0xd9963a56, ValueHW(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST(CRC, Values) {
  ASSERT_NE(ValueSW("a", 1), ValueSW("foo", 3));
  if (hw_) ASSERT_NE(ValueHW("a", 1), ValueHW("foo", 3));
}

TEST(CRC, Extend) {
  ASSERT_EQ(ValueSW("hello world", 11),
            ExtendSW(ValueSW("hello ", 6), "world", 5));
  if (hw_)
    ASSERT_EQ(ValueHW("hello world", 11),
              ExtendHW(ValueHW("hello ", 6), "world", 5));
}

TEST(CRC, Mask) {
  uint32_t crc = Value("foo", 3);
  ASSERT_NE(crc, Mask(crc));
  ASSERT_NE(crc, Mask(Mask(crc)));
  ASSERT_EQ(crc, Unmask(Mask(crc)));
  ASSERT_EQ(crc, Unmask(Unmask(Mask(Mask(crc)))));
}

}  // namespace crc32c
}  // namespace pdlfs

int main(int argc, char** argv) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
