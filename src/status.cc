#include "pdlfs-common/status.h"

/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <string.h>

namespace pdlfs {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

namespace {
/* clang-format off */
#define ERR_NUM(Err) static_cast<int>(Status::k##Err)
static const char* kCodeString[] = {
  [ERR_NUM(Ok)] = "OK",                                       /* 0 */
  [ERR_NUM(NotFound)] = "Not found",                          /* 1 */
  [ERR_NUM(AlreadyExists)] = "Already exists",                /* 2 */
  [ERR_NUM(Corruption)] = "Corruption",                       /* 3 */
  [ERR_NUM(NotSupported)] = "Not implemented",                /* 4 */
  [ERR_NUM(InvalidArgument)] = "Invalid argument",            /* 5 */
  [ERR_NUM(IOError)] = "IO error",                            /* 6 */
  [ERR_NUM(BufferFull)] = "Buffer full",                      /* 7 */
  [ERR_NUM(ReadOnly)] = "Read only",                          /* 8 */
  [ERR_NUM(WriteOnly)] = "Write only",                        /* 9 */
  [ERR_NUM(DeadLocked)] = "Dead locked",                      /* 10 */
  [ERR_NUM(OptimisticLockFailed)] = "Optimistic lock failed", /* 11 */
  [ERR_NUM(TryAgain)] = "Try again",                          /* 12 */
  [ERR_NUM(Disconnected)] = "Disconnected",                   /* 13 */
  [ERR_NUM(AssertionFailed)] = "Assertion failed",            /* 14 */
  [ERR_NUM(AccessDenied)] = "Permission denied",              /* 15 */
  [ERR_NUM(DirExpected)] = "Dir expected",                    /* 16 */
  [ERR_NUM(FileExpected)] = "File expected",                  /* 17 */
  [ERR_NUM(DirNotEmpty)] = "Dir not empty",                   /* 18 */
  [ERR_NUM(DirNotAllocated)] = "Dir not allocated",           /* 19 */
  [ERR_NUM(DirDisabled)] = "Dir disabled",                    /* 20 */
  [ERR_NUM(DirMarkedDeleted)] = "Dir marked deleted",         /* 21 */
};
#undef ERR_NUM
/* clang-format on */
}

std::string Status::ToString() const {
  if (state_ == NULL) {
    return "OK";
  } else {
    const char* type = kCodeString[err_code()];
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    if (length == 0) {
      return type;
    } else {
      std::string result(type);
      result.append(" :");
      result.append(state_ + 5, length);
      return result;
    }
  }
}

}  // namespace pdlfs
