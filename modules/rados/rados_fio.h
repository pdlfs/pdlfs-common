#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/port.h"
#if defined(RADOS)
#include "rados_api.h"
#include "rados_common.h"

namespace pdlfs {
namespace rados {
class RadosFio;  // File I/O implementation atop Rados.

struct RadosFobj {
  rados_completion_t comp;  // Async write operation completion callbacks
  RadosFio* fio;
  uint64_t mtime;  // Cached last file modification time
  uint64_t size;   // Cached file size
  uint64_t off;    // Current read/write position
  int nrefs;
  int err;
};

class RadosFio : public Fio {
 public:
  virtual ~RadosFio();
  virtual Status Creat(const Slice& fentry, Handle** fh);
  virtual Status Open(const Slice& fentry, bool create_if_missing,
                      bool truncate_if_exists, uint64_t* mtime, uint64_t* size,
                      Handle** fh);
  virtual Status GetInfo(const Slice& fentry, Handle* fh, bool* dirty,
                         uint64_t* mtime, uint64_t* size);
  virtual Status Write(const Slice& fentry, Handle* fh, const Slice& data);
  virtual Status Pwrite(const Slice& fentry, Handle* fh, const Slice& data,
                        uint64_t off);
  virtual Status Read(const Slice& fentry, Handle* fh, Slice* result,
                      uint64_t size, char* scratch);
  virtual Status Pread(const Slice& fentry, Handle* fh, Slice* result,
                       uint64_t off, uint64_t size, char* scratch);
  virtual Status Flush(const Slice& fentry, Handle* fh,
                       bool force_sync = false);
  virtual Status Close(const Slice& fentry, Handle* fh);

 private:
  void UpdateAndUnref(RadosFobj*, int err);
  static rados_completion_t New_comp(RadosFobj*);
  static void IO_safe(rados_completion_t, void*);
  void Unref(RadosFobj*);

  RadosFio() {}
  friend class RadosConn;
  port::Mutex mutex_;
  // Constant after construction
  rados_ioctx_t ioctx_;
  rados_t cluster_;
  bool sync_;
};

}  // namespace rados
}  // namespace pdlfs

#endif  // RADOS
