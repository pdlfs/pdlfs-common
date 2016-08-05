#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/coding.h"
#include "pdlfs-common/logging.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include "rados_api.h"

#if defined(RADOS)
#include <rados/librados.h>

namespace pdlfs {
namespace rados {

// Async I/O operation context.
class RadosOpCtx {
 public:
  explicit RadosOpCtx(port::Mutex* mu)
      : cp_(NULL), mu_(mu), nrefs_(1), err_(0) {
    rados_aio_create_completion(this, NULL, IO_safe, &cp_);
  }

  bool ok() { return err_ == 0; }

  int err() { return err_; }

  rados_completion_t comp() { return cp_; }

  void Ref() { nrefs_++; }

  void Unref() {
    assert(nrefs_ > 0);
    nrefs_--;
    if (nrefs_ == 0) {
      delete this;
    }
  }

 private:
  ~RadosOpCtx() {
    if (cp_ != NULL) {
      rados_aio_release(cp_);
    }
  }

  static void IO_safe(rados_completion_t comp, void* arg) {
    RadosOpCtx* ctx = reinterpret_cast<RadosOpCtx*>(arg);
    if (ctx != NULL) {
      MutexLock ml(ctx->mu_);
      int err = rados_aio_get_return_value(comp);
      if (ctx->err_ == 0 && err != 0) {
        ctx->err_ = err;
      }
      ctx->Unref();
    }
  }

  // No copying allowed
  void operator=(const RadosOpCtx&);
  RadosOpCtx(const RadosOpCtx&);
  rados_completion_t cp_;
  port::Mutex* mu_;
  int nrefs_;
  int err_;
};

inline Status RadosError(const char* err_ctx, int err_num) {
  char tmp[50];
  if (err_num == -ENOENT) {
    return Status::NotFound(Slice());
  } else {
    snprintf(tmp, sizeof(tmp), "%s: rados_err=%d", err_ctx, err_num);
    return Status::IOError(tmp);
  }
}

class RadosEmptyFile : public RandomAccessFile, public SequentialFile {
 public:
  explicit RadosEmptyFile() {}
  virtual ~RadosEmptyFile() {}
  virtual Status Read(uint64_t off, size_t n, Slice* result,
                      char* scratch) const {
    *result = Slice();
    return Status::OK();
  }
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    *result = Slice();
    return Status::OK();
  }
  virtual Status Skip(uint64_t n) {
    return Status::OK();  // The file is empty anyway
  }
};

class RadosSequentialFile : public SequentialFile {
 private:
  std::string oid_;
  rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;
  uint64_t off_;

 public:
  RadosSequentialFile(const Slice& fname, rados_ioctx_t ioctx, bool owns_ioctx)
      : rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx), off_(0) {
    oid_ = fname.ToString();
  }

  virtual ~RadosSequentialFile() {
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    int nbytes = rados_read(rados_ioctx_, oid_.c_str(), scratch, n, off_);
    if (nbytes < 0) {
      return RadosError("rados_read", nbytes);
    } else {
      *result = Slice(scratch, nbytes);
      off_ += nbytes;
      return Status::OK();
    }
  }

  virtual Status Skip(uint64_t n) {
    off_ += n;
    return Status::OK();
  }
};

class RadosRandomAccessFile : public RandomAccessFile {
 private:
  std::string oid_;
  mutable rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;

 public:
  RadosRandomAccessFile(const Slice& fname, rados_ioctx_t ioctx,
                        bool owns_ioctx)
      : rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx) {
    oid_ = fname.ToString();
  }

  virtual ~RadosRandomAccessFile() {
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Read(uint64_t off, size_t n, Slice* result,
                      char* scratch) const {
    int nbytes = rados_read(rados_ioctx_, oid_.c_str(), scratch, n, off);
    if (nbytes < 0) {
      return RadosError("rados_read", nbytes);
    } else {
      *result = Slice(scratch, nbytes);
      return Status::OK();
    }
  }
};

class RadosWritableFile : public WritableFile {
 private:
  std::string oid_;
  rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;

 public:
  RadosWritableFile(const Slice& fname, rados_ioctx_t ioctx, bool owns_ioctx)
      : rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx) {
    oid_ = fname.ToString();
  }

  virtual ~RadosWritableFile() {
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Append(const Slice& buf) {
    int r = rados_append(rados_ioctx_, oid_.c_str(), buf.data(), buf.size());
    if (r != 0) {
      return RadosError("rados_append", r);
    } else {
      return Status::OK();
    }
  }

  virtual Status Close() { return Status::OK(); }
  virtual Status Flush() { return Status::OK(); }
  virtual Status Sync() { return Status::OK(); }
};

class RadosAsyncWritableFile : public WritableFile {
 private:
  port::Mutex* mu_;
  RadosOpCtx* async_op_;
  std::string oid_;
  rados_ioctx_t rados_ioctx_;
  bool owns_ioctx_;

  Status Ref() {
    Status s;
    MutexLock ml(mu_);
    if (async_op_->ok()) {
      async_op_->Ref();
    } else {
      s = RadosError("rados_bg_io", async_op_->err());
    }

    return s;
  }

 public:
  RadosAsyncWritableFile(const Slice& fname, port::Mutex* mu,
                         rados_ioctx_t ioctx, bool owns_ioctx = true)
      : mu_(mu), rados_ioctx_(ioctx), owns_ioctx_(owns_ioctx) {
    async_op_ = new RadosOpCtx(mu_);
    oid_ = fname.ToString();
    Truncate();
  }

  virtual ~RadosAsyncWritableFile() {
    mu_->Lock();
    async_op_->Unref();
    mu_->Unlock();
    if (owns_ioctx_) {
      rados_ioctx_destroy(rados_ioctx_);
    }
  }

  virtual Status Append(const Slice& buf) {
    Status s = Ref();
    if (s.ok()) {
      rados_aio_append(rados_ioctx_, oid_.c_str(),
                       async_op_->comp(),  // aio callback
                       buf.data(), buf.size());
    }

    return s;
  }

  Status Truncate() {
    Status s = Ref();
    if (s.ok()) {
      rados_aio_write_full(rados_ioctx_, oid_.c_str(),
                           async_op_->comp(),  // aio callback
                           "", 0);
    }

    return s;
  }

  virtual Status Close() { return Status::OK(); }

  virtual Status Flush() { return Status::OK(); }

  virtual Status Sync() {
    Status s;
    rados_aio_flush(rados_ioctx_);
    MutexLock ml(mu_);
    if (!async_op_->ok()) {
      s = RadosError("rados_bg_io", async_op_->err());
    }

    return s;
  }
};

}  // namespace rados
}  // namespace pdlfs

#endif  // RADOS
