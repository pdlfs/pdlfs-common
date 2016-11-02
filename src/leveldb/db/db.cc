/*
 * Copyright (c) 2011 The LevelDB Authors.
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "pdlfs-common/leveldb/db/db.h"
#include "pdlfs-common/leveldb/db/options.h"
#include "pdlfs-common/leveldb/db/snapshot.h"
#include "pdlfs-common/leveldb/db/write_batch.h"

#include "pdlfs-common/dbfiles.h"
#include "pdlfs-common/env.h"

// TODEL
#include <iostream>
#define LOG(...) std::cout << std::dec << __FILE__ << ":" << __LINE__ << \
                   ":" << __func__ << " | " << __VA_ARGS__ << std::endl;

namespace pdlfs {

DB::~DB() {}

Snapshot::~Snapshot() {}

SnapshotImpl::~SnapshotImpl() {}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

Status DestroyVLog(const std::string& vlog_name, const DBOptions& options) {
	LOG("============");
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(vlog_name, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }
  FileLock* lock;
  const std::string lockname = LockFileName(vlog_name);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    for (size_t i = 0; i < filenames.size(); i++) {
    	if (filenames[i] == "." || filenames[i] == "..") {
    		continue;
    	}
			LOG("Will delete:" << vlog_name << "/" << filenames[i]);
			Status del = env->DeleteFile(vlog_name + "/" + filenames[i]);
			if (result.ok() && !del.ok()) {
				result = del;
			}
    }

    // Ignore error since state is already gone
    env->UnlockFile(lock);
    env->DeleteFile(lockname);

    // Ignore error in case dir contains other files
    env->DeleteDir(vlog_name);
  }
  return result;
}

Status DestroyDB(const std::string& dbname, const DBOptions& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }


  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
      	LOG("Will delete:" << dbname << "/" << filenames[i]);
      	Status del;
      	if (type == kColumnLevelDBDir) {
					LOG("Will soon delete leveldb:" << dbname << "/" << filenames[i]);
      		del = DestroyDB(dbname + "/" + filenames[i], options);
      	} else if (type == kColumnVLogDir) {
					LOG("Will soon delete vlog:" << dbname << "/" << filenames[i]);
      		del = DestroyVLog(dbname + "/" + filenames[i], options);
      	} else {
      		del = env->DeleteFile(dbname + "/" + filenames[i]);
        }
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    LOG("============");

    // Ignore error since state is already gone
    env->UnlockFile(lock);
    env->DeleteFile(lockname);

    // Ignore error in case dir contains other files
    env->DeleteDir(dbname);
  }
  return result;
}

}  // namespace pdlfs
