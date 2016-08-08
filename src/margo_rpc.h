#pragma once

/*
 * Copyright (c) 2015-2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#if defined(MARGO) && defined(MERCURY)
#include <abt.h>
#include <margo.h>
#include "mercury_rpc.h"

namespace pdlfs {
namespace rpc {

class MargoRPC {
 public:
  typedef MercuryRPC::AddrEntry AddrEntry;
  typedef MercuryRPC::Addr Addr;
  hg_return_t Lookup(const std::string& addr, AddrEntry** result);
  MargoRPC(bool listen, const RPCOptions& options);
  void Unref();
  void Ref();

  MercuryRPC* hg_;
  margo_instance_id margo_id_;
  hg_id_t hg_rpc_id_;
  void RegisterRPC();
  Status Start();
  Status Stop();

  class Client;

 private:
  ~MargoRPC();
  // No copying allowed
  void operator=(const MargoRPC&);
  MargoRPC(const MargoRPC&);
  int num_io_threads_;
  uint64_t rpc_timeout_;
  port::Mutex mutex_;
  int refs_;
};

}  // namespace rpc
}  // namespace pdlfs

#endif
