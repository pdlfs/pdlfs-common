/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include "pdlfs-common/status.h"

#include <net/if.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <string>
#include <vector>

namespace pdlfs {

struct Ifr {
  // Interface name (such as eth0)
  std::string name;
  // Network address (in the form of 11.22.33.44)
  std::string ip;
  // MTU number (such as 1500, 65520)
  int mtu;
};

class PosixIf {
 public:
  PosixIf() : fd_(-1) {
    ifconf_.ifc_buf = reinterpret_cast<char*>(ifr_);
    ifconf_.ifc_len = sizeof(ifr_);
  }

  ~PosixIf() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

  Status IfConf(std::vector<Ifr>* results);

  Status Open();

 private:
  struct ifreq ifr_[64];  // If record buffer
  struct ifconf ifconf_;
  // No copying allowed
  void operator=(const PosixIf&);
  PosixIf(const PosixIf&);

  int fd_;
};

}  // namespace pdlfs
