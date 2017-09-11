/*
 * Copyright (c) 2015-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "posix_netdev.h"
#include "posix_env.h"

#include <arpa/inet.h>
#include <errno.h>

namespace pdlfs {

Status PosixIf::IfConf(std::vector<Ifr>* results) {
  char buf[INET_ADDRSTRLEN];
  Ifr ifr;

  results->clear();
  for (size_t i = 0; i < ifconf_.ifc_len / sizeof(ifr_[0]); ++i) {
    struct sockaddr_in* s_in =
        reinterpret_cast<sockaddr_in*>(&ifr_[i].ifr_addr);

    ifr.ip =
        inet_ntop(AF_INET, &s_in->sin_addr, buf, INET_ADDRSTRLEN);  // Ip addr
    // Skip if unresolvable
    if (ifr.ip != NULL) {
      ifr.name = ifr_[i].ifr_name;  // Logic If name
      ifr.mtu = ifr_[i].ifr_mtu;    // If mtu

      results->push_back(ifr);
    }
  }

  return Status::OK();
}

Status PosixIf::Open() {
  Status s;

  fd_ = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
  if (fd_ == -1) {
    s = IOError("socket", errno);
  } else {
    int r = ioctl(fd_, SIOCGIFCONF, &ifconf_);
    if (r == -1) {
      s = IOError("ioctl", errno);
    }
  }

  return s;
}

}  // namespace pdlfs
