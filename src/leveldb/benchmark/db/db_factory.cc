//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"

#include <string>
#include "basic_db.h"
#include "lock_stl_db.h"
#include "ycsb_columnardb.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties& props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "lock_stl") {
    return new LockStlDB;
  } else if (props["dbname"] == "columnardb0") {
    return new YCSBColumnarDB(pdlfs::kLSMStyle);
  } else if (props["dbname"] == "columnardb1") {
    return new YCSBColumnarDB(pdlfs::kLSMKeyStyle);
  } else if (props["dbname"] == "leveldb") {
    return new YCSBColumnarDB();
  } else
    return NULL;
}
