/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "sql_invalid_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

  SqlInvalidException::SqlInvalidException(const std::string &sql) :
      BadgerDbException(""), sql(sql) {
    std::stringstream ss;
    ss << "SQL input is invalid:  " << sql << "\n";
    message_.assign(ss.str());
  }

}
