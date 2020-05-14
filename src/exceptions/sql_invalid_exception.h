/**
 * @author Atosh Dustosh
 *
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"
#include "../types.h"

namespace badgerdb {

  /**
   * @brief An exception that is thrown when SQL input is invalid.
   */
  class SqlInvalidException: public BadgerDbException {
    public:
      /**
       * Constructs a SQL invalid exception for the sql input.
       */
      explicit SqlInvalidException(const std::string &sql);

    protected:

      /*
       * SQL input.
       */
      const std::string &sql;
  };

}
