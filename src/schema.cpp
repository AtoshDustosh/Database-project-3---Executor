/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "schema.h"

#include <string>

using namespace std;

namespace badgerdb {

TableSchema TableSchema::fromSQLStatement(const string& sql) {
  string tableName;
  vector<Attribute> attrs;
  bool isTemp = false;
  // TODO: create attribute definitions from sql
  return TableSchema(tableName, attrs, isTemp);
}

void TableSchema::print() const {
  // TODO
}

}  // namespace badgerdb