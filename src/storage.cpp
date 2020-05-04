/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"

using namespace std;

namespace badgerdb {

RecordId HeapFileManager::insertTuple(const string& tuple,
                                      File& file,
                                      BufMgr* bufMgr) {
  // TODO
}

void HeapFileManager::deleteTuple(const RecordId& rid,
                                  File& file,
                                  BufMgr* bugMgr) {
  // TODO
}

string HeapFileManager::createTupleFromSQLStatement(const string& sql,
                                                    const Catalog* catalog) {
  // TODO
}
}  // namespace badgerdb