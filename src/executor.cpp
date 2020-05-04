/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <functional>
#include <string>
#include <iostream>
#include <ctime>

#include "storage.h"

using namespace std;

namespace badgerdb {

void TableScanner::print() const {
  // TODO: Printer the contents of the table
}

JoinOperator::JoinOperator(const File& leftTableFile,
                           const File& rightTableFile,
                           const TableSchema& leftTableSchema,
                           const TableSchema& rightTableSchema,
                           const Catalog* catalog,
                           BufMgr* bufMgr)
    : leftTableFile(leftTableFile),
      rightTableFile(rightTableFile),
      leftTableSchema(leftTableSchema),
      rightTableSchema(rightTableSchema),
      resultTableSchema(
          createResultTableSchema(leftTableSchema, rightTableSchema)),
      catalog(catalog),
      bufMgr(bufMgr),
      isComplete(false) {
  // nothing
}

TableSchema JoinOperator::createResultTableSchema(
    const TableSchema& leftTableSchema,
    const TableSchema& rightTableSchema) {
  vector<Attribute> attrs;
  // TODO: add attribute definitions
  return TableSchema("TEMP_TABLE", attrs, true);
}

void JoinOperator::printRunningStats() const {
  cout << "# Result Tuples: " << numResultTuples << endl;
  cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
  cout << "# I/Os: " << numIOs << endl;
}

bool OnePassJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;

  // TODO: Execute the join algorithm

  isComplete = true;
  return true;
}

bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;
  
  // TODO: Execute the join algorithm

  isComplete = true;
  return true;
}

BucketId GraceHashJoinOperator::hash(const string& key) const {
  std::hash<string> strHash;
  return strHash(key) % numBuckets;
}

bool GraceHashJoinOperator ::execute(int numAvailableBufPages, File& resultFile) {
  if (isComplete)
    return true;

  numResultTuples = 0;
  numUsedBufPages = 0;
  numIOs = 0;
  
  // TODO: Execute the join algorithm

  isComplete = true;
  return true;
}

}  // namespace badgerdb