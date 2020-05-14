/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include <math.h>
#include <stdlib.h>

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>
#include <regex>
#include <map>

#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/sql_invalid_exception.h"
#include "exceptions/file_exists_exception.h"
#include "executor.h"
#include "file_iterator.h"
#include "page.h"
#include "schema.h"
#include "page_iterator.h"
#include "storage.h"

using namespace badgerdb;

void createDatabase(BufMgr *bufMgr, Catalog *catalog) {
  // Create table schemas
  TableSchema leftTableSchema = TableSchema::fromSQLStatement(
      "CREATE TABLE r (a CHAR(8) NOT NULL UNIQUE, b INT);");
  TableSchema rightTableSchema = TableSchema::fromSQLStatement(
      "CREATE TABLE s (b INT UNIQUE NOT NULL, c VARCHAR(8));");

  leftTableSchema.print();
  rightTableSchema.print();

  // Create table files
  string leftTableFilename = "r.tbl";
  string rightTableFilename = "s.tbl";
  try {
    File::remove(leftTableFilename);
    File::remove(rightTableFilename);
  } catch (FileNotFoundException &e) {
  }
  File leftTableFile = File::create(leftTableFilename);
  File rightTableFile = File::create(rightTableFilename);

  // Add table schemas and filenames to catalog
  catalog->addTableSchema(leftTableSchema, leftTableFilename);
  catalog->addTableSchema(rightTableSchema, rightTableFilename);

  // Insert tuples
  int leftTableRows = 500;
  int rightTableRows = 100;

  std::cout << "creating tuples for " << leftTableFile.filename() << "..." << "\n";
  for (int i = 0; i < leftTableRows; i++) {
    if (i % (leftTableRows / 10) == 0) {
      std::cout << (i / (leftTableRows / 100)) << "%...\n";
    }
    stringstream ss;
    // INSERT INTO r VALUES (string, integer)
    ss << "INSERT INTO r VALUES ('r" << i << "', " << (i % rightTableRows) << ");";
    string tuple = HeapFileManager::createTupleFromSQLStatement(ss.str(), catalog);
    HeapFileManager::insertTuple(tuple, leftTableFile, bufMgr);
//    std::cout << ss.str() << "\n";
  }

  std::cout << "creating tuples for " << rightTableFile.filename() << "..." << "\n";
  for (int i = 0; i < rightTableRows; i++) {
    if (i % (rightTableRows / 10) == 0) {
      std::cout << (i / (rightTableRows / 100)) << "%...\n";
    }
    stringstream ss;
    ss << "INSERT INTO s VALUES (" << i << ", 's" << i << "');";
    string tuple = HeapFileManager::createTupleFromSQLStatement(ss.str(), catalog);
    HeapFileManager::insertTuple(tuple, rightTableFile, bufMgr);
//    std::cout << ss.str() << "\n";
  }

  // Print all tuples in tables
  TableScanner leftTableScanner(leftTableFile, leftTableSchema, bufMgr);
//  leftTableScanner.print();
  TableScanner rightTableScanner(rightTableFile, rightTableSchema, bufMgr);
//  rightTableScanner.print();
}

void testOnePassJoin(BufMgr *bufMgr, Catalog *catalog) {
  TableId leftTableId = catalog->getTableId("r");
  TableId rightTableId = catalog->getTableId("s");
  TableSchema leftTableSchema = catalog->getTableSchema(leftTableId);
  TableSchema rightTableSchema = catalog->getTableSchema(rightTableId);

  // Create one-pass join operator
  File tempLeftFile = File::open(catalog->getTableFilename(leftTableId));
  File tempRightFile = File::open(catalog->getTableFilename(rightTableId));
  OnePassJoinOperator joinOperator(tempLeftFile, tempRightFile, leftTableSchema,
      rightTableSchema, catalog, bufMgr);
  TableSchema resultSchema = joinOperator.getResultTableSchema();

  // Join two tables using one-pass join
  string filename = leftTableSchema.getTableName() + "_OPJ_"
      + rightTableSchema.getTableName() + ".tbl";
  try {
    File::remove(filename);
  } catch (const FileNotFoundException &e) {
  }
  File resultFile = File::create(filename);
  joinOperator.execute(100, resultFile);

  // Print running statistics
  joinOperator.printRunningStats();

  // Print all tuples in result
  TableScanner scanner(resultFile, resultSchema, bufMgr);
  scanner.print();
}

void testNestedLoopJoin(BufMgr *bufMgr, Catalog *catalog) {
  TableId leftTableId = catalog->getTableId("r");
  TableId rightTableId = catalog->getTableId("s");
  TableSchema leftTableSchema = catalog->getTableSchema(leftTableId);
  TableSchema rightTableSchema = catalog->getTableSchema(rightTableId);

  // Create nested-loop join operator
  File tempLeftFile = File::open(catalog->getTableFilename(leftTableId));
  File tempRightFile = File::open(catalog->getTableFilename(rightTableId));
  NestedLoopJoinOperator joinOperator(tempLeftFile, tempRightFile, leftTableSchema,
      rightTableSchema, catalog, bufMgr);
  TableSchema resultSchema = joinOperator.getResultTableSchema();

  // Join two tables using one-pass join
  string filename = leftTableSchema.getTableName() + "_NLJ_"
      + rightTableSchema.getTableName() + ".tbl";
  try {
    File::remove(filename);
  } catch (const FileNotFoundException &e) {
  }
  File resultFile = File::create(filename);
  joinOperator.execute(10, resultFile);

  // Print running statistics
  joinOperator.printRunningStats();

  // Print all tuples in result
  TableScanner scanner(resultFile, resultSchema, bufMgr);
  scanner.print();
}

void myTest() {

  map<int, string> mapStudent;

  mapStudent.insert(pair<int, string>(1, "student_one"));

  mapStudent.insert(pair<int, string>(2, "student_two"));

  mapStudent.insert(pair<int, string>(3, "student_three"));

  map<int, string>::iterator iter;

  iter = mapStudent.find(1);

  if (iter != mapStudent.end()) {
    cout << "Find, the value is " << iter->second << endl;
  } else {
    cout << "Do not Find" << endl;
  }

  iter++;
  cout << "map element: " << iter->second << endl;
  iter++;
  cout << "map element: " << iter->second << endl;
  iter++;
  if (iter == mapStudent.end()) {
    cout << "??? null ???" << endl;
  } else {
    cout << "map element: " << iter->second << endl;
  }
}

int main() {
  setvbuf(stdout, NULL, _IONBF, 0);
  myTest();
  // Create buffer pool
  int availableBufPages = 256;
  BufMgr *bufMgr = new BufMgr(availableBufPages);

// Create system catalog
  Catalog *catalog = new Catalog("lab3");

// Create tables
  createDatabase(bufMgr, catalog);

// Test one-pass join operator
  std::cout << "Test One-Pass Join ..." << endl;
  testOnePassJoin(bufMgr, catalog);

// Test nested-loop join operator
  std::cout << "Test Nested-Loop Join ..." << endl;
  testNestedLoopJoin(bufMgr, catalog);

// Destroy objects
  delete bufMgr;
  delete catalog;

  std::cout << "Test Completed" << endl;

  return 0;
}
