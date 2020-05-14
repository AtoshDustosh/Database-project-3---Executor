/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include <regex>
#include <iostream>

#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "storage.h"
#include "buffer.h"

using namespace std;

namespace badgerdb {

  /*
   * Created from an sql statement like "INSERT INTO r VALUES ('string', 32)".
   * Tuple format:
   *   "tableName \t attrValue1 \t attrValue2 ..."
   */

  RecordId HeapFileManager::insertTuple(const string &tuple, File &file,
      BufMgr *bufMgr) {
    Page *page;
    PageId pageNo;
    RecordId recId;
    try {
      bufMgr->allocPage(&file, pageNo, page);
      recId = page->insertRecord(tuple);
      bufMgr->unPinPage(&file, pageNo, true);
      bufMgr->flushFile(&file);
    } catch (const BufferExceededException &e) {
      std::cout << "throws buffer exceeded exception" << "\n";
    } catch (const PageNotPinnedException &e) {
      std::cout << "throws page not pinned exception" << "\n";
    } catch (const BadBufferException &e) {
      std::cout << "throws bad buffer exception" << "\n";
    } catch (const PagePinnedException &e) {
      std::cout << "throws page pinned exception" << "\n";
    }
    return recId;
  }

  void HeapFileManager::deleteTuple(const RecordId &rid, File &file, BufMgr *bufMgr) {
    PageId pageNo = rid.page_number;
    Page *page;
    try {
      bufMgr->readPage(&file, pageNo, page);
      page->deleteRecord(rid);
      bufMgr->unPinPage(&file, pageNo, true);
      bufMgr->flushFile(&file);
    } catch (const BufferExceededException &e) {
      std::cout << "throws buffer exceeded exception" << "\n";
    } catch (const PageNotPinnedException &e) {
      std::cout << "throws page not pinned exception" << "\n";
    } catch (const BadBufferException &e) {
      std::cout << "throws bad buffer exception" << "\n";
    } catch (const PagePinnedException &e) {
      std::cout << "throws page pinned exception" << "\n";
    }
  }

  string HeapFileManager::createTupleFromSQLStatement(const string &sql,
      const Catalog *catalog) {
//    cout << "creating a new tuple using sql: '" << sql << "'\n";
    // sql example: INSERT INTO r VALUES ('string', integer)
    string tuple = "";
    TableId tableID;
    vector<string> attrsVec;

    // components of regex tool
    string pattern =
        "INSERT\\sINTO\\s([0-9a-zA-Z_]+)\\sVALUES\\s\\(((\\w|\\'|\\s|,)+)\\);";
    regex sqlRegex = regex(pattern);
    smatch results;

    // storage for results
    string tableName;
    string attrsString;

    // extract tableName and attributes' values
    bool isMatch = regex_match(sql, results, sqlRegex);
    if (isMatch) {
      tableName = results[1];
      attrsString = results[2];
//      cout << "table name: " << tableName << "\n";
//      cout << "attributes: " << attrsString << "\n";
    }
    tableID = catalog->getTableId(tableName);
    TableSchema tableSchema = catalog->getTableSchema(tableID);

    // extract every attribute's value
    while (attrsString.find(", ") < attrsString.length()) {
      string subLeft = attrsString.substr(0, attrsString.find(", "));
      string subRight = attrsString.substr(attrsString.find(", ") + 2,
          attrsString.length());
      attrsString = subRight;
      attrsVec.push_back(subLeft);
    }
    attrsVec.push_back(attrsString);

    // check whether the values match the type of their attributes ... no need
    // as this is not the project's main objective

    // compose elements into a tuple
    tuple = tableName;
    for (uint32_t i = 0; i < attrsVec.size(); i++) {
      tuple = tuple + "\t" + attrsVec[i];
    }

    return tuple;
  }
} // namespace badgerdb
