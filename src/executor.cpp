/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"
#include "buffer.h"

#include <functional>
#include <string>
#include <iostream>
#include <ctime>
#include <map>

#include "storage.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/buffer_exceeded_exception.h"

namespace badgerdb {
  void printVectorInt(vector<int> vec) {
    for (unsigned int i = 0; i < vec.size(); i++) {
      std::cout << vec[i] << "\t";
    }
    std::cout << endl;
  }

  vector<string> split(const string tuple, const string delimiters) {
    vector<string> tokens;
    string::size_type lastPos = tuple.find_first_not_of(delimiters, 0);
    string::size_type pos = tuple.find_first_of(delimiters, lastPos);
    while (string::npos != pos || string::npos != lastPos) {
      tokens.push_back(tuple.substr(lastPos, pos - lastPos));
      lastPos = tuple.find_first_not_of(delimiters, pos);
      pos = tuple.find_first_of(delimiters, lastPos);
    }
    return tokens;
  }

  void TableScanner::print() const {
    std::cout << "scanning file - " << this->tableFile.filename() << "\n";
    try {
      File *file = &(this->tableFile);
      FileIterator itFile = file->begin();
      Page page;
      PageIterator itPage;
      string record;

      while (itFile != file->end()) {
        page = *(itFile);
        itPage = page.begin();
        while (itPage != page.end()) {
          record = *(itPage);
          std::cout << "record(pageNo: " << page.page_number() << ") - '" << record
              << "'\n";
          itPage++;
        }
        itFile++;
      }

    } catch (const InvalidPageException &e) {
      std::cout << "throws invalid page exception" << "\n";
    }
  }

  JoinOperator::JoinOperator(File &leftTableFile, File &rightTableFile,
      const TableSchema &leftTableSchema, const TableSchema &rightTableSchema,
      const Catalog *catalog, BufMgr *bufMgr) :
      leftTableFile(leftTableFile), rightTableFile(rightTableFile), leftTableSchema(
          leftTableSchema), rightTableSchema(rightTableSchema), resultTableSchema(
          createResultTableSchema(leftTableSchema, rightTableSchema)), catalog(catalog), bufMgr(
          bufMgr), isComplete(false) {
    // nothing
  }

  TableSchema JoinOperator::createResultTableSchema(const TableSchema &leftTableSchema,
      const TableSchema &rightTableSchema) {
    vector<Attribute> attrs;
    int leftAttrsNum = leftTableSchema.getAttrCount();
    int rightAttrsNum = rightTableSchema.getAttrCount();
    for (int i = 0; i < leftAttrsNum; i++) {
      string attrName = leftTableSchema.getAttrName(i);
      DataType attrType = leftTableSchema.getAttrType(i);
      int maxSize = leftTableSchema.getAttrMaxSize(i);
      bool isNotNull = leftTableSchema.isAttrNotNull(i);
      bool isUnique = leftTableSchema.isAttrUnique(i);
      Attribute tempAttr(attrName, attrType, maxSize, isNotNull, isUnique);
      tempAttr.isNotNull = isNotNull;
      tempAttr.isUnique = isUnique;
      attrs.push_back(tempAttr);
    }
    for (int i = 0; i < rightAttrsNum; i++) {
      string attrName = rightTableSchema.getAttrName(i);
      if (leftTableSchema.hasAttr(attrName)) {
        continue;
      } else {
        DataType attrType = rightTableSchema.getAttrType(i);
        int maxSize = rightTableSchema.getAttrMaxSize(i);
        bool isNotNull = rightTableSchema.isAttrNotNull(i);
        bool isUnique = rightTableSchema.isAttrUnique(i);
        Attribute tempAttr(attrName, attrType, maxSize, isNotNull, isUnique);
        tempAttr.isNotNull = isNotNull;
        tempAttr.isUnique = isUnique;
        attrs.push_back(tempAttr);
      }
    }
    return TableSchema("TEMP_TABLE", attrs, true);
  }

  void JoinOperator::printRunningStats() const {
    cout << "# Result Tuples: " << this->numResultTuples << endl;
    cout << "# Used Buffer Pages: " << this->numUsedBufPages << endl;
    cout << "# I/Os: " << this->numIOs << endl;
  }

  bool OnePassJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
    std::cout << "... executing one-pass join" << "\n";
    if (this->isComplete)
      return true;

    this->resultTableSchema.print();

    this->numResultTuples = 0;
    this->numUsedBufPages = 0;
    this->numIOs = 0;

    // result output
    File *resultFilePointer = &resultFile;
    PageId resultPageNo;
    Page *resultPage;

    // hash structure
    vector<string> joinAttrs;
    vector<int> joinAttrsIDLeft;
    vector<int> joinAttrsIDRight;
    vector<int> joinAttrsIDResult;
    map<string, vector<string>> bufferMap;

    // left table - 500; right table - 100
    File *leftFile = &(this->leftTableFile);
    File *rightFile = &(this->rightTableFile);
    FileIterator itRightFile = rightFile->begin();
    FileIterator itLeftFile = leftFile->begin();

    // confirm the attributes' names and ids used for one-pass join
    int leftTableAttrsNum = this->leftTableSchema.getAttrCount();
    int rightTableAttrsNum = this->rightTableSchema.getAttrCount();
    int resultTableAttrsNum = this->resultTableSchema.getAttrCount();
    for (int i = 0; i < leftTableAttrsNum; i++) {
      string leftAttrName = this->leftTableSchema.getAttrName(i);
      for (int j = 0; j < rightTableAttrsNum; j++) {
        string rightAttrName = this->rightTableSchema.getAttrName(j);
        if (leftAttrName == rightAttrName) {
          joinAttrs.push_back(leftAttrName);
          joinAttrsIDLeft.push_back(i);
          joinAttrsIDRight.push_back(j);
//          std::cout << "join attribute: " << leftAttrName << endl;
//          std::cout << "id in left: " << i << endl;
//          std::cout << "id in right: " << j << endl;
        }
      }
    }
    for (int i = 0; i < resultTableAttrsNum; i++) {
      string resultAttrName = this->resultTableSchema.getAttrName(i);
      for (unsigned int j = 0; j < joinAttrs.size(); j++) {
        string joinAttrName = joinAttrs[j];
        if (joinAttrName == resultAttrName) {
          joinAttrsIDResult.push_back(i);
        }
      }
    }
//    printVectorInt(joinAttrsIDLeft);
//    printVectorInt(joinAttrsIDRight);
//    printVectorInt(joinAttrsIDResult);

// build stage
    while (itLeftFile != leftFile->end()) {
      Page page = *(itLeftFile);
      PageIterator itPage = page.begin();
      while (itPage != page.end()) {
        string record = *(itPage);
//        std::cout << "record(pageNo: " << page.page_number() << ") - '" << record
//            << "'\n";
        vector<string> attrs = split(record, "\t");
        string key = "";
        for (unsigned int i = 0; i < joinAttrsIDLeft.size(); i++) {
          /*
           * about the 'attrs[joinAttrsIDLeft[i] + 1]'.
           *   this is according to the format of tuples stored in a Page,
           *   which is like "table_name \t attr1 \t attr2 \t ... "
           */
          key = key + attrs[joinAttrsIDLeft[i] + 1];
        }
        if (bufferMap.count(key) > 0) {
          bufferMap.at(key).push_back(record);
        } else {
          vector<string> values;
          values.push_back(record);
          bufferMap.insert(pair<string, vector<string>>(key, values));
        }
        itPage++;
        this->numIOs++;
        this->numUsedBufPages++;
      }
      itLeftFile++;
    }

//    for (auto &x : bufferMap) {
//      string key = x.first;
//      vector<string> value = x.second;
//      std::cout << "key: " << key << endl;
//      for (unsigned int i = 0; i < value.size(); i++) {
//        std::cout << "\t" << value[i] << endl;
//      }
//    }

// probe stage
    while (itRightFile != rightFile->end()) {
      Page page = *(itRightFile);
      PageIterator itPage = page.begin();
      while (itPage != page.end()) {
        this->bufMgr->allocPage(resultFilePointer, resultPageNo, resultPage);
        string record = *(itPage);
//        std::cout << "record(pageNo: " << page.page_number() << ") - '" << record
//            << "'\n";
        vector<string> attrs = split(record, "\t");
        string key = "";
        for (unsigned int i = 0; i < joinAttrsIDRight.size(); i++) {
          /*
           * about the 'attrs[joinAttrsIDRight[i] + 1]'.
           *   this is according to the format of tuples stored in a Page,
           *   which is like "table_name \t attr1 \t attr2 \t ... "
           */
          key = key + attrs[joinAttrsIDRight[i] + 1];
        }
        if (bufferMap.count(key) > 0) { // join the tuples
          vector<string> tuples = bufferMap[key];
          for (unsigned int i = 0; i < tuples.size(); i++) {
            string token = tuples[i];
            string joinedTuple = "";
            // add the left part
            vector<string> leftTokens = split(token, "\t");
            for (unsigned int j = 1; j < leftTokens.size(); j++) {
              joinedTuple = joinedTuple + leftTokens[j] + "\t";
            }

            // add the right part
            vector<string> rightTokens = split(record, "\t");
            for (unsigned int j = 1; j < rightTokens.size(); j++) {
              bool contains = false;
              for (unsigned int k = 0; k < joinAttrsIDRight.size(); k++) {
                if (j - 1 == (unsigned int) joinAttrsIDRight[k]) {
                  contains = true;
                  break;
                }
              }
              if (contains == false) {
                joinedTuple = joinedTuple + rightTokens[j] + "\t";
              }
            }

//            std::cout << "joined: " << joinedTuple << endl;
            resultPage->insertRecord("result\t" + joinedTuple);
            this->numResultTuples++;
          }
        } else { // do nothing
        }
        this->bufMgr->unPinPage(resultFilePointer, resultPageNo, true);
        this->bufMgr->flushFile(resultFilePointer);
        itPage++;
        this->numIOs++;
        this->numUsedBufPages++;
      }
      itRightFile++;
    }

    this->isComplete = true;
    return true;
  }

  bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
    std::cout << "... executing nested-loop join" << "\n";
    if (this->isComplete)
      return true;

    this->resultTableSchema.print();

    this->numResultTuples = 0;
    this->numUsedBufPages = 0;
    this->numIOs = 0;

    // size of a block when executing nested-loop join
    const int blockSize = 50;

    // result output
    File *resultFilePointer = &resultFile;
    PageId resultPageNo;
    Page *resultPage;

    // hash structure
    vector<string> joinAttrs;
    vector<int> joinAttrsIDLeft;
    vector<int> joinAttrsIDRight;
    vector<int> joinAttrsIDResult;
    map<string, vector<string>> bufferMap;

    // left table - 500; right table - 100
    File *leftFile = &(this->leftTableFile);
    File *rightFile = &(this->rightTableFile);
    FileIterator itRightFile = rightFile->begin();
    FileIterator itLeftFile = leftFile->begin();

    // confirm the attributes' names and ids used for one-pass join
    int leftTableAttrsNum = this->leftTableSchema.getAttrCount();
    int rightTableAttrsNum = this->rightTableSchema.getAttrCount();
    int resultTableAttrsNum = this->resultTableSchema.getAttrCount();
    for (int i = 0; i < leftTableAttrsNum; i++) {
      string leftAttrName = this->leftTableSchema.getAttrName(i);
      for (int j = 0; j < rightTableAttrsNum; j++) {
        string rightAttrName = this->rightTableSchema.getAttrName(j);
        if (leftAttrName == rightAttrName) {
          joinAttrs.push_back(leftAttrName);
          joinAttrsIDLeft.push_back(i);
          joinAttrsIDRight.push_back(j);
//          std::cout << "join attribute: " << leftAttrName << endl;
//          std::cout << "id in left: " << i << endl;
//          std::cout << "id in right: " << j << endl;
        }
      }
    }
    for (int i = 0; i < resultTableAttrsNum; i++) {
      string resultAttrName = this->resultTableSchema.getAttrName(i);
      for (unsigned int j = 0; j < joinAttrs.size(); j++) {
        string joinAttrName = joinAttrs[j];
        if (joinAttrName == resultAttrName) {
          joinAttrsIDResult.push_back(i);
        }
      }
    }
//    printVectorInt(joinAttrsIDLeft);
//    printVectorInt(joinAttrsIDRight);
//    printVectorInt(joinAttrsIDResult);

    int blockUsedCount = 0;
    // build stage
    while (itLeftFile != leftFile->end()) {
      Page leftPage = *(itLeftFile);
      PageIterator itLeftPage = leftPage.begin();
      this->bufMgr->allocPage(resultFilePointer, resultPageNo, resultPage);
      while (itLeftPage != leftPage.end()) {
        string leftRecord = *(itLeftPage);
//        std::cout << "record(pageNo: " << leftPage.page_number() << ") - '"
//            << leftRecord << "'\n";
        vector<string> leftAttrs = split(leftRecord, "\t");
        string leftKey = "";
        for (unsigned int i = 0; i < joinAttrsIDLeft.size(); i++) {
          /*
           * about the 'leftAttrs[joinAttrsIDLeft[i] + 1]'.
           *   this is according to the format of tuples stored in a Page,
           *   which is like "table_name \t attr1 \t attr2 \t ... "
           */
          leftKey = leftKey + leftAttrs[joinAttrsIDLeft[i] + 1];
        }
        if (bufferMap.count(leftKey) > 0) {
          bufferMap.at(leftKey).push_back(leftRecord);
        } else {
          vector<string> values;
          values.push_back(leftRecord);
          bufferMap.insert(pair<string, vector<string>>(leftKey, values));
        }
        blockUsedCount++;
        if ((blockUsedCount % blockSize == 0 && blockUsedCount != 0) == false) {
          itLeftPage++;
          continue;
        } else {
          // do nothing
          this->numIOs++;
          std::cout << "block used count: " << blockUsedCount << endl;
        }
        // probe stage
        itRightFile = rightFile->begin();
        while (itRightFile != rightFile->end()) {
          Page rightPage = *(itRightFile);
          PageIterator itRightPage = rightPage.begin();
          while (itRightPage != rightPage.end()) {
            string rightRecord = *(itRightPage);
//            std::cout << "record(pageNo: " << rightPage.page_number() << ") - '"
//                << rightRecord << "'\n";
            vector<string> rightAttrs = split(rightRecord, "\t");
            string rightKey = "";
            for (unsigned int i = 0; i < joinAttrsIDRight.size(); i++) {
              /*
               * about the 'rightAttrs[joinAttrsIDRight[i] + 1]'.
               *   this is according to the format of tuples stored in a Page,
               *   which is like "table_name \t attr1 \t attr2 \t ... "
               */
              rightKey = rightKey + rightAttrs[joinAttrsIDRight[i] + 1];
            }
            if (bufferMap.count(rightKey) > 0) {
              // join the tuples
              vector<string> tuples = bufferMap[rightKey];
              for (unsigned int i = 0; i < tuples.size(); i++) {
                string token = tuples[i];
                string joinedTuple = "";
                // add the left part
                vector<string> leftTokens = split(token, "\t");
                for (unsigned int j = 1; j < leftTokens.size(); j++) {
                  joinedTuple = joinedTuple + leftTokens[j] + "\t";
                }

                // add the right part
                vector<string> rightTokens = split(rightRecord, "\t");
                for (unsigned int j = 1; j < rightTokens.size(); j++) {
                  bool contains = false;
                  for (unsigned int k = 0; k < joinAttrsIDRight.size(); k++) {
                    if (j - 1 == (unsigned int) joinAttrsIDRight[k]) {
                      contains = true;
                      break;
                    }
                  }
                  if (contains == false) {
                    joinedTuple = joinedTuple + rightTokens[j] + "\t";
                  }
                }

//                std::cout << "joined: " << joinedTuple << endl;
                resultPage->insertRecord("result\t" + joinedTuple);
                this->numResultTuples++;
              }
            } else {
              // do nothing
            }
            itRightPage++;
            this->numIOs++;
          } // end of right page iteration
          itRightFile++;
        } // end of right file iteration
        bufferMap.clear();
        itLeftPage++;
      } // end of left page iteration
      this->bufMgr->unPinPage(resultFilePointer, resultPageNo, true);
      this->bufMgr->flushFile(resultFilePointer);
      itLeftFile++;
    } // end of left file iteration

    this->numUsedBufPages = blockSize + 1;

    this->isComplete = true;
    return true;
  }

  BucketId GraceHashJoinOperator::hash(const string &key) const {
    std::hash<string> strHash;
    return strHash(key) % this->numBuckets;
  }

  bool GraceHashJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
    if (this->isComplete)
      return true;

    this->numResultTuples = 0;
    this->numUsedBufPages = 0;
    this->numIOs = 0;

    // NO NEED TO DO

    this->isComplete = true;
    return true;
  }

} // namespace badgerdb
