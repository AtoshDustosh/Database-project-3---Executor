/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "schema.h"
#include "exceptions/sql_invalid_exception.h"

#include <regex>
#include <iostream>

namespace badgerdb {

  TableSchema TableSchema::fromSQLStatement(const string &sql) {
    string tableName;
    vector<Attribute> attrs;
    bool isTemp = false;

    // patterns used
    string headerPattern = "CREATE\\sTABLE\\s([a-zA-Z_]+)\\s";
    string leftBracPattern = "\\(";
    string keywordsPattern = "(\\s(INT|NOT\\sNULL|UNIQUE|(VAR)?CHAR\\(\\d+\\)))+";
    string declarationsPattern = "(([a-zA-Z_]+)" + keywordsPattern + ",\\s)*"
        + "(([a-zA-Z_]+)" + keywordsPattern + ")";
    string rightBracPattern = "\\);";

    string pattern = headerPattern + leftBracPattern + declarationsPattern
        + rightBracPattern;

    // regex declaration
    regex table_regex;
    smatch search_results;
    smatch match_results;

    // extracted string
    string header = "";
    string declarations = "";

    // simple match check - whether the sql statement matches the format
    table_regex = regex(pattern);
    if (regex_search(sql, table_regex) == 0) {
      throw SqlInvalidException(sql);
    }

    // extract header and declarations from sql statement
    table_regex = regex(headerPattern);
    if (regex_search(sql, search_results, table_regex)) {
      //    cout << "Prefix: '" << search_results.prefix() << "'\n";
      //    cout << search_results[0] << '\n';
      header = search_results[0];
      //    cout << "Suffix: '" << search_results.suffix() << "\'\n\n";
      declarations = search_results.suffix();
      declarations = declarations.substr(1, declarations.length() - 3);
    }

    //  cout << "header: '" << header << "'\n";
    //  cout << "declarations: '" << declarations << "'\n\n";

    // match header and extract table name
    table_regex = regex(headerPattern);
    bool isMatch = regex_match(header, match_results, table_regex);
    if (isMatch) {
      tableName = match_results[1];
    }
//    cout << "tableName: '" << tableName << "'\n\n";

    // split declarations
    vector<string> vecDef;
    string temp = declarations;
    while (temp.find(", ") < temp.length()) {
      string subLeft = temp.substr(0, temp.find(", "));
      string subRight = temp.substr(temp.find(", ") + 2, temp.length());
      //    cout << "left: '" << subLeft << "'\n";
      //    cout << "right: '" << subRight << "'\n\n";
      temp = subRight;
      vecDef.push_back(subLeft);
    }
    vecDef.push_back(temp);

    // process declarations one by one
    for (uint32_t i = 0; i < vecDef.size(); i++) {
      // extract tokens from a declaration
      vector<string> vecToken;
      temp = vecDef[i];
//      cout << "processing: '" << temp << "'\n";
      while (temp.find(" ") < temp.length()) {
        string subLeft = temp.substr(0, temp.find(" "));
        string subRight = temp.substr(temp.find(" ") + 1, temp.length());
//        cout << "token: '" << subLeft << "'\n";
//        cout << "remained: '" << subRight << "'\n";
        temp = subRight;
        vecToken.push_back(subLeft);
      }
      vecToken.push_back(temp);
      // process tokens and build attribute from a declaration
      string attrName;
      DataType attrType;
      int maxSize = 0;
      bool isNotNull = false;
      bool isUnique = false;
      // vecToken[0] processed
      attrName = vecToken[0];
      // vecToken[1] processed
      regex tempReg = regex("(INT|VARCHAR|CHAR)(\\((\\d+)\\))?");
      isMatch = regex_match(vecToken[1], match_results, tempReg);
      if (isMatch) {
        if (match_results[1] == "INT") {
          maxSize = 0;
          attrType = badgerdb::DataType::INT;
        } else if (match_results[1] == "VARCHAR") {
          maxSize = stoi(match_results[3], 0, 10);
          attrType = badgerdb::DataType::VARCHAR;
        } else if (match_results[1] == "CHAR") {
          maxSize = stoi(match_results[3], 0, 10);
          attrType = badgerdb::DataType::CHAR;
        }
      }
      uint32_t notnull_complete = 0;
      for (uint32_t j = 2; j < vecToken.size(); j++) {
        string token = vecToken[j];
        if (token == "UNIQUE") {
          isUnique = true;
        } else if (token == "NOT" && notnull_complete == 0) {
          notnull_complete = 1;
        } else if (token == "NULL" && notnull_complete == 1) {
          isNotNull = true;
        }
      }
//      cout << attrName << "," << attrType << "," << maxSize << "," << isNotNull
//          << "," << isUnique << "\n";
      Attribute attr(attrName, attrType, maxSize, isNotNull, isUnique);
      attr.isNotNull = isNotNull;
      attr.isUnique = isUnique;
      attrs.push_back(attr);
//      cout << "\n";
    }
//    cout << "\n";
    return TableSchema(tableName, attrs, isTemp);
  }

  void TableSchema::print() const {
    cout << this->tableName << "\n";
    cout << "|name\t" << "|type\t\t" << "|size\t" << "|notnull\t" << "|unique\t|"
        << "\n";
    for (uint32_t i = 0; i < this->attrs.size(); i++) {
      Attribute attr = this->attrs[i];
      cout << "|" << attr.attrName << "\t";
      if (attr.attrType == badgerdb::DataType::INT) {
        cout << "|INT\t\t";
      } else if (attr.attrType == badgerdb::DataType::CHAR) {
        cout << "|CHAR\t\t";
      } else if (attr.attrType == badgerdb::DataType::VARCHAR) {
        cout << "|VARCHAR\t";
      }
      cout << "|" << attr.maxSize << "\t";
      if (attr.isNotNull == true) {
        cout << "|yes\t\t";
      } else {
        cout << "|no\t\t";
      }
      if (attr.isUnique == true) {
        cout << "|yes\t";
      } else {
        cout << "|no\t";
      }
      cout << "|\n";
    }
  }

} // namespace badgerdb
