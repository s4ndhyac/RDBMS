
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

#define RM_EOF (-1) // end of a scan operator

class RelationManager;
// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator
{
public:
  RM_ScanIterator();
  ~RM_ScanIterator();

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  string tableName;
  string conditionAttribute;
  CompOp compOp;
  const void *value;
  const vector<string> *attributeNames;
  vector<Attribute> recordDescriptor;
  RBFM_ScanIterator rbfm_ScanIterator;
  RelationManager *rm;
  FileHandle fileHandle;
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager *instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
          const string &conditionAttribute,
          const CompOp compOp,                  // comparison type such as "<" and "="
          const void *value,                    // used in the comparison
          const vector<string> &attributeNames, // a list of projected attributes
          RM_ScanIterator &rm_ScanIterator);

  void persistCurrentTableId();

  // Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

protected:
  RelationManager();
  ~RelationManager();

private:
  RecordBasedFileManager *rbfm;
  const string columnCatalog = "Columns.tbl";
  const string tableCatalog = "Tables.tbl";
  const string currentTableIDFile = "CurrentTableID.tbl";
  int current_table_id = 3;
  // Describe schema of Tables catalog table
  vector<Attribute> tblRecordDescriptor;

  // Describe schema for Columns catalog table
  // Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
  vector<Attribute> colRecordDescriptor;

  vector<Attribute> tableIdRecordDescriptor;

  vector<Attribute> currentTableIDRecordDescriptor;

  RC getRecordDescriptorForTable(const string tableName, vector<Attribute> &recordDescriptor);
  int getTableIdForTable(string tableName, RID &rid);
  void readCurrentTableID();
};

#endif
