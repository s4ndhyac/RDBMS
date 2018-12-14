
#include "rm.h"

#include <stdlib.h>
#include <cstring>
#include <iostream>

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"

RC success = 0;
RC failure = 1;

enum TableType { SYSTEM_TABLE = 0, USER_TABLE = 1 };

RelationManager *RelationManager::_rm = 0;
RelationManager *RelationManager::instance() {
  if (!_rm) _rm = new RelationManager();

  return _rm;
}

RelationManager::RelationManager() {
  rbfm = RecordBasedFileManager::instance();

  tblRecordDescriptor.push_back((Attribute){"table-type", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-name", TypeVarChar, 50});
  tblRecordDescriptor.push_back((Attribute){"file-name", TypeVarChar, 50});

  // Describe schema for Columns catalog table
  // Columns(table-id:int, column-name:varchar(50), column-type:int,
  // column-length:int, column-position:int)
  colRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-name", TypeVarChar, 50});
  colRecordDescriptor.push_back((Attribute){"column-type", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-length", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-position", TypeInt, 4});

  currentTableIDRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});

  indexTableRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  indexTableRecordDescriptor.push_back(
      (Attribute){"index-name", TypeVarChar, 50});
  indexTableRecordDescriptor.push_back(
      (Attribute){"index-file-name", TypeVarChar, 50});
}

RelationManager::~RelationManager() {}

RC RelationManager::createCatalog() {
  FileHandle fileHandle;
  rbfm->createFile(tableCatalog);
  rbfm->openFile(tableCatalog, fileHandle);

  //  Prepare raw table record for insertion
  //	Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  RawRecordPreparer tblCtlgPrp = RawRecordPreparer(tblRecordDescriptor);
  char *tableCatalogRecord = (char *)malloc(PAGE_SIZE);
  memset(tableCatalogRecord, 0, PAGE_SIZE);
  tblCtlgPrp
      .setField(SYSTEM_TABLE)  // table-type
      .setField(1)             // table-id
      .setField("Tables")      // table-name
      .setField(tableCatalog)  // file-name
      .prepareRecord(tableCatalogRecord);

  // insert first tableCatalog record for 'Tables' table
  RID rid;
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord, rid);
  free(tableCatalogRecord);
  //  Prepare raw table record for insertion
  //	Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  char *tableCatalogRecord2 = (char *)malloc(PAGE_SIZE);
  memset(tableCatalogRecord2, 0, PAGE_SIZE);
  tblCtlgPrp
      .setField(SYSTEM_TABLE)   // table-type
      .setField(2)              // table-id
      .setField("Columns")      // table-name
      .setField(columnCatalog)  // file-name
      .prepareRecord(tableCatalogRecord2);

  // insert second tableCatalog record for 'Tables' table
  RID rid2;
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord2,
                     rid2);

  memset(tableCatalogRecord2, 0, PAGE_SIZE);
  tblCtlgPrp
      .setField(SYSTEM_TABLE)  // table-type
      .setField(3)             // table-id
      .setField("Index")       // table-name
      .setField(indexCatalog)  // file-name
      .prepareRecord(tableCatalogRecord2);

  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord2,
                     rid2);

  rbfm->closeFile(fileHandle);
  free(tableCatalogRecord2);

  // Open file for columns table
  FileHandle fileHandleForCols;
  rbfm->createFile(columnCatalog);
  rbfm->openFile(columnCatalog, fileHandleForCols);

  //  Prepare raw table record for insertion
  //	 (1 , "table-id"        , TypeInt     , 4  , 1)
  //	 (1 , "table-name"      , TypeVarChar , 50 , 2)
  //	 (1 , "file-name"       , TypeVarChar , 50 , 3)
  //	 (2 , "table-id"        , TypeInt     , 4  , 1)
  //	 (2 , "column-name"     , TypeVarChar , 50 , 2)
  //	 (2 , "column-type"     , TypeInt     , 4  , 3)
  //	 (2 , "column-length"   , TypeInt     , 4  , 4)
  //	 (2 , "column-position" , TypeInt     , 4  , 5)

  //	 (1 , "table-id"        , TypeInt     , 4  , 1)
  RawRecordPreparer colCtlgPrp = RawRecordPreparer(colRecordDescriptor);
  char *columnCatalogRecord = (char *)malloc(PAGE_SIZE);
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(1)           // table-id
      .setField("table-id")  // column-name
      .setField(TypeInt)     // column-type
      .setField(4)           // column-length
      .setField(1)           // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (1 , "table-name"      , TypeVarChar , 50 , 2)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(1)             // table-id
      .setField("table-name")  // column-name
      .setField(TypeVarChar)   // column-type
      .setField(50)            // column-length
      .setField(2)             // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (1 , "file-name"       , TypeVarChar , 50 , 3)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(1)            // table-id
      .setField("file-name")  // column-name
      .setField(TypeVarChar)  // column-type
      .setField(50)           // column-length
      .setField(3)            // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (2 , "table-id"        , TypeInt     , 4  , 1)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(2)           // table-id
      .setField("table-id")  // column-name
      .setField(TypeInt)     // column-type
      .setField(4)           // column-length
      .setField(1)           // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (2 , "column-name"     , TypeVarChar , 50 , 2)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(2)              // table-id
      .setField("column-name")  // column-name
      .setField(TypeVarChar)    // column-type
      .setField(50)             // column-length
      .setField(2)              // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (2 , "column-type"     , TypeInt     , 4  , 3)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(2)              // table-id
      .setField("column-type")  // column-name
      .setField(TypeInt)        // column-type
      .setField(4)              // column-length
      .setField(3)              // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (2 , "column-length"   , TypeInt     , 4  , 4)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(2)                // table-id
      .setField("column-length")  // column-name
      .setField(TypeInt)          // column-type
      .setField(4)                // column-length
      .setField(4)                // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //	 (2 , "column-position" , TypeInt     , 4  , 5)
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(2)                  // table-id
      .setField("column-position")  // column-name
      .setField(TypeInt)            // column-type
      .setField(4)                  // column-length
      .setField(5)                  // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  //  {"table-id", TypeInt, 4};
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(3)           // table-id
      .setField("table-id")  // column-name
      .setField(TypeInt)     // column-type
      .setField(4)           // column-length
      .setField(1)           // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);
  //  {"index-name", TypeVarChar, 50};
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(3)             // table-id
      .setField("index-name")  // column-name
      .setField(TypeVarChar)   // column-type
      .setField(50)            // column-length
      .setField(2)             // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);
  //  {"index-file-name", TypeVarChar, 50});
  memset(columnCatalogRecord, 0, PAGE_SIZE);
  colCtlgPrp
      .setField(3)                  // table-id
      .setField("index-file-name")  // column-name
      .setField(TypeVarChar)        // column-type
      .setField(50)                 // column-length
      .setField(3)                  // column-position
      .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor,
                     columnCatalogRecord, rid);

  rbfm->closeFile(fileHandleForCols);
  free(columnCatalogRecord);

  // create empty file for index catalog
  FileHandle fileHandleForIndexCat;
  rbfm->createFile(indexCatalog);
  rbfm->openFile(indexCatalog, fileHandleForIndexCat);
  rbfm->closeFile(fileHandleForIndexCat);

  current_table_id = 4;
  rbfm->createFile(currentTableIDFile);
  FileHandle tableIDFileHandle;
  rbfm->openFile(currentTableIDFile, tableIDFileHandle);
  RID tableIDRID;
  RawRecordPreparer maxIdRecordPrp =
      RawRecordPreparer(currentTableIDRecordDescriptor);
  char *maxIDRecord = (char *)malloc(PAGE_SIZE);
  memset(maxIDRecord, 0, PAGE_SIZE);
  maxIdRecordPrp.setField(current_table_id).prepareRecord(maxIDRecord);
  rbfm->insertRecord(tableIDFileHandle, currentTableIDRecordDescriptor,
                     maxIDRecord, tableIDRID);
  rbfm->closeFile(tableIDFileHandle);
  free(maxIDRecord);
  return success;
}

void RelationManager::persistCurrentTableId() {
  FileHandle fileHandle;
  rbfm->openFile(currentTableIDFile, fileHandle);
  RID rid = {0, 0};
  RawRecordPreparer maxIdRecordPrp =
      RawRecordPreparer(currentTableIDRecordDescriptor);
  char *maxIDRecord = (char *)malloc(PAGE_SIZE);
  memset(maxIDRecord, 0, PAGE_SIZE);
  maxIdRecordPrp.setField(current_table_id).prepareRecord(maxIDRecord);
  rbfm->updateRecord(fileHandle, currentTableIDRecordDescriptor, maxIDRecord,
                     rid);
  free(maxIDRecord);

  rbfm->closeFile(fileHandle);
}

bool isSystemTable(const string &tableName) {
  if (tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0)
    return true;
  return false;
}

RC RelationManager::deleteCatalog() {
  if (rbfm->destroyFile(tableCatalog) != 0) {
    return -1;
  }

  if (rbfm->destroyFile(columnCatalog) != 0) {
    return -1;
  }

  if (rbfm->destroyFile(currentTableIDFile) != 0) {
    current_table_id = 1;
    return -1;
  }
  return success;
}

RC RelationManager::createTable(const string &tableName,
                                const vector<Attribute> &attrs) {
  const string fileName = tableName + ".tbl";
  FileHandle fileHandle;
  int status = rbfm->createFile(fileName);

  if (status != success) {
    //  cout << "Create table failed for table " << tableName << endl;
    // cout << "File already exists" << endl;
    return status;
  }

  // insert tuple in table catalog
  readCurrentTableID();
  rbfm->openFile(tableCatalog, fileHandle);
  RawRecordPreparer tblRecordPrp = RawRecordPreparer(tblRecordDescriptor);
  RID rid;
  char *tableCatalogRecord = (char *)malloc(PAGE_SIZE);
  memset(tableCatalogRecord, 0, PAGE_SIZE);
  tblRecordPrp.setField(USER_TABLE)
      .setField(current_table_id)
      .setField(tableName)
      .setField(fileName)
      .prepareRecord(tableCatalogRecord);
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord, rid);
  rbfm->closeFile(fileHandle);

  // insert tuples in column catalog
  FileHandle colFileHandle;
  rbfm->openFile(columnCatalog, colFileHandle);
  RawRecordPreparer colRecordPrp = RawRecordPreparer(colRecordDescriptor);
  char *colCatalogRecord;
  colCatalogRecord = (char *)malloc(PAGE_SIZE);
  int colPosition = 1;
  for (Attribute attr : attrs) {
    memset(colCatalogRecord, 0, PAGE_SIZE);
    colRecordPrp.setField(current_table_id)
        .setField(attr.name)
        .setField(attr.type)
        .setField(attr.length)
        .setField(colPosition++)
        .prepareRecord(colCatalogRecord);
    rbfm->insertRecord(colFileHandle, colRecordDescriptor, colCatalogRecord,
                       rid);
  }
  rbfm->closeFile(colFileHandle);
  free(colCatalogRecord);

  current_table_id++;
  persistCurrentTableId();

  return success;
}

RC RelationManager::deleteTable(const string &tableName) {
  if (isSystemTable(tableName)) return -1;

  RID tableIdRID;
  const int tableId = getTableIdForTable(tableName, tableIdRID);

  if (tableId == 0) {
    return -1;
  }
  // delete tables from table catalog
  FileHandle fileHandle;

  if (rbfm->openFile(tableCatalog, fileHandle) != 0) return -1;

  if (rbfm->deleteRecord(fileHandle, tblRecordDescriptor, tableIdRID) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0) return -1;

  vector<string> attrNames;
  attrNames.push_back("table-id");

  RBFM_ScanIterator rbfmsi;

  // delete records from column catalog
  FileHandle colFileHandle;
  if (rbfm->openFile(columnCatalog, colFileHandle) != 0) return -1;

  rbfm->scan(colFileHandle, colRecordDescriptor, "table-id", EQ_OP, &tableId,
             attrNames, rbfmsi);

  void *buffer = malloc(PAGE_SIZE);
  RID rid;
  vector<RID> ridsToDelete;
  while (rbfmsi.getNextRecord(rid, buffer) != RBFM_EOF) {
    ridsToDelete.push_back(rid);
  }
  for (RID ridToDelete : ridsToDelete) {
    if (rbfm->deleteRecord(colFileHandle, colRecordDescriptor, ridToDelete) !=
        0)
      return -1;
  }

  if (rbfm->closeFile(colFileHandle) != 0) return -1;

  // remove the file to destroy
  string filename = tableName + ".tbl";
  if (rbfm->destroyFile(filename) != 0) return -1;

  return success;
}

RC RelationManager::getAttributes(const string &tableName,
                                  vector<Attribute> &attrs) {
  RC status = getRecordDescriptorForTable(tableName, attrs);
  return status;
}

/**
 * modifies key to store attrValue in form | length of string | string |
 * @param data
 * @param key
 * @param recordDescriptor
 * @param attributeName
 * @return
 */
RC getValueFromRawData(const void *data, void *key,
                       const vector<Attribute> &recordDescriptor,
                       const string &attributeName) {
  //  Record record = Record(recordDescriptor, (char *)data);
  RawRecord record =
      RawRecord(static_cast<const char *>(data), recordDescriptor);
  Value keyValue = record.getAttributeValue(attributeName);
  memset(key, 0, PAGE_SIZE);
  memcpy(key, keyValue.data, keyValue.size);
  // TODO: Need to handle for null pointer values. Do not insert null values in
  // index
  return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
                                RID &rid) {
  if (isSystemTable(tableName)) return failure;
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  RC rc = getRecordDescriptorForTable(tableName, recordDescriptor);
  if (rc == failure)  // if the table does not exist
    return rc;
  rbfm->openFile(fileName, fileHandle);
  rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
  rbfm->closeFile(fileHandle);

  // get tableId of given tableName
  RID tableIdRID;
  const int tableId = getTableIdForTable(tableName, tableIdRID);

  // scan the Index.tbl to check which attributes of given table have indexes on
  // them
  vector<string> indexTblAttrNames;
  indexTblAttrNames.push_back("table-id");
  indexTblAttrNames.push_back("index-name");
  indexTblAttrNames.push_back("index-file-name");
  RM_ScanIterator rm_ScanIterator;
  const string tableIdAttribute = "table-id";

  RBFM_ScanIterator rbfmScanner;
  FileHandle indexTblFileHandle;
  rbfm->openFile(indexCatalog, indexTblFileHandle);

  rbfm->scan(indexTblFileHandle, indexTableRecordDescriptor, tableIdAttribute,
             EQ_OP, &tableId, indexTblAttrNames, rbfmScanner);

  // create index entry for each row returned by Index.tbl
  void *indexRow = malloc(PAGE_SIZE);
  void *key = malloc(PAGE_SIZE);
  void *attributeName = malloc(PAGE_SIZE);
  Attribute attribute;
  IndexManager *ixManager = IndexManager::instance();
  IXFileHandle ixFileHandle;
  RID indexRID;
  // for each matching attribute in the Index.tbl
  while (rbfmScanner.getNextRecord(indexRID, indexRow) != RBFM_EOF) {
    // get the name of the index attribute
    getIndexAttribute(indexRow, attributeName, indexTableRecordDescriptor);

    // find the attribute type
    for (size_t i = 0; i < recordDescriptor.size(); i++) {
      if ((recordDescriptor[i].name).compare((char *)attributeName) == 0) {
        attribute = recordDescriptor[i];
        break;
      }
    }

    // Get key from insert raw data
    getValueFromRawData(data, key, recordDescriptor, (char *)attributeName);

    // prepare the index file name
    string indexFileName = tableName + "_" + *(char *)attributeName + "_idx";

    if (ixManager->openFile(indexFileName, ixFileHandle) != 0) {
      return -1;
    }

    if (ixManager->insertEntry(ixFileHandle, attribute, key, rid) != 0) {
      return -1;
    }

    if (ixManager->closeFile(ixFileHandle) != 0) {
      return -1;
    }
  }

  rbfm->closeFile(indexTblFileHandle);

  free(indexRow);
  indexRow = nullptr;
  free(key);
  key = nullptr;
  free(attributeName);
  attributeName = nullptr;
  return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
  if (isSystemTable(tableName)) return failure;
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);
  rbfm->openFile(fileName, fileHandle);
  char *oldRecord = (char *)malloc(PAGE_SIZE);
  memset(oldRecord, 0, PAGE_SIZE);
  rbfm->readRecord(fileHandle, recordDescriptor, rid, oldRecord);
  RC status = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
  rbfm->closeFile(fileHandle);

  // Get tableId from given tableName
  RID tableIdRID;
  const int tableId = getTableIdForTable(tableName, tableIdRID);

  // scan Index.tbl by tableId to get all attributes having indexes from current
  // table
  vector<string> indexTblAttrNames;
  indexTblAttrNames.push_back("table-id");
  indexTblAttrNames.push_back("index-name");
  indexTblAttrNames.push_back("index-file-name");
  RM_ScanIterator rm_ScanIterator;
  const string tableIdAttribute = "table-id";

  RBFM_ScanIterator rbfmScanner;
  FileHandle indexTblFileHandle;
  rbfm->openFile(indexCatalog, indexTblFileHandle);

  rbfm->scan(indexTblFileHandle, indexTableRecordDescriptor, tableIdAttribute,
             EQ_OP, &tableId, indexTblAttrNames, rbfmScanner);

  // delete index entry for each attribute returned by Index.tbl
  void *indexRow = malloc(PAGE_SIZE);
  void *key = malloc(PAGE_SIZE);
  void *attributeName = malloc(PAGE_SIZE);
  Attribute attribute;
  IndexManager *ixManager = IndexManager::instance();
  IXFileHandle ixFileHandle;
  RID indexRID;
  // for each matching attribute in the Index.tbl
  while (rbfmScanner.getNextRecord(indexRID, indexRow) != RBFM_EOF) {
    getIndexAttribute(indexRow, attributeName, indexTableRecordDescriptor);

    for (int i = 0; i < recordDescriptor.size(); i++) {
      if ((recordDescriptor[i].name).compare((char *)attributeName) == 0) {
        attribute = recordDescriptor[i];
        break;
      }
    }
    string indexFileName = tableName + "_" + *(char *)attributeName + "_idx";

    if (ixManager->openFile(indexFileName, ixFileHandle) != 0) {
      return -1;
    }

    // get the value of the old key
    memset(key, 0, PAGE_SIZE);
    getValueFromRawData(oldRecord, key, recordDescriptor,
                        (char *)attributeName);

    // delete the index entry by rid and curr key
    if (ixManager->deleteEntry(ixFileHandle, attribute, key, rid) != 0) {
      return -1;
    }

    if (ixManager->closeFile(ixFileHandle) != 0) {
      return -1;
    }
  }

  rbfm->closeFile(indexTblFileHandle);

  free(key);
  free(indexRow);
  free(attributeName);
  free(oldRecord);

  return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
                                const RID &rid) {
  if (isSystemTable(tableName)) return failure;
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);
  rbfm->openFile(fileName, fileHandle);
  char *oldRecord = (char *)malloc(PAGE_SIZE);
  memset(oldRecord, 0, PAGE_SIZE);
  rbfm->readRecord(fileHandle, recordDescriptor, rid, oldRecord);
  RC status = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
  rbfm->closeFile(fileHandle);

  // Get tableId from given tableName
  RID tableIdRID;
  const int tableId = getTableIdForTable(tableName, tableIdRID);

  // scan the Index.tbl to check which attributes of given table have indexes on
  // them
  vector<string> indexTblAttrNames;
  indexTblAttrNames.push_back("table-id");
  indexTblAttrNames.push_back("index-name");
  indexTblAttrNames.push_back("index-file-name");
  RM_ScanIterator rm_ScanIterator;
  const string tableIdAttribute = "table-id";

  RBFM_ScanIterator rbfmScanner;
  FileHandle indexTblFileHandle;
  rbfm->openFile(indexCatalog, indexTblFileHandle);

  rbfm->scan(indexTblFileHandle, indexTableRecordDescriptor, tableIdAttribute,
             EQ_OP, &tableId, indexTblAttrNames, rbfmScanner);

  // delete and re-insert index entry for each row returned by Index.tbl
  void *indexRow = malloc(PAGE_SIZE);
  void *key = malloc(PAGE_SIZE);
  void *attributeName = malloc(PAGE_SIZE);
  Attribute attribute;
  IndexManager *ixManager = IndexManager::instance();
  IXFileHandle ixFileHandle;
  RID indexRID;
  // for each matching attribute in the Index.tbl
  while (rbfmScanner.getNextRecord(indexRID, indexRow) != RBFM_EOF) {
    getIndexAttribute(indexRow, attributeName, indexTableRecordDescriptor);

    for (int i = 0; i < recordDescriptor.size(); i++) {
      if ((recordDescriptor[i].name).compare((char *)attributeName) == 0) {
        attribute = recordDescriptor[i];
        break;
      }
    }
    string indexFileName = tableName + "_" + *(char *)attributeName + "_idx";

    if (ixManager->openFile(indexFileName, ixFileHandle) != 0) {
      return -1;
    }

    // Get the value of the old key
    memset(key, 0, PAGE_SIZE);
    getValueFromRawData(oldRecord, key, recordDescriptor,
                        (char *)attributeName);

    // delete the index entry by rid and curr key
    if (ixManager->deleteEntry(ixFileHandle, attribute, key, rid) != 0) {
      return -1;
    }

    // Get key from insert raw data
    memset(key, 0, PAGE_SIZE);
    getValueFromRawData(data, key, recordDescriptor, (char *)attributeName);

    // insert an index entry by new key and rid
    if (ixManager->insertEntry(ixFileHandle, attribute, key, rid) != 0) {
      return -1;
    }

    if (ixManager->closeFile(ixFileHandle) != 0) {
      return -1;
    }
  }

  rbfm->closeFile(indexTblFileHandle);

  free(indexRow);
  free(key);
  free(attributeName);
  free(oldRecord);

  return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
                              void *data) {
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  int tableExists = getRecordDescriptorForTable(tableName, recordDescriptor);
  if (tableExists == failure) return failure;
  rbfm->openFile(fileName, fileHandle);
  RC status = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
                               const void *data) {
  RC status = rbfm->printRecord(attrs, data);
  return status;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
                                  const string &attributeName, void *data) {
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";

  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);

  if (rbfm->openFile(fileName, fileHandle) == failure) return failure;

  RC status = rbfm->readAttribute(fileHandle, recordDescriptor, rid,
                                  attributeName, data);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute, const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
  rm_ScanIterator.tableName = tableName;
  rm_ScanIterator.conditionAttribute = conditionAttribute;
  rm_ScanIterator.compOp = compOp;
  rm_ScanIterator.value = value;
  rm_ScanIterator.attributeNames = &attributeNames;
  rbfm->openFile(tableName + ".tbl", rm_ScanIterator.fileHandle);
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, rm_ScanIterator.recordDescriptor);
  //  rbfm->scan(fileHandle, recordDescriptor,
  //		  conditionAttribute, compOp, rbfmScanner value, attributeNames,
  //		  rm_ScanIterator.rbfm_ScanIterator);
  rbfm->scan(rm_ScanIterator.fileHandle, rm_ScanIterator.recordDescriptor,
             conditionAttribute, compOp, value, attributeNames,
             rm_ScanIterator.rbfm_ScanIterator);
  return success;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
                                  const string &attributeName) {
  return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName,
                                 const Attribute &attr) {
  return -1;
}

RC getKeyFromData(const void *data, const Attribute &attribute, void *key) {
  int offset = 1;
  if (attribute.type == TypeVarChar) {
    int length;
    memcpy(&length, (char *)data + offset, sizeof(int));
    memcpy(key, (char *)data + offset, sizeof(int) + length);
  } else {
    memcpy(key, (char *)data + offset, sizeof(int));
  }
  return 0;
}

RC RelationManager::createIndex(const string &tableName,
                                const string &attributeName) {
  vector<Attribute> attributes;
  getAttributes(tableName, attributes);
  Attribute currAttribute;
  int i = 0;
  for (i = 0; i < attributes.size(); i++) {
    if (attributes[i].name.compare(attributeName) == 0)
      currAttribute = attributes[i];
  }

  // create index file
  int tableId;
  RID rid;
  tableId = getTableIdForTable(tableName, rid);
  IndexManager *ixManager = IndexManager::instance();
  string fileName = tableName + "_" + attributeName + "_" + "idx";
  ixManager->createFile(fileName);

  // index all the existing records
  vector<string> attributeNames;
  attributeNames.push_back(attributeName);

  RM_ScanIterator rm_ScanIterator;
  IXFileHandle ixFileHandle;
  if (ixManager->openFile(fileName, ixFileHandle) != 0) {
    return -1;
  }

  scan(tableName, "", NO_OP, NULL, attributeNames, rm_ScanIterator);
  void *data = malloc(PAGE_SIZE);
  void *key = malloc(PAGE_SIZE);
  while (rm_ScanIterator.getNextTuple(rid, data) != -1) {
    // get key from data
    getKeyFromData(data, currAttribute, key);
    ixManager->insertEntry(ixFileHandle, currAttribute, key, rid);
  }

  if (ixManager->closeFile(ixFileHandle) != 0) {
    return -1;
  }

  free(data);
  free(key);

  // insert in the index catalog
  FileHandle indexFileHandle;
  rbfm->openFile(indexCatalog, indexFileHandle);
  RID indexRecId;
  RawRecordPreparer indexRecordPrp =
      RawRecordPreparer(indexTableRecordDescriptor);
  char *indexCatalogRecord = (char *)malloc(PAGE_SIZE);

  memset(indexCatalogRecord, 0, PAGE_SIZE);
  indexRecordPrp.setField(tableId)
      .setField(attributeName)
      .setField(fileName)
      .prepareRecord(indexCatalogRecord);
  rbfm->insertRecord(indexFileHandle, indexTableRecordDescriptor,
                     indexCatalogRecord, indexRecId);

  rbfm->closeFile(indexFileHandle);
  free(indexCatalogRecord);
  return 0;
}

RC RelationManager::destroyIndex(const string &tableName,
                                 const string &attributeName) {
  string fileName = tableName + "_" + attributeName + "_idx";

  // destroy file
  IndexManager *ixManager = IndexManager::instance();
  if (ixManager->destroyFile(fileName) != 0) {
    return -1;
  }

  // Remove from Index.tbl

  // form fileName in expected format byte array for scanning row in Index.tbl
  int length = fileName.length();
  void *fileNameBuf = malloc(length + sizeof(int));
  memcpy(fileNameBuf, &length, sizeof(int));
  memcpy((char *)fileNameBuf + sizeof(int), fileName.c_str(), length);

  vector<string> attributeNames;
  RM_ScanIterator rm_ScanIterator;
  scan(indexCatalog, "index-file-name", EQ_OP, fileNameBuf, attributeNames,
       rm_ScanIterator);

  RID indexRID;
  void *indexRow = malloc(PAGE_SIZE);
  if (rm_ScanIterator.getNextTuple(indexRID, indexRow) != 0) return -1;

  if (rbfm->deleteRecord(rm_ScanIterator.fileHandle, indexTableRecordDescriptor,
                         indexRID) != 0) {
    return -1;
  }

  if (rbfm->closeFile(rm_ScanIterator.fileHandle) != 0) {
    return -1;
  }

  free(indexRow);
  free(fileNameBuf);

  return 0;
}

RC RelationManager::indexScan(const string &tableName,
                              const string &attributeName, const void *lowKey,
                              const void *highKey, bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
  IndexManager *ixManager = IndexManager::instance();
  const string fileName = tableName + "_" + attributeName + "_" + "idx";

  if (ixManager->openFile(fileName, rm_IndexScanIterator.ixFileHandle) != 0) {
    return -1;
  }

  // Get Attribute from attribute name for ixscan
  Attribute currAttribute;
  vector<Attribute> attributes;
  getAttributes(tableName, attributes);

  for (unsigned i = 0; i < attributes.size(); i++) {
    if (attributes[i].name.compare(attributeName) == 0) {
      currAttribute = attributes[i];
      break;
    }
  }

  ixManager->scan(rm_IndexScanIterator.ixFileHandle, currAttribute, lowKey,
                  highKey, lowKeyInclusive, highKeyInclusive,
                  rm_IndexScanIterator.ixScanIterator);
  return 0;
}

RM_ScanIterator::RM_ScanIterator() {
  rm = RelationManager::instance();
  tableName = "";
  conditionAttribute = "";
  compOp = NO_OP;
  value = NULL;
  attributeNames = NULL;
}

RM_ScanIterator::~RM_ScanIterator() {}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
  if (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
    return success;
  } else {
    return RM_EOF;
  }
}

RC RM_ScanIterator::close() {
  tableName = "";
  conditionAttribute = "";
  compOp = NO_OP;
  value = NULL;
  attributeNames = NULL;
  rbfm->closeFile(fileHandle);
  return success;
}

RC RelationManager::getRecordDescriptorForTable(
    const string tableName, vector<Attribute> &recordDescriptor) {
  FileHandle fileHandle;
  rbfm->openFile(columnCatalog, fileHandle);
  string conditionAttribute = "table-id";
  CompOp compOp = EQ_OP;
  RID tableIdRId;
  const int value = getTableIdForTable(tableName, tableIdRId);

  if (value == 0) {
    rbfm->closeFile(fileHandle);
    return failure;
  }
  vector<string> attributeNames;
  attributeNames.push_back("column-name");
  attributeNames.push_back("column-type");
  attributeNames.push_back("column-length");
  RBFM_ScanIterator rbfm_ScanIterator;
  rbfm->scan(fileHandle, colRecordDescriptor, conditionAttribute, compOp,
             (void *)&value, attributeNames, rbfm_ScanIterator);

  RID rid;
  char *data = (char *)malloc(PAGE_SIZE);  // max record size
  memset(data, 0, PAGE_SIZE);
  while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
    Attribute attr;
    // | 1 NIA | 4 bytes varlen| varchar | 4 varlen | varchar | 4 byte int|
    int offset = 1;
    int strlength = 0;
    memcpy(&strlength, data + offset, sizeof(int));
    char *attributeName = (char *)malloc(strlength);
    memset(attributeName, 0, strlength);
    memcpy(attributeName, data + offset + sizeof(int), strlength);
    offset += sizeof(int) + strlength;

    int attributeType = 0;
    memcpy(&attributeType, data + offset, sizeof(int));
    offset += sizeof(int);

    int attributeLength = 0;
    memcpy(&attributeLength, data + offset, sizeof(int));
    offset += sizeof(int);

    attr.name.assign(attributeName, (unsigned)strlength);
    //		attr.name = attributeName;
    switch (attributeType) {
      case TypeInt:
        attr.type = TypeInt;
        break;
      case TypeReal:
        attr.type = TypeReal;
        break;
      case TypeVarChar:
        attr.type = TypeVarChar;
        break;
    }
    attr.length = (AttrLength)attributeLength;

    recordDescriptor.push_back(attr);
  }

  rbfm->closeFile(fileHandle);
  free(data);
  data = NULL;
  return success;
}

int RelationManager::getTableIdForTable(string tableName, RID &rid) {
  FileHandle fileHandle;
  rbfm->openFile(tableCatalog, fileHandle);
  //  cout << "File opened " << opened << endl;
  const string &conditionAttribute = "table-name";
  CompOp compOp = EQ_OP;
  char *value = (char *)malloc(PAGE_SIZE);
  memset(value, 0, PAGE_SIZE);

  int valueLength = tableName.length();
  memcpy(value, &valueLength, sizeof(valueLength));
  memcpy(value + sizeof(valueLength), tableName.c_str(), tableName.length());

  RBFM_ScanIterator rbfm_ScanIterator;
  vector<string> attributeNames;
  attributeNames.push_back("table-id");
  rbfm->scan(fileHandle, tblRecordDescriptor, conditionAttribute, compOp,
             (void *)value, attributeNames, rbfm_ScanIterator);

  int tableid = 0;
  rid = {0, 0};
  void *data = malloc(10);
  memset(data, 0, 10);
  unsigned char *nullIndicatorArray = (unsigned char *)malloc(1);
  memset(nullIndicatorArray, 0, 1);
  while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF) {
    memcpy(nullIndicatorArray, data, 1);
    if (isFieldNull(nullIndicatorArray, 0)) {
      cerr << "No such table" << tableName << endl;
      break;
    } else {
      // get table id
      memcpy(&tableid, (char *)data + 1, sizeof(tableid));
      break;
    }
  }
  RC closed = rbfm->closeFile(fileHandle);
  //  cout << "File closed " << closed << endl;
  free(data);
  data = NULL;
  return tableid;
}

void RelationManager::readCurrentTableID() {
  FileHandle fileHandle;
  rbfm->openFile(currentTableIDFile, fileHandle);

  char *data = (char *)malloc(5);
  memset(data, 0, 5);
  rbfm->readAttribute(fileHandle, currentTableIDRecordDescriptor, (RID){0, 0},
                      "table-id", data);

  // 1 byte for null indicator array
  memcpy(&current_table_id, data + 1, sizeof(current_table_id));

  rbfm->closeFile(fileHandle);
}

RM_IndexScanIterator::RM_IndexScanIterator() {}

RM_IndexScanIterator::~RM_IndexScanIterator() {}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
  if (this->ixScanIterator.getNextEntry(rid, key) == -1) {
    return -1;
  }
  return 0;
}

RC RM_IndexScanIterator::close() {
  if (this->ixScanIterator.close() != 0) {
    return -1;
  }
  return 0;
}

void getIndexAttribute(void *indexRowBuffer, void *attrName,
                       vector<Attribute> &indexRecDesc) {
  //  Record record = Record(indexRecDesc, (char *)indexRowBuffer);
  RawRecord record =
      RawRecord(static_cast<const char *>(indexRowBuffer), indexRecDesc);
  string indexAttrName = "index-name";
  Value attributeName = record.getAttributeValue(indexAttrName);
  int length;
  memcpy(&length, attributeName.data, sizeof(int));
  memset(attrName, 0, PAGE_SIZE);
  memcpy((char *)attrName, (char *)attributeName.data + sizeof(length),
         length);  // just copy string payload
}
