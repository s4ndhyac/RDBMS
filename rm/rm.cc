
#include "rm.h"

#include <stdlib.h>
#include <cstring>
#include <iostream>

#include "../rbf/pfm.h"

RC success = 0;
RC failure = 1;

enum TableType
{
  SYSTEM_TABLE = 0,
  USER_TABLE = 1
};

RelationManager *RelationManager::instance()
{
  static RelationManager _rm;
  return &_rm;
}

RelationManager::RelationManager()
{
  rbfm = RecordBasedFileManager::instance();

  tblRecordDescriptor.push_back((Attribute){"table-type", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-name", TypeVarChar, 50});
  tblRecordDescriptor.push_back((Attribute){"file-name", TypeVarChar, 50});

  // Describe schema for Columns catalog table
  // Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
  colRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-name", TypeVarChar, 50});
  colRecordDescriptor.push_back((Attribute){"column-type", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-length", TypeInt, 4});
  colRecordDescriptor.push_back((Attribute){"column-position", TypeInt, 4});

  currentTableIDRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
  FileHandle fileHandle;
  rbfm->createFile(tableCatalog);
  rbfm->openFile(tableCatalog, fileHandle);

  //  Prepare raw table record for insertion
  //	Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  RawRecordPreparer tblCtlgPrp = RawRecordPreparer(tblRecordDescriptor);
  char *tableCatalogRecord = (char*) malloc(PAGE_SIZE);
  memset(tableCatalogRecord, 0, PAGE_SIZE);
		  tblCtlgPrp
                                 .setField(SYSTEM_TABLE) //table-type
                                 .setField(1)            //table-id
                                 .setField("Tables")     //table-name
                                 .setField(tableCatalog) //file-name
                                 .prepareRecord(tableCatalogRecord);

  // insert first tableCatalog record for 'Tables' table
  RID rid;
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord, rid);
  free(tableCatalogRecord);
  //  Prepare raw table record for insertion
  //	Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
  char *tableCatalogRecord2 = (char*) malloc(PAGE_SIZE);
  memset(tableCatalogRecord2, 0 , PAGE_SIZE);
tblCtlgPrp
                                  .setField(SYSTEM_TABLE)  //table-type
                                  .setField(2)             //table-id
                                  .setField("Columns")     //table-name
                                  .setField(columnCatalog) //file-name
                                  .prepareRecord(tableCatalogRecord2);

  // insert second tableCatalog record for 'Tables' table
  RID rid2;
  rbfm->insertRecord(fileHandle, tblRecordDescriptor, tableCatalogRecord2, rid2);
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
  char *columnCatalogRecord = (char*) malloc( PAGE_SIZE);
  memset(columnCatalogRecord,0,PAGE_SIZE);
colCtlgPrp
                                  .setField(1)          // table-id
                                  .setField("table-id") // column-name
                                  .setField(TypeInt)    // column-type
                                  .setField(4)          // column-length
                                  .setField(1)          // column-position
                                  .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (1 , "table-name"      , TypeVarChar , 50 , 2)
  memset(columnCatalogRecord,0,PAGE_SIZE);
 colCtlgPrp
                            .setField(1)            // table-id
                            .setField("table-name") // column-name
                            .setField(TypeVarChar)  // column-type
                            .setField(50)           // column-length
                            .setField(2)            // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (1 , "file-name"       , TypeVarChar , 50 , 3)
	memset(columnCatalogRecord,0,PAGE_SIZE);
   colCtlgPrp
                            .setField(1)           // table-id
                            .setField("file-name") // column-name
                            .setField(TypeVarChar) // column-type
                            .setField(50)          // column-length
                            .setField(3)           // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "table-id"        , TypeInt     , 4  , 1)
	memset(columnCatalogRecord,0,PAGE_SIZE);
     colCtlgPrp
                            .setField(2)          // table-id
                            .setField("table-id") // column-name
                            .setField(TypeInt)    // column-type
                            .setField(4)          // column-length
                            .setField(1)          // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-name"     , TypeVarChar , 50 , 2)
	memset(columnCatalogRecord,0,PAGE_SIZE);
   colCtlgPrp
                            .setField(2)             // table-id
                            .setField("column-name") // column-name
                            .setField(TypeVarChar)   // column-type
                            .setField(50)            // column-length
                            .setField(2)             // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-type"     , TypeInt     , 4  , 3)
	memset(columnCatalogRecord,0,PAGE_SIZE);
   colCtlgPrp
                            .setField(2)             // table-id
                            .setField("column-type") // column-name
                            .setField(TypeInt)       // column-type
                            .setField(4)             // column-length
                            .setField(3)             // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-length"   , TypeInt     , 4  , 4)
	memset(columnCatalogRecord,0,PAGE_SIZE);
   colCtlgPrp
                            .setField(2)               // table-id
                            .setField("column-length") // column-name
                            .setField(TypeInt)         // column-type
                            .setField(4)               // column-length
                            .setField(4)               // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);

  //	 (2 , "column-position" , TypeInt     , 4  , 5)
	memset(columnCatalogRecord,0,PAGE_SIZE);
   colCtlgPrp
                            .setField(2)                 // table-id
                            .setField("column-position") // column-name
                            .setField(TypeInt)           // column-type
                            .setField(4)                 // column-length
                            .setField(5)                 // column-position
                            .prepareRecord(columnCatalogRecord);
  rbfm->insertRecord(fileHandleForCols, colRecordDescriptor, columnCatalogRecord, rid);
  rbfm->closeFile(fileHandleForCols);
  free(columnCatalogRecord);

  current_table_id = 3;
  rbfm->createFile(currentTableIDFile);
  FileHandle tableIDFileHandle;
  rbfm->openFile(currentTableIDFile, tableIDFileHandle);
  RID tableIDRID;
  RawRecordPreparer maxIdRecordPrp = RawRecordPreparer(currentTableIDRecordDescriptor);
  char *maxIDRecord = (char*) malloc(PAGE_SIZE);
  memset(maxIDRecord,0,PAGE_SIZE);
  maxIdRecordPrp.setField(current_table_id).prepareRecord(maxIDRecord);
  rbfm->insertRecord(tableIDFileHandle, currentTableIDRecordDescriptor, maxIDRecord, tableIDRID);
  rbfm->closeFile(tableIDFileHandle);
  free(maxIDRecord);

  return success;
}

void RelationManager::persistCurrentTableId()
{
  FileHandle fileHandle;
  rbfm->openFile(currentTableIDFile, fileHandle);
  RID rid = {0, 0};
  RawRecordPreparer maxIdRecordPrp = RawRecordPreparer(currentTableIDRecordDescriptor);
  char *maxIDRecord = (char*) malloc(PAGE_SIZE);
  memset(maxIDRecord,0,PAGE_SIZE);
  maxIdRecordPrp.setField(current_table_id).prepareRecord(maxIDRecord);
  rbfm->updateRecord(fileHandle, currentTableIDRecordDescriptor, maxIDRecord, rid);
  free(maxIDRecord);

  rbfm->closeFile(fileHandle);
}

bool isSystemTable(const string &tableName)
{
  if (tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0)
    return true;
  return false;
}

RC RelationManager::deleteCatalog()
{
  if (rbfm->destroyFile(tableCatalog) != 0)
  {
    return -1;
  }

  if (rbfm->destroyFile(columnCatalog) != 0)
  {
    return -1;
  }

  if (rbfm->destroyFile(currentTableIDFile) != 0)
  {
    current_table_id = 1;
    return -1;
  }
  return success;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

  const string fileName = tableName + ".tbl";
  FileHandle fileHandle;
  int status = rbfm->createFile(fileName);

  if (status != success){
	//  cout << "Create table failed for table " << tableName << endl;
	 // cout << "File already exists" << endl;
	  return status;
  }

  // insert tuple in table catalog
  readCurrentTableID();
  rbfm->openFile(tableCatalog, fileHandle);
  RawRecordPreparer tblRecordPrp = RawRecordPreparer(tblRecordDescriptor);
  RID rid;
  char *tableCatalogRecord = (char*) malloc(PAGE_SIZE);
  memset(tableCatalogRecord,0,PAGE_SIZE);
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
  colCatalogRecord = (char*)malloc(PAGE_SIZE);
  int colPosition = 1;
  for (Attribute attr : attrs)
  {
	  memset(colCatalogRecord, 0 , PAGE_SIZE);
    colRecordPrp
                           .setField(current_table_id)
                           .setField(attr.name)
                           .setField(attr.type)
                           .setField(attr.length)
                           .setField(colPosition++)
                           .prepareRecord(colCatalogRecord);
    rbfm->insertRecord(colFileHandle, colRecordDescriptor, colCatalogRecord, rid);
  }
    free(colCatalogRecord);

  current_table_id++;
  persistCurrentTableId();

  return success;
}

RC RelationManager::deleteTable(const string &tableName)
{
  if (isSystemTable(tableName))
    return -1;

  RID tableIdRID;
  const int tableId = getTableIdForTable(tableName, tableIdRID);

  if (tableId == 0){
	  return -1;
  }
  // delete tables from table catalog
  FileHandle fileHandle;

  if (rbfm->openFile(tableCatalog, fileHandle) != 0) //TODO: Confirm is .tbl is required
    return -1;

  if (rbfm->deleteRecord(fileHandle, tblRecordDescriptor, tableIdRID) != 0)
    return -1;

  if (rbfm->closeFile(fileHandle) != 0)
    return -1;

  vector<string> attrNames;
  attrNames.push_back("table-id");

  RBFM_ScanIterator rbfmsi;

  // delete records from column catalog
  FileHandle colFileHandle;
  if (rbfm->openFile(columnCatalog, colFileHandle) != 0) //TODO: Confirm is .tbl is required
    return -1;

  rbfm->scan(colFileHandle, colRecordDescriptor, "table-id", EQ_OP, &tableId, attrNames, rbfmsi);

  void *buffer = malloc(PAGE_SIZE);
  RID rid;
  vector<RID> ridsToDelete;
  while (rbfmsi.getNextRecord(rid, buffer) != RBFM_EOF)
  {

	  ridsToDelete.push_back(rid);
  }
  for (RID ridToDelete: ridsToDelete){

    if (rbfm->deleteRecord(colFileHandle, colRecordDescriptor, ridToDelete) != 0)
      return -1;
  }

  if (rbfm->closeFile(colFileHandle) != 0)
    return -1;


  // remove the file to destroy
  string filename = tableName + ".tbl";
  if (rbfm->destroyFile(filename) != 0)
    return -1;

  return success;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  RC status = getRecordDescriptorForTable(tableName, attrs);
  return status;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if (isSystemTable(tableName))
		return false;
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  RC rc = getRecordDescriptorForTable(tableName, recordDescriptor);
  if (rc == failure) // if the table does not exist
	  return rc;
  rbfm->openFile(fileName, fileHandle);
  RC status = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if (isSystemTable(tableName))
		return false;
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);
  rbfm->openFile(fileName, fileHandle);
  RC status = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if (isSystemTable(tableName))
		return false;
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);
  rbfm->openFile(fileName, fileHandle);
  RC status = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";
  vector<Attribute> recordDescriptor;
  int tableExists = getRecordDescriptorForTable(tableName, recordDescriptor);
  if (tableExists == failure)
	  return failure;
  rbfm->openFile(fileName, fileHandle);
  RC status = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  RC status = rbfm->printRecord(attrs, data);
  return status;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  FileHandle fileHandle;
  string fileName = tableName + ".tbl";

  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);

  if (rbfm->openFile(fileName, fileHandle) == failure)
    return failure;

  RC status = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
  rbfm->closeFile(fileHandle);
  return status;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
  rm_ScanIterator.tableName = tableName;
  rm_ScanIterator.conditionAttribute = conditionAttribute;
  rm_ScanIterator.compOp = compOp;
  rm_ScanIterator.value = value;
  rm_ScanIterator.attributeNames = &attributeNames;
  rbfm->openFile(tableName + ".tbl", rm_ScanIterator.fileHandle);
  vector<Attribute> recordDescriptor;
  getRecordDescriptorForTable(tableName, recordDescriptor);
  //  rbfm->scan(fileHandle, recordDescriptor,
  //		  conditionAttribute, compOp, rbfmScanner value, attributeNames,
  //		  rm_ScanIterator.rbfm_ScanIterator);
  rm_ScanIterator.recordDescriptor = recordDescriptor;
  rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor,
             conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
  return success;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
  return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
  return -1;
}

RM_ScanIterator::RM_ScanIterator()
{

  rm = RelationManager::instance();
  tableName = "";
  conditionAttribute = "";
  compOp = NO_OP;
  value = NULL;
  attributeNames = NULL;
}

RM_ScanIterator::~RM_ScanIterator()
{
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{

  if (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
  {
    return success;
  }
  else
  {
    return RM_EOF;
  }
}

RC RM_ScanIterator::close()
{
  tableName = "";
  conditionAttribute = "";
  compOp = NO_OP;
  value = NULL;
  attributeNames = NULL;
  rbfm->closeFile(fileHandle);
  return success;
}

RC RelationManager::getRecordDescriptorForTable(const string tableName, vector<Attribute> &recordDescriptor)
{

  FileHandle fileHandle;
  rbfm->openFile(columnCatalog, fileHandle);
  string conditionAttribute = "table-id";
  CompOp compOp = EQ_OP;
  RID tableIdRId;
  const int value = getTableIdForTable(tableName, tableIdRId);

  if (value == 0)
  {
    rbfm->closeFile(fileHandle);
    return failure;
  }
  vector<string> attributeNames;
  attributeNames.push_back("column-name");
  attributeNames.push_back("column-type");
  attributeNames.push_back("column-length");
  RBFM_ScanIterator rbfm_ScanIterator;
  rbfm->scan(fileHandle, colRecordDescriptor, conditionAttribute,
             compOp, (void *)&value, attributeNames, rbfm_ScanIterator);

  RID rid;
  char *data = (char *)malloc(PAGE_SIZE); // max record size
  memset(data, 0, PAGE_SIZE);
  while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
  {
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
    switch (attributeType)
    {
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

int RelationManager::getTableIdForTable(std::string tableName, RID &rid)
{
  FileHandle fileHandle;
  rbfm->openFile(tableCatalog, fileHandle);

  const string &conditionAttribute = "table-name";
  CompOp compOp = EQ_OP;
  char *value = (char *)malloc(4 + tableName.length());
  memset(value,0, 4 + tableName.length());

  int valueLength = tableName.length();
  memcpy(value, &valueLength, 4);
  memcpy(value + 4, tableName.c_str(), tableName.length());

  RBFM_ScanIterator rbfm_ScanIterator;
  vector<string> attributeNames;
  attributeNames.push_back("table-id");
  rbfm->scan(fileHandle, tblRecordDescriptor, conditionAttribute, compOp,
             (void *)value, attributeNames, rbfm_ScanIterator);

  int tableid = 0;
  rid = {0, 0};
  void *data = malloc(10);
  memset(data,0,10);
  unsigned char *nullIndicatorArray = (unsigned char *)malloc(1);
  memset(nullIndicatorArray,0,1);
  while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
  {
    memcpy(nullIndicatorArray, data, 1);
    if (isFieldNull(nullIndicatorArray, 0))
    {
      cerr << "No such table" << tableName
           << endl;
      break;
    }
    else
    {
      // get table id
      memcpy(&tableid, (char *)data + 1, sizeof(tableid));
      break;
    }
  }
  rbfm->closeFile(fileHandle);
  free(data);
  data = NULL;
  return tableid;
}

void RelationManager::readCurrentTableID()
{
  FileHandle fileHandle;
  rbfm->openFile(currentTableIDFile, fileHandle);

  char *data = (char *)malloc(5);
  memset(data, 0, 5);
  rbfm->readAttribute(fileHandle, currentTableIDRecordDescriptor,
                      (RID){0, 0}, "table-id", data);

  // 1 byte for null indicator array
  memcpy(&current_table_id, data + 1, sizeof(current_table_id));

  rbfm->closeFile(fileHandle);
}
