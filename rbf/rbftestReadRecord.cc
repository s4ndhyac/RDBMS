/*
 * UnitTest.cc
 *
 *  Created on: Oct 19, 2018
 *      Author: dantis
 */

#include <rbf/pfm.h>
#include <rbf/rbfm.h>
#include <rbf/test_util.h>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>

using namespace std;

bool isFieldNullTest(unsigned char *nullIndicatorArray, int fieldIndex)
{
  int byteNumber = fieldIndex / 8;
  bool isNull = nullIndicatorArray[byteNumber] & (1 << (7 - fieldIndex % 8));
  return isNull;
}

int UnitTest(RecordBasedFileManager *rbfm, int tableCount, int columnCount)
{
  // Functions tested
  // 1. Create Record-Based File
  // 2. Open Record-Based File
  // 3. Insert Record
  // 4. Read Record
  // 5. Close Record-Based File
  // 6. Destroy Record-Based File
  cout << endl
       << "***** In RBF Test Case 8 *****" << endl;

  RC rc;
  string maxIdFile = "CurrentTableID.tbl";
  string colFileName = "Columns.tbl";
  string tableFileName = "Tables.tbl";

  vector<Attribute> colRecordDesc;
  vector<Attribute> tblRecordDescriptor;
  vector<Attribute> maxIdRecordDesc;

  maxIdRecordDesc.push_back((Attribute){"table-id", TypeInt, 4});

  tblRecordDescriptor.push_back((Attribute){"table-type", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-id", TypeInt, 4});
  tblRecordDescriptor.push_back((Attribute){"table-name", TypeVarChar, 50});
  tblRecordDescriptor.push_back((Attribute){"file-name", TypeVarChar, 50});

  colRecordDesc.push_back((Attribute){"table-id", TypeInt, 4});
  colRecordDesc.push_back((Attribute){"column-name", TypeVarChar, 50});
  colRecordDesc.push_back((Attribute){"column-type", TypeInt, 4});
  colRecordDesc.push_back((Attribute){"column-length", TypeInt, 4});
  colRecordDesc.push_back((Attribute){"column-position", TypeInt, 4});

  // Open the file "test8"
  FileHandle colFileHandle;
  FileHandle tblFileHandle;
  FileHandle maxIdFileHandle;

  void *returnedData;
  RID rid;

  rc = rbfm->openFile(maxIdFile, maxIdFileHandle);
  assert(rc == success && "Opening the file should not fail.");
  rid = {0,0};
  returnedData = malloc(100);
  rc = rbfm->readRecord(maxIdFileHandle, maxIdRecordDesc, rid, returnedData);
  rbfm->printRecord(maxIdRecordDesc, returnedData);
  int maxTableId;
  memcpy(&maxTableId, (char*)returnedData+1, 4);
  rbfm->closeFile(maxIdFileHandle);
  cout << endl ;

 cout << "===============Tables.tbl========================" << endl;

  rc = rbfm->openFile(tableFileName, tblFileHandle);
  assert(rc == success && "Opening the file should not fail.");
  rid = {0,0};
  for (int i=0; i<tableCount; i++){
  rc = rbfm->readRecord(tblFileHandle, tblRecordDescriptor, rid, returnedData);
  if (rc == success){
  rbfm->printRecord(tblRecordDescriptor, returnedData);
  }
  rid.slotNum++; // this will break if more than one page
  cout << endl ;
  }
  rbfm->closeFile(maxIdFileHandle);

 cout << "===============columns.tbl========================" << endl;
  rc = rbfm->openFile(colFileName, colFileHandle);
  assert(rc == success && "Opening the file should not fail.");

  rid = {0,0};
  for (int i=0; i<columnCount; i++){
  rc = rbfm->readRecord(colFileHandle, colRecordDesc, rid, returnedData);
//  assert(rc == success && "Reading a record should not fail.");

  if (rc == success){
  rbfm->printRecord(colRecordDesc, returnedData);
  }
  cout << endl;
  rid.slotNum ++; // this will break if more than one page
  }

  // Compare whether the two memory blocks are the same
//  if (memcmp(record, returnedData, recordSize) != 0)
//  {
//    cout << "[FAIL] Test Case 8 Failed!" << endl
//         << endl;
//    free(record);
//    free(returnedData);
//    return -1;
//  }
//
  cout << endl;

  // Compare whether the string attribute is the same or not
//  char *attributeData = (char *)malloc(30);
//  memset(attributeData, 0, 30);
//  rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "EmpName", attributeData);
//  assert(rc == success && "reading attribute should not fail");
//
//  unsigned char nullIndicatorArray = attributeData[0];
//  if (!isFieldNullTest(&nullIndicatorArray, 0))
//  {
//    assert(strcmp(attributeData + 5, "Anteater") == 0 && "Anteater attribute should be same");
//    int lengthOfString = 0;
//    memcpy(&lengthOfString, attributeData + 1, sizeof(int));
//    assert(lengthOfString == strlen("Anteater") && "Read attribute should return the length of the string");
//  }
  // Close the file "test8"
  rc = rbfm->closeFile(colFileHandle);
  assert(rc == success && "Closing the file should not fail.");

//  // Destroy the file
//  rc = rbfm->destroyFile(fileName);
//  assert(rc == success && "Destroying the file should not fail.");
//
//  rc = destroyFileShouldSucceed(fileName);
//  assert(rc == success && "Destroying the file should not fail.");

//  free(record);
  free(returnedData);
  return -1;
}

int main(int argc, char* argv[])
{
  if (argc != 3){
	  cerr<< "need number of columns rids" << argc << endl;
	  exit(1);
  }
  std::istringstream iss( argv[1]);

  int nooftables;
  iss>>nooftables;

  std::istringstream iss2( argv[2]);
  int noofcol;
  iss2 >> noofcol;

  // To test the functionality of the record-based file manager
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  RC rcmain = UnitTest(rbfm, nooftables, noofcol);
  return rcmain;
}
