#ifndef _rbfm_h_
#define _rbfm_h_

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "pfm.h"

using namespace std;
typedef unsigned short int r_slot;
struct SlotDirectory {
  r_slot offset = 0;
  r_slot length = 0;
};

struct PageRecordInfo {
  r_slot numberOfSlots = 0;
  /**
   * @{code freeSpacePointer} points to the first free space position available
   * in the page
   *
   */
  r_slot freeSpacePos = 0;
};
// Record ID
typedef struct {
  unsigned pageNum;  // page number
  unsigned slotNum;  // slot number in the page
} RID;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
  string name;        // attribute name
  AttrType type;      // attribute type
  AttrLength length;  // attribute length
};

// rbfm.cc functions
void makeFieldNull(unsigned char *nullIndicatorArray, unsigned int fieldIndex);
bool isFieldNull(unsigned char *nullIndicatorArray, int fieldIndex);
r_slot getRecordDirectorySize(const PageRecordInfo &pri);
void getPageRecordInfo(PageRecordInfo &pageRecordInfo, char const *pageData);
void putPageRecordInfo(PageRecordInfo &pri, char *pageData);
RC updatePageRecordInfo(PageRecordInfo &pri, void *pageData);
r_slot getLengthOfRecordAndTransformRecord(
    const void *data, const vector<Attribute> &recordDescriptor, char *record);
SlotDirectory getSlotForRID(char const *pageData, RID rid, SlotDirectory &slot);
RC updateSlotDirectory(const RID &rid, void *pageData,
                       SlotDirectory &updatedSlot);

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
  EQ_OP = 0,  // no condition// =
  LT_OP,      // <
  LE_OP,      // <=
  GT_OP,      // >
  GE_OP,      // >=
  NE_OP,      // !=
  NO_OP       // no condition
} CompOp;

/********************************************************************************
 The scan iterator is NOT required to be implemented for the part 1 of the
 project
 ********************************************************************************/

#define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
 public:
  RBFM_ScanIterator();
  ~RBFM_ScanIterator(){};

  // Adding Data members required for iteration
  int isEOF;
  RID nextRID;

  FileHandle *fileHandle;
  vector<Attribute> recordDescriptor;
  string conditionAttribute;
  CompOp compOp;
  const void *value;
  vector<string> attributeNames;

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close();
};

class RecordBasedFileManager {
 public:
  static RecordBasedFileManager *instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field]
  //  [actual value for the second field] ... 1) For y fields, there is
  //  n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields =>
  //     ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.) Each bit represents
  //     whether each field value is null or not. If k-th bit from the left is
  //     set to 1, k-th field value is null. We do not include anything in the
  //     actual data part. If k-th bit from the left is set to 0, k-th field
  //     contains non-null values. If there are more than 8 fields, then you
  //     need to find the corresponding byte first, then find a corresponding
  //     bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store
  //     the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of
  //  readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle,
                  const vector<Attribute> &recordDescriptor, const void *data,
                  RID &rid);

  RC readRecord(FileHandle &fileHandle,
                const vector<Attribute> &recordDescriptor, const RID &rid,
                void *data);

  // This method will be mainly used for debugging/testing.
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

  /******************************************************************************************************************************************************************
         IMPORTANT, PLEASE READ: All methods below this comment (other than the
     constructor and destructor) are NOT required to be implemented for the part
     1 of the project
         ******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle,
                  const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle,
                  const vector<Attribute> &recordDescriptor, const void *data,
                  const RID &rid);

  RC readAttribute(FileHandle &fileHandle,
                   const vector<Attribute> &recordDescriptor, const RID &rid,
                   const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one
  // by one.
  RC scan(
      FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,  // comparision type such as "<" and "="
      const void *value,    // used in the comparison
      const vector<string> &attributeNames,  // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

 public:
 protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

 private:
  RID getInternalRID(const vector<Attribute> &recordDesc,
                     FileHandle &fileHandle, const RID &externalRID);
  char *readRecordInInternalFormat(FileHandle &fileHandle, const RID &rid);
  static RecordBasedFileManager *_rbf_manager;
  PagedFileManager *pfm;
};

/**
 * Interprets the record as stored in the heap file.
 * The record format used to understand the fields is as follows:<br>
 * <pre>
 *  ------------------------------------------
 *  | #F| TIA| F1PTR | F2PTR | NIA | F1 | F2 |
 *  ------------------------------------------
 *  </pre>
 *  Where   TIA - Tombstone indicator
 *  		#F  - No of fields
 *
 */
class Record {
 private:
  vector<Attribute> recordDescriptor;
  char *recordData;
  //  shared_ptr<char> recordData;
  r_slot recordSize;
  r_slot numberOfFields = 0;
  char tombstoneIndicator = 0;
  RID tombstoneRID;
  //  r_slot *fieldPointers = NULL;
  shared_ptr<r_slot> fieldPointers;
  //  char *inputData = NULL;
  shared_ptr<char> inputData;
  shared_ptr<unsigned char> nullIndicatorArray;
  r_slot sizeOfNullIndicatorArray = 0;

  void setNumberOfFields();

  void setTombstoneIndicator();

  void setTombstoneIndicatorPtr();

  void setFieldPointers();

  void setInputData();

  void setNullIndicatorArray();

  r_slot getRawRecordSize();

 public:
  Record(const vector<Attribute> &recordDesc, char *const dataOfStoredRecord);
  //	~Record(){
  //		delete rawData;
  //		delete recordData;
  //		rawData = NULL;
  //		recordData = NULL;
  //	}

  ~Record();
  r_slot getNumberOfFields();

  r_slot getLastFieldIndex();

  r_slot getFirstFieldIndex();

  bool getAttributeValue(const string &attributeName, char *attributeValue);

  bool getAttributeValue(r_slot fieldNumber, char *attributeValue);

  AttrType getAttributeType(const string &attributeName);

  r_slot getRecordSize();

  bool isTombstone();

  bool isFieldNull(r_slot fieldIndex);

  /**
   *  Gets the internal RID in a record tombstone. <br>
   *  If the record's Tombstone indicator is false,
   *  then this method will throw an error and terminate
   * @return
   */
  RID getTombstoneRID();
};

/**
 * Prepares the raw record given the fields.
 * It prepares the record for insertion
 * in the following format: <br>
 * <pre>
 * ------------------------------
 * |Null Indicator Array| Fields|
 * ------------------------------
 * </pre>
 */
class RawRecordPreparer {
 private:
  int recordDataOffset = 0;
  unsigned int fieldIndexCounter = 0;
  int currentRecordSize = 0;
  char *recordData = NULL;
  unsigned char *nullIndicatorArray = NULL;
  int nullIndicatorArraySize = 0;
  vector<Attribute> recordDescriptor;
  void resizeRecordDataIfNeeded(int size);
  bool isValidField();
  void resetCounters();

 public:
  RawRecordPreparer(const vector<Attribute> &recordDescriptor);
  ~RawRecordPreparer();
  void prepareRecord(char *recordToReturn);
  RawRecordPreparer &setField(const string &value);
  RawRecordPreparer &setField(int value);
  RawRecordPreparer &setField(AttrLength value);
  RawRecordPreparer &setField(float value);
  RawRecordPreparer &setNull();
  RawRecordPreparer &setRecordDescriptor(
      const vector<Attribute> &recordDescriptor);
};

bool CheckCondition(AttrType conditionAttributeType, char *attributeValue,
                    const void *value, CompOp compOp);
struct Value {
  AttrType type;  // type of value
  void *data;     // value
  size_t size = 0;
  bool operator<(const Value &o) const {
    if (data == nullptr && o.data == nullptr) return true;

    if (data == nullptr && o.data != nullptr) return false;

    if (data != nullptr && o.data == nullptr) return false;

    switch (type) {
      case TypeInt: {
        int *myInt = (int *)data;
        int *otherInt = (int *)o.data;
        return ((*myInt - *otherInt) < 0);
      }
      case TypeReal: {
        float *myFloat = (float *)data;
        float *otherFloat = (float *)o.data;
        return ((*myFloat - *otherFloat) < 0);
      }
      case TypeVarChar: {
        int myLength = size - sizeof(int);
        int oLength = o.size - sizeof(int);
        string myVarChar, otherVarChar;
        myVarChar.assign((char *)data + sizeof(myLength), myLength);
        otherVarChar.assign((char *)o.data + sizeof(oLength), oLength);
        return ((myVarChar.compare(otherVarChar)) < 0);
      }
      default:
        return false;
    }
  }
};

class RawRecord {
 public:
  RawRecord(const char *rawRecord, const vector<Attribute> &attrs);
  Value getAttributeValue(const string &attrName);
  Value getAttributeValue(Attribute &attr);
  Value getAttributeValue(int attrIndex);
  bool isFieldNull(int index);
  const unsigned char *getNullIndicatorArray() const;
  int getNullIndicatorSize() const;
  size_t getRecordSize() const;
  const char *getBuffer() const;
  const vector<Attribute> &getAttributes() const;
  ~RawRecord();

 private:
  vector<Value> attributeValue;
  const char *rawRecord;
  vector<Attribute> attributes;
  void setUpAttributeValue();
};
int getSizeForNullIndicator(int fieldCount);

#endif
