
#include "qe.h"

#include <stddef.h>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <vector>

#include "../rbf/pfm.h"

Filter::Filter(Iterator* input, const Condition& condition) {
  this->input = input;
  this->condition = condition;
  getAttributes(attributes);
  this->cEval = new ConditionEvaluator(condition, attributes);
}

RC Filter::getNextTuple(void* data) {
  RC isEOF = false;
  while (true) {
    isEOF = input->getNextTuple(data);
    RawRecord dataRecord((const char*)data, attributes);
    if (isEOF != QE_EOF) {
      if (cEval->evaluateFor(dataRecord)) {
        return (isEOF);
      }
    } else {
      return (isEOF);
    }
  }
}

void Filter::getAttributes(vector<Attribute>& attrs) const {
  input->getAttributes(attrs);
}

ConditionEvaluator::ConditionEvaluator(const Condition& condition,
                                       vector<Attribute>& attrs) {
  this->condition = condition;
  this->attributes = attrs;
}

bool ConditionEvaluator::evaluateFor(RawRecord& rawRecord) {
  CompOp compOp = condition.op;
  char* value;
  Value lhsValue = rawRecord.getAttributeValue(condition.lhsAttr);

  if (condition.bRhsIsAttr) {
    value = (char*)rawRecord.getAttributeValue(condition.rhsAttr).data;
  } else {
    value = (char*)condition.rhsValue.data;
  }
  return (CheckCondition(lhsValue.type, (char*)lhsValue.data, value, compOp));
}

Project::Project(Iterator* input, const vector<string>& attrNames) {
  this->input = input;
  attributeNames = attrNames;
}

Project::~Project() {}

RC Project::getNextTuple(void* data) {
  char* unprojectedData = new char[PAGE_SIZE]();
  RC isEOF = input->getNextTuple(unprojectedData);
  if (isEOF != 0) {
    delete[] unprojectedData;
    return (isEOF);
  }

  vector<Attribute> attrs;
  input->getAttributes(attrs);
  RawRecord dataRecord(unprojectedData, attrs);

  //	getSizeForNullIndicator((int)(this->attributeNames.size()));
  //	int niaSize = ceil(this->attributeNames.size()/8.0);
  int niaSize = getSizeForNullIndicator((int)(this->attributeNames.size()));
  int offset = niaSize;
  unsigned char* nullIndicator = new unsigned char[niaSize]();
  for (size_t i = 0; i < attributeNames.size(); i++) {
    Value value = dataRecord.getAttributeValue(attributeNames[i]);
    if (value.data == nullptr) {
      makeFieldNull(nullIndicator, i);
      continue;
    }
    switch (value.type) {
      case TypeVarChar:
        int length;
        memcpy(&length, value.data, sizeof(int));
        memcpy((char*)data + offset, value.data, sizeof(int) + length);
        offset += sizeof(int) + length;
        break;
      default:
        memcpy((char*)data + offset, value.data, sizeof(int));
        offset += sizeof(int);
        break;
    }
  }
  memcpy(data, nullIndicator, niaSize);
  delete[] unprojectedData;
  delete[] nullIndicator;
  return (isEOF);
}

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute>& attrs) const {
  vector<Attribute> unprojectedAttrs;
  input->getAttributes(unprojectedAttrs);

  for (string attrName : attributeNames) {
    for (Attribute attr : unprojectedAttrs) {
      if (attrName.compare(attr.name) == 0) attrs.push_back(attr);
    }
  }
}

BNLJoin::BNLJoin(
    Iterator* leftIn,            // Iterator of input R
    TableScan* rightIn,          // TableScan Iterator of input S
    const Condition& condition,  // Join condition
    const unsigned numPages      // # of pages that can be loaded into memory,
    //   i.e., memory block size (decided by the optimizer)
) {
  this->leftIn = leftIn;
  this->rightIn = rightIn;
  this->condition = condition;
  this->numPages = numPages;
  this->leftTableBufferOffset = 0;
  this->leftTableBuffer = new char[numPages * PAGE_SIZE]();
  this->rightTupleBuffer = new char[PAGE_SIZE]();
  // set attributes. call getAttributes only after setting left,right attrs
  this->leftIn->getAttributes(this->leftInAttributes);
  this->rightIn->getAttributes(this->rightInAttributes);
  this->getAttributes(joinedAttributes);
  // setup condition Evaluator
  this->cEval = new ConditionEvaluator(this->condition, joinedAttributes);
  //  loadNextBlockInMemory();
  joinedRecBuffer = new char[MAX_JOINED_RECORD_SIZE]();
  //  RC isLeftEmpty = loadNextBlockInMemory();
  //  RC isRightEmpty = rightIn->getNextTuple(rightTupleBuffer);
  //  tableEmpty = (isLeftEmpty == QE_EOF) || (isRightEmpty == QE_EOF);
};

BNLJoin::~BNLJoin() {
  if (leftTableBuffer) {
    delete[] leftTableBuffer;
    leftTableBuffer = nullptr;
  }
  if (rightTupleBuffer) {
    delete[] rightTupleBuffer;
    rightTupleBuffer = nullptr;
  }
  if (cEval) {
    delete cEval;
    cEval = nullptr;
  }
  if (joinedRecBuffer) {
    delete[] joinedRecBuffer;
    joinedRecBuffer = nullptr;
  }
}

void BNLJoin::getAttributes(vector<Attribute>& attrs) const {
  // concatenation of left and right attributes
  attrs.insert(attrs.begin(), leftInAttributes.begin(), leftInAttributes.end());
  attrs.insert(attrs.end(), rightInAttributes.begin(), rightInAttributes.end());
}

/**
 *
 * @param leftTupleBuf : set the pointer to the next buffer offset if the offset
 * is valid. Else set it to nullptr.
 * @return false if you've reached the end of left buf, true otherwise
 */
bool BNLJoin::getNextLeftRecord(const char*& leftTupleBuf) {
  if (matchingLeftRecords.empty()) {
    leftTupleBuf = nullptr;
    return (false);
  }

  leftTupleBuf = matchingLeftRecords.back();
  RawRecord currentRecord(leftTupleBuf, leftInAttributes);
  //  leftTableBufferOffset += currentRecord.getRecordSize();
  matchingLeftRecords.pop_back();
  return (true);
}

RC BNLJoin::setState() {
  if (!matchingLeftRecords.empty()) {
    leftTuplePointer = matchingLeftRecords.back();
    matchingLeftRecords.pop_back();
    return (success);
  }

  if (hashTable.empty() | rightIn->getNextTuple(rightTupleBuffer) == QE_EOF) {
    rightIn->setIterator();
    rightIn->getNextTuple(rightTupleBuffer);
    bool loadSuccess = loadNextBlockInMemory() != QE_EOF;
    if (!loadSuccess) return QE_EOF;
  }

  map<Value, vector<char*>>::iterator it;
  do {
    RawRecord rightRecord(rightTupleBuffer, rightInAttributes);
    Value joinKey = rightRecord.getAttributeValue(condition.rhsAttr);
    it = hashTable.find(joinKey);
  } while (it == hashTable.end() &&
           rightIn->getNextTuple(rightTupleBuffer) != QE_EOF);

  if (it != hashTable.end()) {
    matchingLeftRecords = it->second;
    leftTuplePointer = matchingLeftRecords.back();
    matchingLeftRecords.pop_back();
    return (success);
  }
  setState();  // else do the same thing for the next left block
}

/**
 *
 * @param data : buffer to fill with tuple that satisfies the condition
 * @return 0 if the data points to a valid tuple, return -1 if no more records
 * satisfy the condition
 */
RC BNLJoin::getNextTuple(void* data) {
  // create a joined record buffer

  //  while (true) {
  RC status = setState();  // set the left and right tuple

  if (status == QE_EOF) {
    return QE_EOF;
  }

  // create raw record object to get record sizes
  RawRecord leftRecord(leftTuplePointer, leftInAttributes);
  RawRecord rightRecord(rightTupleBuffer, rightInAttributes);

  //    // get record sizes for left and right records
  //    size_t sizeOfLeftRecord = leftRecord.getRecordSize();
  //    size_t sizeOfRightRecord = rightRecord.getRecordSize();

  // prepare a joined record buffer by copying left and right records
  memset(joinedRecBuffer, 0, MAX_JOINED_RECORD_SIZE);
  joinRecords(leftRecord, rightRecord, joinedRecBuffer, joinedAttributes);
  RawRecord joinedRecord(joinedRecBuffer, joinedAttributes);

  //    // Evaluate the condition on this join
  //    if (cEval->evaluateFor(joinedRecord)) {
  //      // if the join satisfies the condition then copy the joined record
  //      // to data buffer and return status success
  memcpy(data, joinedRecord.getBuffer(), joinedRecord.getRecordSize());
  //      break;
  //    }
  //  }

  return (success);
}

void joinRecords(const RawRecord& leftRec, const RawRecord& rightRec,
                 char* joinedRecBuf, vector<Attribute> joinedAttrs) {
  //  vector<Attribute> joinedAttrs;
  //  this->getAttributes(joinedAttrs);

  int leftNIASize = leftRec.getNullIndicatorSize();
  int leftPayloadSize = leftRec.getRecordSize() - leftNIASize;
  const unsigned char* leftNIA = leftRec.getNullIndicatorArray();

  int rightNIASize = rightRec.getNullIndicatorSize();
  int rightPayloadSize = rightRec.getRecordSize() - rightNIASize;
  const unsigned char* rightNIA = rightRec.getNullIndicatorArray();

  int joinedNIASize = getSizeForNullIndicator((int)(joinedAttrs.size()));
  unsigned char* joinedNIA = new unsigned char[joinedNIASize]();

  for (size_t i = 0; i < joinedAttrs.size(); ++i) {
    if (i < leftRec.getAttributes().size()) {
      joinedNIA[i / CHAR_BIT] |=
          (leftNIA[i / CHAR_BIT] & (1 << (7 - (i % CHAR_BIT))));

    } else {
      int rightNIAIndex = i - leftRec.getAttributes().size();
      joinedNIA[i / CHAR_BIT] |= (rightNIA[rightNIAIndex / CHAR_BIT] &
                                  (1 << (7 - (rightNIAIndex % CHAR_BIT))));
    }
  }

  // copy the joinedNIA
  memcpy(joinedRecBuf, joinedNIA, joinedNIASize);
  int joinedBufOffset = joinedNIASize;

  // copy the left payload
  memcpy(joinedRecBuf + joinedBufOffset, leftRec.getBuffer() + leftNIASize,
         leftPayloadSize);
  joinedBufOffset += leftPayloadSize;

  // copy the right payload
  memcpy(joinedRecBuf + joinedBufOffset, rightRec.getBuffer() + rightNIASize,
         rightPayloadSize);

  delete[] joinedNIA;
}

RC BNLJoin::loadNextBlockInMemory() {
  hashTable.clear();
  memset(leftTableBuffer, 0, PAGE_SIZE * numPages);
  size_t lTblBuffOffset = 0;
  RC Left_EOF = 0;
  char* leftRecordBuffer = new char[PAGE_SIZE]();
  while (lTblBuffOffset < numPages * PAGE_SIZE) {
    Left_EOF = leftIn->getNextTuple(leftRecordBuffer);
    if (Left_EOF == QE_EOF) break;
    RawRecord tempLeftRec(leftRecordBuffer, leftInAttributes);
    size_t sizeOfRecord = tempLeftRec.getRecordSize();

    memcpy(this->leftTableBuffer + lTblBuffOffset, leftRecordBuffer,
           sizeOfRecord);
    // leftRecord will die at end of iteration.
    // Therefore getAttributeValue returns a copy of value
    RawRecord leftRecord(leftTableBuffer + lTblBuffOffset, leftInAttributes);
    // value ultimately points to leftTableBuffer. So we need to maintain
    // its lifetime
    Value joinAttrKey = leftRecord.getAttributeValue(condition.lhsAttr);
    hashTable[joinAttrKey].push_back(this->leftTableBuffer + lTblBuffOffset);
    lTblBuffOffset += sizeOfRecord;
  }
  sizeOfLeftBuffer = lTblBuffOffset;  // set the size of the left buffer
  leftTableBufferOffset = 0;          // reset the left table buffer offset
  delete[] leftRecordBuffer;
  if (Left_EOF == QE_EOF && lTblBuffOffset == 0)  // we do not want to send
  //	EOF if there are new records read into the left memory buffer
  {
    return QE_EOF;
  } else {
    return (success);
  }
}

void INLJoin::getAttributes(vector<Attribute>& attrs) const {
  vector<Attribute> lAttr, rAttr;
  leftIn->getAttributes(lAttr);
  rightIn->getAttributes(rAttr);
  attrs.insert(attrs.begin(), lAttr.begin(), lAttr.end());
  attrs.insert(attrs.end(), rAttr.begin(), rAttr.end());
}

RC INLJoin::getNextTuple(void* data) {
  RC isEOF = setState();

  if (isEOF == QE_EOF) {
    return QE_EOF;
  }

  RawRecord leftRec(leftTuple, lAttr);
  RawRecord rightRec(rightTuple, rAttr);
  memset(joinedRecBuf, 0, MAX_JOINED_RECORD_SIZE);
  joinRecords(leftRec, rightRec, joinedRecBuf, joinedAttributes);

  RawRecord joinedRec(joinedRecBuf, joinedAttributes);

  memcpy(data, joinedRec.getBuffer(), joinedRec.getRecordSize());

  return success;
}

RC INLJoin::setState() {
  do {
    while (rightIn->getNextTuple(rightTuple) != QE_EOF) {
      return success;
    }
  } while (leftIn->getNextTuple(leftTuple) != QE_EOF &&
           resetRightIterator() == success);

  return QE_EOF;
}

RC INLJoin::resetRightIterator() {
  memset(lowKey, 0, PAGE_SIZE);
  memset(highKey, 0, PAGE_SIZE);
  bool lowKeyInclusive, highKeyInclusive;

  RawRecord leftRec(leftTuple, lAttr);
  //  RawRecord rightRec(rightTuple, rAttr);

  Value leftValue = leftRec.getAttributeValue(condition.lhsAttr);
  //  Value leftValue = condition.bRhsIsAttr
  //                        ? rightRec.getAttributeValue(condition.rhsAttr)
  //                        : condition.rhsValue;

  // you will always have the left tuple with you
  // use that to query the index of the right table
  // so the below conditions need to be applied on lhsAttr and should
  // be seen from the right table's point of view

  switch (condition.op) {
    case EQ_OP:
      memcpy(lowKey, leftValue.data, leftValue.size);
      memcpy(highKey, leftValue.data, leftValue.size);
      lowKeyInclusive = true;
      highKeyInclusive = true;
      break;
    case NO_OP:
      delete[] lowKey;
      delete[] highKey;
      lowKey = nullptr;
      highKey = nullptr;
      lowKeyInclusive = true;
      highKeyInclusive = true;
      break;
    case LT_OP:  // lhsAttr < rhsAttr; think like nullptr > rhsAttr > lhsAttr
      delete[] highKey;
      highKey = nullptr;
      memcpy(lowKey, leftValue.data, leftValue.size);
      lowKeyInclusive = false;
      highKeyInclusive = false;
      break;
    case LE_OP:  // lshAttr <= rhsAttr; think like nullptr > rhsAttr >= lhsAttr
      delete[] highKey;
      highKey = nullptr;
      memcpy(lowKey, leftValue.data, leftValue.size);
      lowKeyInclusive = true;
      highKeyInclusive = false;
      break;
    case GT_OP:  // lhsAttr > rhsAttr; think like nullptr < rhsAttr < lhsAttr
      memcpy(highKey, leftValue.data, leftValue.size);
      delete[] lowKey;
      lowKey = nullptr;
      lowKeyInclusive = false;
      highKeyInclusive = false;
      break;
    case GE_OP:  // lhsAttr >= rhsAttr; think like nullptr < rhsAttr <= lhsAttr
      memcpy(highKey, leftValue.data, leftValue.size);
      delete[] lowKey;
      lowKey = nullptr;
      lowKeyInclusive = false;
      highKeyInclusive = true;
      break;  // >=
    default:
      cerr << "Invalid use of operator for index scanning" << condition.op
           << endl;
      exit(1);
      break;
  }

  rightIn->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);

  //  void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive,
  //                   bool highKeyInclusive) {
  return success;
}

INLJoin::INLJoin(Iterator* leftIn, IndexScan* rightIn,
                 const Condition& condition) {
  // read left iterator and attributes
  this->leftIn = leftIn;
  this->leftIn->getAttributes(this->lAttr);

  // read right iterator and attributes
  this->rightIn = rightIn;
  this->rightIn->getAttributes(this->rAttr);

  // join the attributes after reading both the attributes
  // leftIn and rightIn need to be initialized before call to this
  this->getAttributes(this->joinedAttributes);
  this->condition = condition;

  // allocate memory for right and left tuple
  this->rightTuple = new char[PAGE_SIZE]();
  this->leftTuple = new char[PAGE_SIZE]();

  // lastly initialize condition evaluator with joinedAttributes
  this->cEval = new ConditionEvaluator(this->condition, joinedAttributes);

  lowKey = new char[PAGE_SIZE]();
  highKey = new char[PAGE_SIZE]();

  joinedRecBuf = new char[MAX_JOINED_RECORD_SIZE]();

  // prepare initial condition for getNextTuple
  isLeftTableEmpty = (leftIn->getNextTuple(leftTuple) == QE_EOF);
  if (!isLeftTableEmpty) resetRightIterator();
}

INLJoin::~INLJoin() {
  if (cEval) {
    delete cEval;
    cEval = nullptr;
  }
  if (leftTuple) {
    delete[] leftTuple;
    leftTuple = nullptr;
  }
  if (rightTuple) {
    delete[] rightTuple;
    rightTuple = nullptr;
  }
  delete[] lowKey;
  lowKey = nullptr;
  delete[] highKey;
  highKey = nullptr;
  delete[] joinedRecBuf;
  joinedRecBuf = nullptr;
}

ConditionEvaluator::~ConditionEvaluator() {}

// Aggregate

void aggregateFunctions(AggregateOp op, float& aggVal, float& val,
                        float& avgCount) {
  switch (op) {
    case MIN: {
      if (val < aggVal) aggVal = val;
      break;
    }
    case MAX: {
      if (val > aggVal) aggVal = val;
      break;
    }
    case COUNT: {
      aggVal += 1;
      break;
    }
    case SUM: {
      aggVal += val;
      break;
    }
    case AVG: {
      if (avgCount > 0) {
        aggVal = val / avgCount;
      } else
        cerr << "Error";
      break;
    }
  }
}

Aggregate::Aggregate(Iterator* input, Attribute aggAttr, AggregateOp op) {
  this->iterator = input;
  this->aggAttr = aggAttr;
  this->op = op;
  this->isGroupBy = false;
  this->getAttributes(this->aggAttrs);
}

Aggregate::Aggregate(Iterator* input, Attribute aggAttr, Attribute groupAttr,
                     AggregateOp op) {
  this->iterator = input;
  this->aggAttr = aggAttr;
  this->groupAttr = groupAttr;
  this->op = op;
  this->isGroupBy = true;
  this->getAttributes(this->aggAttrs);

  // initialize aggregate variables
  float count = 0;
  float sum = 0.0;
  float max = __FLT_MIN__;
  float min = __FLT_MAX__;
  float aggCount = 1.0f;

  char* tupleData = (char*)malloc(PAGE_SIZE);  // TODO: free this
  vector<Attribute> ittrAttrs;
  this->iterator->getAttributes(ittrAttrs);
  // iterate and get next tuple
  while (this->iterator->getNextTuple(tupleData) != QE_EOF) {
    RawRecord dataRecord(tupleData, ittrAttrs);

    // get the group by attribute value
    string groupByVal;
    Value gvalue = dataRecord.getAttributeValue(groupAttr);
    if (gvalue.type == TypeVarChar) {
      int l;
      memcpy(&l, gvalue.data, sizeof(int));
      char* groupByVarchar = (char*)malloc(l);
      memcpy(groupByVarchar, (char*)gvalue.data + sizeof(int), l);
      groupByVal.assign(groupByVarchar, l);
      free(groupByVarchar);
    } else if (gvalue.type == TypeInt) {
      int groupByInt;
      memcpy(&groupByInt, gvalue.data, sizeof(int));
      groupByVal = to_string(groupByInt);
    } else {
      float groupByFloat;
      memcpy(&groupByFloat, gvalue.data, sizeof(float));
      groupByVal = to_string(groupByFloat);
    }

    float attrValue;
    Value value = dataRecord.getAttributeValue(aggAttr);
    if (value.type == TypeInt) {
      int intAttrValue;
      memcpy(
          &intAttrValue, value.data,
          sizeof(int));  // TODO: value.data format does not have nullIndicator
      attrValue = (float)intAttrValue;
    } else if (value.type == TypeReal)
      memcpy(&attrValue, value.data, sizeof(float));

    if (groupedVals.count(groupByVal) == 0) {
      count = 0;
      sum = 0.0;
      max = __FLT_MIN__;
      min = __FLT_MAX__;
    } else {
      count = groupedVals[groupByVal];
      sum = groupedVals[groupByVal];
      max = groupedVals[groupByVal];
      min = groupedVals[groupByVal];
    }

    aggregateFunctions(COUNT, count, attrValue, aggCount);
    aggregateFunctions(SUM, sum, attrValue, aggCount);

    if (this->op == MAX)
      aggregateFunctions(MAX, max, attrValue, aggCount);
    else if (this->op == MIN)
      aggregateFunctions(MIN, min, attrValue, aggCount);

    if (op == COUNT)
      groupedVals[groupByVal] = count;
    else if (op == SUM)
      groupedVals[groupByVal] = sum;
    else if (op == MAX)
      groupedVals[groupByVal] = max;
    else if (op == MIN)
      groupedVals[groupByVal] = min;
  }

  this->nextGroupedVal = groupedVals.begin();
  free(tupleData);
}

void returnAggregateVal(AggregateOp& op, void* data, float& min, float& max,
                        float& count, float& sum, float& avg, bool isGroupBy,
                        string& groupByVal, const Attribute& groupByAttr) {
  int nullBytes = 1;
  void* nullByteBuffer = malloc(nullBytes);
  memset(nullByteBuffer, 0, nullBytes);

  int offset = 0;
  memcpy((char*)data + offset, nullByteBuffer, nullBytes);
  offset += nullBytes;

  if (isGroupBy) {
    if (groupByAttr.type == TypeVarChar) {
      int len = strlen(groupByVal.c_str());
      memcpy((char*)data + offset, &len, sizeof(int));
      offset += sizeof(int);
      memcpy((char*)data + offset, groupByVal.c_str(), len);
      offset += len;
    } else if (groupByAttr.type == TypeInt) {
      int ogi = stoi(groupByVal);
      memcpy((char*)data + offset, &ogi, sizeof(int));
      offset += sizeof(int);
    } else {
      float ogf = stof(groupByVal);
      memcpy((char*)data + offset, &ogf, sizeof(float));
      offset += sizeof(float);
    }
  }

  switch (op) {
    case MIN: {
      memcpy((char*)data + offset, &min, sizeof(float));
      break;
    }
    case MAX: {
      memcpy((char*)data + offset, &max, sizeof(float));
      break;
    }
    case COUNT: {
      memcpy((char*)data + offset, &count, sizeof(float));
      break;
    }
    case SUM: {
      memcpy((char*)data + offset, &sum, sizeof(float));
      break;
    }
    case AVG: {
      memcpy((char*)data + offset, &avg, sizeof(float));
      break;
    }
  }
}

RC Aggregate::getNextTuple(void* data) {
  // initialize aggregate variables
  float count = 0;
  float average = 0.0;
  float sum = 0.0;
  float max = __FLT_MIN__;
  float min = __FLT_MAX__;
  float aggCount = 1.0f;
  string groupByVal;
  bool isEOF = true;

  if (isGroupBy) {
    if (nextGroupedVal != groupedVals.end()) {
      isEOF = false;
      groupByVal = nextGroupedVal->first;
      if (op == COUNT)
        count = nextGroupedVal->second;
      else if (op == SUM)
        sum = nextGroupedVal->second;
      else if (op == MAX)
        max = nextGroupedVal->second;
      else if (op == MIN)
        min = nextGroupedVal->second;

      nextGroupedVal++;
    }
  } else {
    char* tupleData = (char*)malloc(PAGE_SIZE);  // TODO: free this
    vector<Attribute> ittrAttrs;
    this->iterator->getAttributes(ittrAttrs);
    // iterate and get next tuple

    while (this->iterator->getNextTuple(tupleData) != QE_EOF) {
      isEOF = false;
      // get attribute value from raw record
      // NOTE: Attribute can only be int or real
      // get attribute value from tuple format

      RawRecord dataRecord(tupleData, ittrAttrs);
      float attrValue;
      Value value = dataRecord.getAttributeValue(aggAttr);
      if (value.type == TypeInt) {
        int intAttrValue;
        memcpy(
            &intAttrValue, value.data,
            sizeof(
                int));  // TODO: value.data format does not have nullIndicator
        attrValue = (float)intAttrValue;
      } else if (value.type == TypeReal)
        memcpy(&attrValue, value.data, sizeof(float));

      // check and update the aggregate variables

      aggregateFunctions(COUNT, count, attrValue, aggCount);
      aggregateFunctions(SUM, sum, attrValue, aggCount);

      if (this->op == MAX)
        aggregateFunctions(MAX, max, attrValue, aggCount);
      else if (this->op == MIN)
        aggregateFunctions(MIN, min, attrValue, aggCount);
    }

    if (this->op == AVG) aggregateFunctions(AVG, average, sum, count);

    free(tupleData);
  }

  // return aggregate variables in required format
  if (!isEOF)
    returnAggregateVal(this->op, data, min, max, count, sum, average, isGroupBy,
                       groupByVal, groupAttr);

  return isEOF ? -1 : 0;
}

void Aggregate::getAttributes(vector<Attribute>& attrs) const {
  this->iterator->getAttributes(attrs);
}
