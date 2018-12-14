#ifndef _qe_h_
#define _qe_h_

#include <map>
#include <vector>

#include "../ix/ix.h"
#include "../rbf/rbfm.h"
#include "../rm/rm.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum { MIN = 0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

class ConditionEvaluator;
class RawRecord;
struct Condition;

struct Condition {
  string lhsAttr;   // left-hand side attribute
  CompOp op;        // comparison operator
  bool bRhsIsAttr;  // TRUE if right-hand side is an attribute and not a value;
                    // FALSE, otherwise.
  string rhsAttr;   // right-hand side attribute if bRhsIsAttr = TRUE
  Value rhsValue;   // right-hand side value if bRhsIsAttr = FALSE
};

class ConditionEvaluator {
 public:
  ConditionEvaluator(const Condition &condition, vector<Attribute> &attrs);
  bool evaluateFor(Record &record);
  bool evaluateFor(RawRecord &rawRecord);
  ~ConditionEvaluator();

 private:
  Condition condition;
  vector<Attribute> attributes;
};

class Iterator {
  // All the relational operators and access methods are iterators.
 public:
  virtual RC getNextTuple(void *data) = 0;
  virtual void getAttributes(vector<Attribute> &attrs) const = 0;
  virtual ~Iterator(){};
};

class TableScan : public Iterator {
  // A wrapper inheriting Iterator over RM_ScanIterator
 public:
  RelationManager &rm;
  RM_ScanIterator *iter;
  string tableName;
  vector<Attribute> attrs;
  vector<string> attrNames;
  RID rid;

  TableScan(RelationManager &rm, const string &tableName,
            const char *alias = NULL)
      : rm(rm) {
    // Set members
    this->tableName = tableName;

    // Get Attributes from RM
    rm.getAttributes(tableName, attrs);

    // Get Attribute Names from RM
    unsigned i;
    for (i = 0; i < attrs.size(); ++i) {
      // convert to char *
      attrNames.push_back(attrs.at(i).name);
    }

    // Call RM scan to get an iterator
    iter = new RM_ScanIterator();
    rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

    // Set alias
    if (alias) this->tableName = alias;
  };

  // Start a new iterator given the new compOp and value
  void setIterator() {
    iter->close();
    delete iter;
    iter = new RM_ScanIterator();
    rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
  };

  RC getNextTuple(void *data) { return iter->getNextTuple(rid, data); };

  void getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for (i = 0; i < attrs.size(); ++i) {
      string tmp = tableName;
      tmp += ".";
      tmp += attrs.at(i).name;
      attrs.at(i).name = tmp;
    }
  };

  ~TableScan() { iter->close(); };
};

class IndexScan : public Iterator {
  // A wrapper inheriting Iterator over IX_IndexScan
 public:
  RelationManager &rm;
  RM_IndexScanIterator *iter;
  string tableName;
  string attrName;
  vector<Attribute> attrs;
  char key[PAGE_SIZE];
  RID rid;

  IndexScan(RelationManager &rm, const string &tableName,
            const string &attrName, const char *alias = NULL)
      : rm(rm) {
    // Set members
    this->tableName = tableName;
    this->attrName = attrName;

    // Get Attributes from RM
    rm.getAttributes(tableName, attrs);

    // Call rm indexScan to get iterator
    iter = new RM_IndexScanIterator();
    rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

    // Set alias
    if (alias) this->tableName = alias;
  };

  // Start a new iterator given the new key range
  void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive,
                   bool highKeyInclusive) {
    iter->close();
    delete iter;
    iter = new RM_IndexScanIterator();
    rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                 highKeyInclusive, *iter);
  };

  RC getNextTuple(void *data) {
    int rc = iter->getNextEntry(rid, key);
    if (rc == 0) {
      rc = rm.readTuple(tableName.c_str(), rid, data);
    }
    return rc;
  };

  void getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->attrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for (i = 0; i < attrs.size(); ++i) {
      string tmp = tableName;
      tmp += ".";
      tmp += attrs.at(i).name;
      attrs.at(i).name = tmp;
    }
  };

  ~IndexScan() { iter->close(); };
};

class Filter : public Iterator {
  // Filter operator
 public:
  Filter(Iterator *input,            // Iterator of input R
         const Condition &condition  // Selection condition
  );
  ~Filter(){};

  RC getNextTuple(void *data);
  // For attribute in vector<Attribute>, name it as rel.attr
  void getAttributes(vector<Attribute> &attrs) const;

 private:
  Condition condition;
  Iterator *input;
  ConditionEvaluator *cEval;
  vector<Attribute> attributes;
};

class Project : public Iterator {
  // Projection operator
 public:
  Project(
      Iterator *input,                   // Iterator of input R
      const vector<string> &attrNames);  // vector containing attribute names
  ~Project();

  RC getNextTuple(void *data);
  // For attribute in vector<Attribute>, name it as rel.attr
  void getAttributes(vector<Attribute> &attrs) const;

 private:
  Iterator *input;
  vector<string> attributeNames;
};

class BNLJoin : public Iterator {
  // Block nested-loop join operator
 public:
  BNLJoin(Iterator *leftIn,            // Iterator of input R
          TableScan *rightIn,          // TableScan Iterator of input S
          const Condition &condition,  // Join condition
          const unsigned
              numPages  // # of pages that can be loaded into memory,
                        //   i.e., memory block size (decided by the optimizer)
  );
  ~BNLJoin();

  RC getNextTuple(void *data);
  // For attribute in vector<Attribute>, name it as rel.attr
  void getAttributes(vector<Attribute> &attrs) const;

 private:
  RC success = 0;
  RC failure = -1;
  Iterator *leftIn;
  TableScan *rightIn;
  Condition condition;
  int leftTableBufferOffset = -1;
  char *leftTableBuffer;
  char *rightTupleBuffer;
  const char *leftTuplePointer = nullptr;
  unsigned numPages;
  int sizeOfLeftBuffer = 0;
  RC loadNextBlockInMemory();
  RC setState();
  bool getNextLeftRecord(const char *&leftTupleBuf);
  vector<Attribute> leftInAttributes;
  vector<Attribute> rightInAttributes;
  vector<Attribute> joinedAttributes;
  ConditionEvaluator *cEval;
  std::map<Value, vector<char *>> hashTable;
  vector<char *> matchingLeftRecords;
  char *joinedRecBuffer;
  const size_t MAX_JOINED_RECORD_SIZE = PAGE_SIZE * 2;
};

class INLJoin : public Iterator {
  // Index nested-loop join operator
 public:
  INLJoin(Iterator *leftIn,           // Iterator of input R
          IndexScan *rightIn,         // IndexScan Iterator of input S
          const Condition &condition  // Join condition
  );
  ~INLJoin();

  RC getNextTuple(void *data);
  // For attribute in vector<Attribute>, name it as rel.attr
  void getAttributes(vector<Attribute> &attrs) const;

 private:
  RC success = 0;
  RC failure = -1;
  Iterator *leftIn;
  IndexScan *rightIn;
  Condition condition;
  ConditionEvaluator *cEval;
  char *leftTuple;
  bool leftScanDone = false;
  char *rightTuple;
  bool rightScanDone = false;
  bool isLeftTableEmpty = false;
  vector<Attribute> joinedAttributes;
  vector<Attribute> lAttr;
  vector<Attribute> rAttr;
  char *lowKey, *highKey;
  char *joinedRecBuf;
  const size_t MAX_JOINED_RECORD_SIZE = 2 * PAGE_SIZE;

  RC setState();
  RC resetRightIterator();  // close and open a new iterator
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
  // Grace hash join operator
 public:
  GHJoin(Iterator *leftIn,             // Iterator of input R
         Iterator *rightIn,            // Iterator of input S
         const Condition &condition,   // Join condition (CompOp is always EQ)
         const unsigned numPartitions  // # of partitions for each relation
                                       // (decided by the optimizer)
  ){};
  ~GHJoin(){};

  RC getNextTuple(void *data) { return QE_EOF; };
  // For attribute in vector<Attribute>, name it as rel.attr
  void getAttributes(vector<Attribute> &attrs) const {};
};

class Aggregate : public Iterator {
  // Aggregation operator
 public:
  Iterator *iterator;
  Attribute aggAttr;
  Attribute groupAttr;
  AggregateOp op;
  vector<Attribute> aggAttrs;
  map<string, float> groupedVals;
  map<string, float>::iterator nextGroupedVal;
  bool isGroupBy;

  // Mandatory
  // Basic aggregation
  Aggregate(Iterator *input,    // Iterator of input R
            Attribute aggAttr,  // The attribute over which we are computing an
                                // aggregate
            AggregateOp op      // Aggregate operation
  );

  // Optional for everyone: 5 extra-credit points
  // Group-based hash aggregation
  Aggregate(Iterator *input,    // Iterator of input R
            Attribute aggAttr,  // The attribute over which we are computing an
                                // aggregate
            Attribute groupAttr,  // The attribute over which we are grouping
                                  // the tuples
            AggregateOp op        // Aggregate operation
  );
  ~Aggregate(){};

  RC getNextTuple(void *data);
  // Please name the output attribute as aggregateOp(aggAttr)
  // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
  // output attrname = "MAX(rel.attr)"
  void getAttributes(vector<Attribute> &attrs) const;
};

void joinRecords(const RawRecord &leftRec, const RawRecord &rightRec,
                 char *joinedRecBuf, vector<Attribute> joinedAttrs);
#endif
