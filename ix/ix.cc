#include "ix.h"

#include <float.h>
#include <sys/types.h>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>

#include <memory>

IndexManager *IndexManager::_index_manager = 0;

RC IndexManager::setRootPage(IXFileHandle &ixfileHandle, PageNum rootPgID) {
  char *headerPage = new char[PAGE_SIZE]();
  ixfileHandle.fileHandle.readPage(0, headerPage);

  memcpy(headerPage, &rootPgID, sizeof(rootPgID));

  ixfileHandle.fileHandle.writePage(0, headerPage);

  delete[] headerPage;
  return success;
}

r_slot prepareLeafEntry(char *leafEntryBuf, const void *key, RID rid,
                        const Attribute &attribute) {
  int keySize = 0;
  if (attribute.type == TypeVarChar) {
    int length = 0;
    memcpy(&length, key, sizeof(length));
    keySize = sizeof(length) + length;

  } else {
    keySize = sizeof(int);
  }
  memcpy(leafEntryBuf, key, keySize);
  memcpy(leafEntryBuf + keySize, &rid, sizeof(rid));

  return keySize + sizeof(rid);
}

r_slot prepareIntermediateEntry(char *intermediateEntryBuffer,
                                char *leafEntryBuf, int leafEntrySize,
                                PageNum lPg, PageNum rPg) {
  int offset = 0;
  // left page pointer
  memcpy(intermediateEntryBuffer, &lPg, sizeof(lPg));
  offset += sizeof(lPg);

  // data
  memcpy(intermediateEntryBuffer + offset, leafEntryBuf, leafEntrySize);
  offset += leafEntrySize;

  // right page pointer
  memcpy(intermediateEntryBuffer + offset, &rPg, sizeof(rPg));
  offset += sizeof(rPg);

  return offset;
}
IndexManager *IndexManager::instance() {
  if (!_index_manager) _index_manager = new IndexManager();

  return _index_manager;
}

void BTPage::prepareEmptyBTPageBuffer(char *rootPage, BTPageType pageType) {
  // set up root page data
  // set leaf/intermediate indicator
  //	BTPageType pageType = BTPageType::LEAF;
  int offset = 0;
  memcpy(rootPage, &pageType, sizeof(pageType));
  offset += sizeof(pageType);
  // set sibling pointer. will be used only in leaf node
  PageNum siblingPage = BTPage::NULL_PAGE;
  memcpy(rootPage + offset, &siblingPage, sizeof(siblingPage));
  offset += sizeof(siblingPage);
  PageRecordInfo pri;
  pri.freeSpacePos = offset;
  pri.numberOfSlots = 0;
  updatePageRecordInfo(pri, rootPage);
}

/**
 * This method will be called for the very first insert in the index file
 * It will create a header page with attribute information and root node info
 * It will create a root page as a leaf node
 * By the end of the method the index file will have 2 pages:0-header, 1-root
 * @param ixfileHandle
 * @param attribute
 * @return status code
 */
RC IndexManager::setUpIndexFile(IXFileHandle &ixfileHandle,
                                const Attribute &attribute) {
  //	PageNum HEADER_PAGE_NUM = 0;
  PageNum ROOT_PAGE_NUM = 1;
  if (ixfileHandle.fileHandle.getNumberOfPages() != 0) {
    cout << "setupIndexFile called on an already setup index file" << endl;
    return failure;
  }
  // setup header page data
  AttrType keyAttributeType = attribute.type;
  AttrLength keyAttributeLength = attribute.length;

  // set up header page buffer and write to file
  char *headerPage = new char[PAGE_SIZE]();
  int offset = 0;
  memcpy(headerPage, &ROOT_PAGE_NUM, sizeof(ROOT_PAGE_NUM));
  offset += sizeof(ROOT_PAGE_NUM);
  memcpy(headerPage + offset, &keyAttributeType, sizeof(keyAttributeType));
  offset += sizeof(keyAttributeType);
  memcpy(headerPage + offset, &keyAttributeLength, sizeof(keyAttributeLength));
  offset += sizeof(keyAttributeLength);
  ixfileHandle.fileHandle.appendPage(headerPage);  // header page id is 0
  delete[](headerPage);
  headerPage = NULL;

  // set up root page data
  char *rootPage = new char[PAGE_SIZE]();
  BTPage::prepareEmptyBTPageBuffer(rootPage, BTPageType::LEAF);

  ixfileHandle.fileHandle.appendPage(rootPage);  // root page id is 1
  delete[](rootPage);
  rootPage = NULL;

  return success;
}

IndexManager::IndexManager() {}

IndexManager::~IndexManager() {}

RC IndexManager::createFile(const string &fileName) {
  return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName) {
  return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
  return pfm->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
  return pfm->closeFile(ixfileHandle.fileHandle);
}

PageNum IndexManager::getRootPageID(IXFileHandle &ixfileHandle) const {
  char *headerPage = new char[PAGE_SIZE]();
  memset(headerPage, 0, PAGE_SIZE);
  ixfileHandle.fileHandle.readPage(0, headerPage);
  unsigned rootID = 0;
  memcpy(&rootID, headerPage, sizeof(rootID));
  delete[](headerPage);
  headerPage = NULL;
  return rootID;
}

RC IndexManager::traverse(IXFileHandle &ixfileHandle,
                          const Attribute &attribute, const void *key,
                          const RID &rid, stack<PageNum> &traversal) {
  if (ixfileHandle.fileHandle.getNumberOfPages() == 0) {
    setUpIndexFile(ixfileHandle, attribute);
  }

  shared_ptr<char> leafEntryBuf(new char[PAGE_SIZE](),
                                std::default_delete<char[]>());
  r_slot entrySize = prepareLeafEntry(leafEntryBuf.get(), key, rid, attribute);
  //	Entry leafEntry(leafEntryBuf, attribute.type);
  shared_ptr<Entry> leafEntry =
      make_shared<Entry>(leafEntryBuf, attribute.type);

  PageNum rootPageId = getRootPageID(ixfileHandle);
  shared_ptr<char> pageData(new char[PAGE_SIZE](),
                            std::default_delete<char[]>());
  ixfileHandle.fileHandle.readPage(rootPageId, pageData.get());

  shared_ptr<BTPage> btPg = make_shared<BTPage>(pageData, attribute);
  PageNum currentPageNum = rootPageId;
  while (btPg->getPageType() != BTPageType::LEAF) {
    traversal.push(currentPageNum);
    r_slot islot;
    bool right = false;
    int index = btPg->binarySearch(*leafEntry);
    if (index >= btPg->getNumberOfSlots()) {
      islot = index - 1;
      right = true;
    } else {
      islot = index < 0 ? index + 1 : index;
    }
    shared_ptr<IntermediateEntry> ptrIEntry =
        dynamic_pointer_cast<IntermediateEntry>(btPg->getEntry(islot));
    if (!right) {  // islot in range
      currentPageNum = ptrIEntry->getLeftPtr();
    } else {  // islot is out of range
              // this means all entries in the file were smaller than
              // ientry and that caused all slots to be read
              // But it may also mean that there were no entries in the file
              // In the first case, we have to traverse to the rightPtr of
              // iEntry Case 2 can never happen. We will never get an empty
              // intermediate node at this point in the code
      currentPageNum = ptrIEntry->getRightPtr();
    }
    ixfileHandle.fileHandle.readPage(currentPageNum, pageData.get());
    btPg = make_shared<BTPage>(pageData, attribute);
  }
  traversal.push(currentPageNum);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
                             const Attribute &attribute, const void *key,
                             const RID &rid) {
  //	char* leafEntryBuf = new char[PAGE_SIZE]();
  shared_ptr<char> leafEntryBuf(new char[PAGE_SIZE](),
                                std::default_delete<char[]>());
  r_slot entrySize = prepareLeafEntry(leafEntryBuf.get(), key, rid, attribute);
  shared_ptr<Entry> leafEntry =
      make_shared<Entry>(leafEntryBuf, attribute.type);
  //	Entry leafEntry(leafEntryBuf, attribute.type);

  stack<PageNum> traversal;
  traverse(ixfileHandle, attribute, key, rid, traversal);

  PageNum currentPageNum = traversal.top();
  shared_ptr<char> pageData(new char[PAGE_SIZE](), default_delete<char[]>());
  ixfileHandle.fileHandle.readPage(currentPageNum, pageData.get());
  shared_ptr<BTPage> btPg;
  btPg = make_shared<BTPage>(pageData, attribute);

  if (btPg->isSpaceAvailableToInsertEntryOfSize(entrySize)) {
    btPg->insertEntryInOrder(*leafEntry);
    ixfileHandle.fileHandle.writePage(traversal.top(), btPg->getPage());
    //		delete[] (leafEntry->getEntryBuffer());
  } else {
    //		traversal.push(currentPageNum);
    shared_ptr<Entry> splitEntry;  // entry whose insertion causes a split
    splitEntry = leafEntry;
    bool exitWithoutSplit = false;
    shared_ptr<SplitInfo> split;
    while (!traversal.empty()) {
      PageNum pageNum = traversal.top();
      traversal.pop();
      ixfileHandle.fileHandle.readPage(pageNum, pageData.get());
      btPg = make_shared<BTPage>(pageData, attribute);

      if (btPg->isSpaceAvailableToInsertEntryOfSize(
              splitEntry->getEntrySize())) {
        btPg->insertEntryInOrder(*splitEntry);
        ixfileHandle.fileHandle.writePage(pageNum, btPg->getPage());
        exitWithoutSplit = true;
        break;
      }
      IntermediateComparator icomp;
      split = btPg->splitNodes(*splitEntry, icomp);

      // First write the right hand page. It does not need any further changes
      // the old left page's sibling pointer gets copied into the right page
      // sibling pointer in the split nodes method. So directly write this page
      // to disk don't move these lines. they need to be together
      ixfileHandle.fileHandle.appendPage(split->rightChild->getPage());
      PageNum rPg = ixfileHandle.fileHandle.getNumberOfPages() - 1;

      // store left's sibling as right page and write it to disk at its
      // old page number
      PageNum lPg = pageNum;
      split->leftChild->setSiblingNode(rPg);
      ixfileHandle.fileHandle.writePage(lPg, split->leftChild->getPage());

      // Now make //the proper connections from parent to left and right page
      split->iEntryParent->setLeftPtr(lPg);
      split->iEntryParent->setRightPtr(rPg);

      splitEntry = split->iEntryParent;

      // recursively push up the iEntryParent if needed
      //		pushUp(ixfileHandle, attribute, split->iEntryParent,
      //traversal);
    }
    if (!exitWithoutSplit) {  // that means split was done till root level
      shared_ptr<char> intermediateRootPageBuffer(
          new char[PAGE_SIZE](), std::default_delete<char[]>());
      BTPage::prepareEmptyBTPageBuffer(intermediateRootPageBuffer.get(),
                                       INTERMEDIATE);
      btPg = make_shared<BTPage>(intermediateRootPageBuffer, attribute);
      btPg->insertEntryInOrder(*splitEntry);

      ixfileHandle.fileHandle.appendPage(btPg->getPage());
      PageNum pNum = ixfileHandle.fileHandle.getNumberOfPages() - 1;
      setRootPage(ixfileHandle, pNum);
    }
  }

  return success;
}
///**
// *
// * @param ixfileHandle
// * @param iEntry
// * @param pathToLeaf
// * @return ifRootChangeRequired
// */
// bool IndexManager::pushUp(IXFileHandle &ixfileHandle, const Attribute& attr,
//		IntermediateEntry &iEntry, stack<PageNum>& pathToLeaf) {
//
//	IntermediateComparator icomp;
//	SplitInfo *split = btPg->splitNodes(leafEntry, icomp);
//
//	// First write the right hand page. It does not need any further changes
//	// the old left page's sibling pointer gets copied into the right page
//sibling
//	// pointer in the split nodes method. So directly write this page to
//disk
//	// don't move these lines. they need to be together
//	ixfileHandle.fileHandle.appendPage(split->rightChild->getPage());
//	PageNum rPg = ixfileHandle.fileHandle.getNumberOfPages();
//
//	// store left's sibling as right page and write it to disk at its
//	// old page number
//	PageNum lPg = currentPageNum;
//	split->leftChild->setSiblingNode(rPg);
//	ixfileHandle.fileHandle.writePage(lPg, split->leftChild->getPage());
//
//	// Now make the proper connections from parent to left and right page
//	split->iEntryParent->setLeftPtr(lPg);
//	split->iEntryParent->setRightPtr(rPg);
//
//}

//}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
                             const Attribute &attribute, const void *key,
                             const RID &rid) {
  if (ixfileHandle.fileHandle.getNumberOfPages() == 0) {
    setUpIndexFile(ixfileHandle, attribute);
  }

  shared_ptr<char> leafEntryBuf(new char[PAGE_SIZE](),
                                std::default_delete<char[]>());
  r_slot entrySize = prepareLeafEntry(leafEntryBuf.get(), key, rid, attribute);
  Entry leafEntry(leafEntryBuf, attribute.type);

  stack<PageNum> traversal;
  traverse(ixfileHandle, attribute, key, rid, traversal);

  PageNum currentPageNum = traversal.top();
  shared_ptr<char> pageData(new char[PAGE_SIZE](),
                            std::default_delete<char[]>());
  ixfileHandle.fileHandle.readPage(currentPageNum, pageData.get());
  shared_ptr<BTPage> btPg;
  btPg = make_shared<BTPage>(pageData, attribute);

  r_slot slotToDelete = btPg->isEntryPresent(leafEntry);
  if (slotToDelete == USHRT_MAX) {
    return failure;
  } else {
    char *entryBuf = new char[PAGE_SIZE]();
    btPg->removeEntry(slotToDelete, entryBuf);
    ixfileHandle.fileHandle.writePage(currentPageNum, btPg->getPage());
    delete[] entryBuf;
    return success;
  }

  return success;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
                      const void *lowKey, const void *highKey,
                      bool lowKeyInclusive, bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
  if (ixfileHandle.fileHandle.getFile() == NULL) return -1;

  ix_ScanIterator.ixfileHandle = &ixfileHandle;
  ix_ScanIterator.attribute = attribute;
  ix_ScanIterator.lowKey = lowKey;
  ix_ScanIterator.highKey = highKey;
  ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
  ix_ScanIterator.highKeyInclusive = highKeyInclusive;

  //		char* lowKeyLeafEntryBuf = new char[PAGE_SIZE]();
  shared_ptr<char> lowKeyLeafEntryBuf(new char[PAGE_SIZE](),
                                      std::default_delete<char[]>());
  shared_ptr<Entry> leafEntry;

  PageNum rootPageId = getRootPageID(ixfileHandle);
  shared_ptr<char> pageData(new char[PAGE_SIZE](),
                            std::default_delete<char[]>());
  shared_ptr<BTPage> btPg;
  ixfileHandle.fileHandle.readPage(rootPageId, pageData.get());
  btPg = make_shared<BTPage>(pageData, attribute);

  PageNum pgToTraverse = rootPageId;
  if (lowKey) {
    RID lowKeyRID;
    lowKeyRID.pageNum = 0;
    lowKeyRID.slotNum = 0;
    r_slot entrySize = prepareLeafEntry(lowKeyLeafEntryBuf.get(), lowKey,
                                        lowKeyRID, attribute);
    leafEntry = make_shared<Entry>(lowKeyLeafEntryBuf, attribute.type);
  }

  while (btPg->getPageType() != BTPageType::LEAF) {
    r_slot islot;
    shared_ptr<IntermediateEntry> ptrIEntry;
    for (islot = 0; islot < btPg->getNumberOfSlots(); islot++) {
      ptrIEntry =
          dynamic_pointer_cast<IntermediateEntry>(btPg->getEntry(islot));
      IntermediateComparator iComp;
      if (leafEntry == nullptr ||
          iComp.compare(*ptrIEntry, *leafEntry) >
              0) {  // if entry in node greater than leaf entry
        break;
      }
    }
    if (islot < btPg->getNumberOfSlots()) {  // islot in range
      pgToTraverse = ptrIEntry->getLeftPtr();
    } else {
      pgToTraverse = ptrIEntry->getRightPtr();
    }
    ixfileHandle.fileHandle.readPage(pgToTraverse, pageData.get());
    btPg = make_shared<BTPage>(pageData, attribute);
  }

  ix_ScanIterator.btPg = btPg;

  ix_ScanIterator.nextLeafEntry = leafEntry;

  shared_ptr<Entry> highLeafEntry;
  if (highKey) {
    //			char* highKeyLeafEntryBuf = new char[PAGE_SIZE]();
    shared_ptr<char> highKeyLeafEntryBuf(new char[PAGE_SIZE](),
                                         std::default_delete<char[]>());
    RID highKeyRID;
    highKeyRID.pageNum = USHRT_MAX;
    highKeyRID.slotNum = USHRT_MAX;
    r_slot entrySize = prepareLeafEntry(highKeyLeafEntryBuf.get(), highKey,
                                        highKeyRID, attribute);
    highLeafEntry = make_shared<Entry>(highKeyLeafEntryBuf, attribute.type);
  }
  ix_ScanIterator.highLeafEntry = highLeafEntry;

  return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
                              const Attribute &attribute) const {
  PageNum rootPageID = getRootPageID(ixfileHandle);
  shared_ptr<char> pageData(new char[PAGE_SIZE](),
                            std::default_delete<char[]>());
  ixfileHandle.fileHandle.readPage(rootPageID, pageData.get());
  BTPage btPage(pageData, attribute);

  cout << "" << endl;
  printBtree(ixfileHandle, &btPage);
  cout << "" << endl;
  /*
   * {
   "keys":["P"],
   "children":[
   {"keys":["C","G","M"],
   "children": [
   {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]},
   {"keys": ["D:[(3,1),(3,2)]","E:[(4,1)]","F:[(5,1)]"]},
   {"keys": ["J:[(5,1),(5,2)]","K:[(6,1),(6,2)]","L:[(7,1)]"]},
   {"keys": ["N:[(8,1)]","O:[(9,1)]"]}
   ]},
   {"keys":["T","X"],
   "children": [
   {"keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]},
   {"keys": ["U:[(13,1)]","V:[(14,1)]"]},
   {"keys": ["Y:[(15,1)]","Z:[(16,1)]"]}
   ]}
   ]
   }
   */
}

IX_ScanIterator::IX_ScanIterator() {
  ixfileHandle = 0;
  lowKey = 0;
  highKey = 0;
  lowKeyInclusive = false;
  highKeyInclusive = false;
  attribute.name = "";
  attribute.type = TypeInt;
  attribute.length = 0;
  nextLeafEntry = 0;
  highLeafEntry = 0;
  isEOF = 0;
  btPg = 0;
  islot = 0;
}

IX_ScanIterator::~IX_ScanIterator() {}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
  bool entryFound = false;
  shared_ptr<char> pageData(new char[PAGE_SIZE](),
                            std::default_delete<char[]>());
  shared_ptr<Entry> pageLeafEntry;
  while (!entryFound && isEOF != IX_EOF) {
    r_slot numberOfSlots = btPg->getNumberOfSlots();
    LeafComparator lcomp;
    while (islot < numberOfSlots) {
      //			btPg->readEntry(islot, entry);
      //			islot++;
      //			Entry *pageLeafEntry = new Entry(entry,
      //attribute.type);
      pageLeafEntry = btPg->getEntry(islot);
      islot++;
      bool isLowerMatch = false;
      if (nextLeafEntry != nullptr)
        isLowerMatch = lowKeyInclusive
                           ? lcomp.compare(*nextLeafEntry, *pageLeafEntry) <= 0
                           : lcomp.compare(*nextLeafEntry, *pageLeafEntry) < 0;
      bool isHigherMatch = false;
      if (highLeafEntry != nullptr)
        isHigherMatch = highKeyInclusive
                            ? lcomp.compare(*highLeafEntry, *pageLeafEntry) >= 0
                            : lcomp.compare(*highLeafEntry, *pageLeafEntry) > 0;
      if ((nextLeafEntry == nullptr || isLowerMatch) &&
          (highLeafEntry == nullptr || isHigherMatch)) {
        entryFound = true;
        break;
      }
    }

    if (!entryFound) {
      PageNum siblingPageNum = btPg->getSiblingPageNum();
      if (siblingPageNum == BTPage::NULL_PAGE) {
        isEOF = IX_EOF;
        break;
      }
      ixfileHandle->fileHandle.readPage(siblingPageNum, pageData.get());
      btPg = make_shared<BTPage>(pageData, attribute);
      islot = 0;
    }
  }

  if (entryFound) {
    char *entry = pageLeafEntry->getEntryBuffer();
    shared_ptr<Entry> hitEntry = pageLeafEntry;
    rid = hitEntry->getRID();
    shared_ptr<Key> hitKey = hitEntry->getKey();
    r_slot keySize = hitKey->getKeySize();
    r_slot keyOffset = hitEntry->getKeyOffset();
    memcpy(key, entry + keyOffset, keySize);
    nextLeafEntry = hitEntry;
    return 0;
  }

  return -1;
}

RC IX_ScanIterator::close() {
  lowKey = 0;
  highKey = 0;
  lowKeyInclusive = false;
  highKeyInclusive = false;
  attribute.name = "";
  attribute.type = TypeInt;
  attribute.length = 0;
  nextLeafEntry = 0;
  highLeafEntry = 0;
  isEOF = 0;
  btPg = 0;
  islot = 0;

  return 0;
}

IXFileHandle::IXFileHandle() {
  ixReadPageCounter = 0;
  ixWritePageCounter = 0;
  ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
                                      unsigned &writePageCount,
                                      unsigned &appendPageCount) {
  if (fileHandle.collectCounterValues(readPageCount, writePageCount,
                                      appendPageCount) != -1)
    return 0;
  return -1;
}

Entry::Entry(shared_ptr<char> entry, AttrType atype) {
  this->aType = atype;
  this->entry = entry;
  switch (aType) {
    case TypeReal:
      key = make_shared<FloatKey>();
      break;
    case TypeInt:
      key = make_shared<IntKey>();
      break;
    case TypeVarChar:
      key = make_shared<StringKey>();
      break;
  }
}

shared_ptr<Entry> Entry::getEntry(shared_ptr<char> entry, AttrType aType,
                                  BTPageType pageType) {
  shared_ptr<Entry> ptrEntry;

  switch (pageType) {
    case INTERMEDIATE:
      ptrEntry = make_shared<IntermediateEntry>(entry, aType);
      break;
    case LEAF:
      ptrEntry = make_shared<Entry>(entry, aType);
      break;
    default:
      cerr << "Error: Invalid page type";
  }
  return ptrEntry;
}

Entry::~Entry() {}
IntermediateEntry::~IntermediateEntry() {}
shared_ptr<Key> Entry::getKey() {
  if (isKeyDataSet) {
    return key;
  }
  int offset = getKeyOffset();
  isKeyDataSet = true;
  key->setKeyData(entry.get(), offset);
  return key;
}
RID Entry::getRID() {
  int offset = getRIDOffset();
  RID rid;
  memcpy(&rid, entry.get() + offset, sizeof(rid));
  return rid;
}

int Entry::getKeyOffset() { return 0; }

int Entry::getRIDOffset() { return getKeyOffset() + getKey()->getKeySize(); }

Key::~Key() {}

void StringKey::setKeyData(char *entry, int offset) {
  int length;
  memcpy(&length, entry + offset, sizeof(length));
  offset += sizeof(length);
  data.assign(entry + offset, entry + offset + length);
  keySize = sizeof(length) + length;
}

r_slot StringKey::getKeySize() {
  if (keySize == 0) {
    cerr << "Before asking for size, set key data using setKeyData" << endl;
  }
  return keySize;
}

string StringKey::getData() { return data; }

int StringKey::compare(Key &other) {
  string otherString = static_cast<StringKey &>(other).getData();
  return this->data.compare(otherString);
}

void IntKey::setKeyData(char *entry, int offset) {
  memcpy(&data, entry + offset, sizeof(int));
}

r_slot IntKey::getKeySize() { return sizeof(int); }

int IntKey::getData() { return data; }

int IntKey::compare(Key &other) {
  int thisData = this->getData();
  int otherData = static_cast<IntKey &>(other).getData();

  return thisData - otherData;
}

void FloatKey::setKeyData(char *entry, int offset) {
  memcpy(&data, entry + offset, sizeof(float));
}

r_slot FloatKey::getKeySize() { return sizeof(float); }

float FloatKey::getData() { return this->data; }
int FloatKey::compare(Key &other) {
  float thisData = this->getData();
  float otherData = static_cast<FloatKey &>(other).getData();

  if (fabs(thisData - otherData) < FLT_EPSILON)
    return 0;

  else if (thisData - otherData < 0)
    return -1;
  else
    return 1;
}

IntermediateEntry::IntermediateEntry(shared_ptr<char> entry, AttrType aType)
    : Entry(entry, aType) {}

PageNum IntermediateEntry::getLeftPtr() {
  memcpy(&leftPtr, this->entry.get(), sizeof(leftPtr));
  return leftPtr;
}

PageNum IntermediateEntry::getRightPtr() {
  memcpy(&rightPtr,
         this->entry.get() + sizeof(leftPtr) + getKey()->getKeySize() +
             sizeof(getRID()),
         sizeof(rightPtr));
  return rightPtr;
}

int IntermediateEntry::getKeyOffset() { return sizeof(leftPtr); }

int IntermediateEntry::getRIDOffset() {
  return sizeof(leftPtr) + getKey()->getKeySize();
}

EntryComparator::~EntryComparator() {}

int IntermediateComparator::compare(Entry &a, Entry &b) {
  if (a.getKey()->compare(*(b.getKey())) != 0)
    return a.getKey()->compare(*(b.getKey()));
  else {
    RID aRid = a.getRID();
    RID bRid = b.getRID();

    if (aRid.pageNum != bRid.pageNum)
      return aRid.pageNum - bRid.pageNum;
    else
      return aRid.slotNum - bRid.slotNum;
  }
}

int LeafComparator::compare(Entry &a, Entry &b) {
  shared_ptr<Key> aKey = a.getKey();
  shared_ptr<Key> bKey = b.getKey();

  return aKey->compare(*bKey);
}
BTPage::BTPage(shared_ptr<char> pageData, const Attribute &attribute) {
  pageBuffer = pageData;
  readAttribute(attribute);
  readPageType();
  readSiblingNode();
  readPageRecordInfo();
  readSlotDirectory();
}

void BTPage::readAttribute(const Attribute &attribute) {
  this->attribute = attribute;
}

void BTPage::readSiblingNode() {
  memcpy(&siblingPage, pageBuffer.get() + sizeof(pageType),
         sizeof(siblingPage));
}
void BTPage::readPageType() {
  memcpy(&pageType, pageBuffer.get(), sizeof(pageType));
}

void BTPage::readPageRecordInfo() {
  memcpy(&pri, pageBuffer.get() + PAGE_SIZE - sizeof(pri), sizeof(pri));
}

/**
 * should be called after initializing {@link #pri}
 */
void BTPage::readSlotDirectory() {
  slots.clear();
  for (r_slot sloti = 0; sloti < pri.numberOfSlots; sloti++) {
    SlotDirectory currentSlot;
    // page number not needed to get slot. hence hardcoded to 0
    getSlotForRID(pageBuffer.get(), RID{0, sloti}, currentSlot);
    slots.push_back(currentSlot);
  }
}

bool BTPage::isSpaceAvailableToInsertEntryOfSize(r_slot length) {
  unsigned freeSpaceAvailable = PAGE_SIZE - pri.freeSpacePos -
                                slots.size() * sizeof(struct SlotDirectory) -
                                sizeof(struct PageRecordInfo);
  if (freeSpaceAvailable >= length + sizeof(struct SlotDirectory)) {
    return true;
  } else
    return false;
}

RC BTPage::insertEntry(const char *const entry, int slotNumber, int length) {
  if (!isSpaceAvailableToInsertEntryOfSize(length)) {
    // I need to split the leaf page and send information about
    // split node to callee so that an intermediate node entry can be
    // created
    return failure;
  }
  SlotDirectory slot;
  slot.offset = pri.freeSpacePos;
  slot.length = length;
  memcpy(pageBuffer.get() + slot.offset, entry, slot.length);

  slots.insert(slots.begin() + slotNumber, slot);
  copySlotsToPage(slots, pageBuffer.get());
  pri.freeSpacePos += length;
  pri.numberOfSlots += 1;
  updatePageRecordInfo(pri, pageBuffer.get());

  return success;
}

void BTPage::copySlotsToPage(vector<SlotDirectory> &slots, char *pageBuffer) {
  for (int i = 0; i < slots.size(); i++) {
    RID updatedRID;
    updatedRID.pageNum = 0;
    updatedRID.slotNum = i;
    updateSlotDirectory(updatedRID, pageBuffer, slots[i]);
  }
  //	memcpy(
  //			pageBuffer + PAGE_SIZE - sizeof(pri)
  //					- sizeof(struct SlotDirectory) * slots.size(),
  //slots.data(), 			slots.size() * sizeof(struct SlotDirectory));
}
/**
 * soft deletes the entry from the page and returns the
 * entry in form of a char buffer
 * @param slotNumber
 * @return
 */

RC BTPage::removeEntry(int slotNumber, char *const entryBuf) {
  readEntry(slotNumber, entryBuf);

  r_slot slotToDeleteLength = slots[slotNumber].length;

  deleteSlotAndRecord(slotNumber, -slots[slotNumber].length);

  pri.numberOfSlots--;
  pri.freeSpacePos -= slotToDeleteLength;
  setPageRecordInfo();  // write to buffer

  return success;
}

void BTPage::setPageType(BTPageType pgType) {
  memcpy(pageBuffer.get(), &pgType, sizeof(pgType));
  this->pageType = pgType;
}

void BTPage::setPageRecordInfo() {
  if (pri.numberOfSlots != slots.size()) {
    cout << "BTPage::setPageRecordInfo() : Number of slots in page record info"
         << pri.numberOfSlots;
    cout << "BTPage::setPageRecordInfo() : Number of slots in slots vector "
         << slots.size();
  }

  updatePageRecordInfo(pri, pageBuffer.get());
}

void BTPage::setSlotDirectory() {
  //	for (r_slot sloti = 0; sloti < pri.numberOfSlots; sloti++) {
  //		SlotDirectory currentSlot;
  //		// page number not needed to get slot. hence hardcoded to 0
  //		getSlotForRID(pageBuffer, RID{0, sloti}, currentSlot);
  //		slots.push_back(currentSlot);
  //	}
  if (pri.numberOfSlots != slots.size()) {
    cout << "BTPage::setSlotDirectory() : Number of slots in page record info"
         << pri.numberOfSlots;
    cout << "BTPage::setSlotDirectory() : Number of slots in slots vector "
         << slots.size();
  }
  copySlotsToPage(slots, pageBuffer.get());
  //	memcpy(
  //			pageBuffer + PAGE_SIZE - sizeof(pri)
  //					- sizeof(struct SlotDirectory) * slots.size(),
  //slots.data(), 			slots.size() * sizeof(struct SlotDirectory));
}

void BTPage::setSiblingNode(PageNum rightSiblingPgNum) {
  int offset = sizeof(pageType);
  memcpy(pageBuffer.get() + offset, &(rightSiblingPgNum),
         sizeof(rightSiblingPgNum));
  siblingPage = rightSiblingPgNum;
}

void BTPage::printPage() {
  cout << "L/I : " << this->pageType;
  cout << "SP  : " << this->siblingPage;
  cout << "FS  : " << this->pri.freeSpacePos;
  cout << "NS  : " << this->pri.numberOfSlots;
  int i = 0;
  for (SlotDirectory slot : slots) {
    cout << "Slot" << i << "  :  " << slot.offset << "," << slot.length << endl;
    i++;
  }
}

RC BTPage::deleteSlotAndRecord(int slotNumberToDelete, int byBytesToShift) {
  SlotDirectory slotToDelete = slots[slotNumberToDelete];
  r_slot nextSlotOffset = slotToDelete.offset + slotToDelete.length;
  r_slot slotToShiftOffset = nextSlotOffset;

  PageRecordInfo pri;
  getPageRecordInfo(pri, pageBuffer.get());

  if (slotToShiftOffset + byBytesToShift < 0 ||
      byBytesToShift + pri.freeSpacePos + getRecordDirectorySize(pri) >
          PAGE_SIZE) {
    return failure;
  }

  memmove(pageBuffer.get() + slotToShiftOffset + byBytesToShift,
          pageBuffer.get() + slotToShiftOffset,
          pri.freeSpacePos - slotToShiftOffset);

  slots.erase(slots.begin() + slotNumberToDelete);
  for (r_slot islot = 0; islot < slots.size(); islot++) {
    if (slots[islot].offset >= slotToShiftOffset)
      slots[islot].offset += byBytesToShift;
  }
  copySlotsToPage(slots, pageBuffer.get());

  return success;
}
BTPageType BTPage::getPageType() { return pageType; }

BTPage::~BTPage() {}

int BTPage::getNumberOfSlots() { return slots.size(); }

RC BTPage::readEntry(r_slot slotNum, char *const entryBuf) {
  assert(slotNum < slots.size() && "Slot index out of bounds");

  memcpy(entryBuf, pageBuffer.get() + slots[slotNum].offset,
         slots[slotNum].length);

  return success;
}

/**
 *
 * @param insertEntry : entry to insert in BTPage. This is the entry that will
 * cause split
 * @param comparator : Comparator to compare the entries
 * @return SplitInfo object
 *
 */
shared_ptr<SplitInfo> BTPage::splitNodes(Entry &insertEntry,
                                         EntryComparator &comparator) {
  shared_ptr<SplitInfo> split = make_shared<SplitInfo>();

  // prepare left child page
  shared_ptr<char> leftChildBuf(new char[PAGE_SIZE](),
                                std::default_delete<char[]>());
  BTPage::prepareEmptyBTPageBuffer(leftChildBuf.get(), this->pageType);
  split->leftChild = make_shared<BTPage>(leftChildBuf, this->attribute);

  // prepare right child page
  //	char* rightChildBuf = new char[PAGE_SIZE]();
  shared_ptr<char> rightChildBuf(new char[PAGE_SIZE](),
                                 std::default_delete<char[]>());
  BTPage::prepareEmptyBTPageBuffer(rightChildBuf.get(), this->pageType);
  split->rightChild = make_shared<BTPage>(rightChildBuf, this->attribute);

  // link the right child to left's sibling
  // it is the  caller's responsibility to link the left page's sibling
  // to right child's page
  // num when the right child will be inserted in the page
  split->rightChild->setSiblingNode(this->siblingPage);

  bool entryInserted = false;
  shared_ptr<Entry> slotEntry;
  shared_ptr<BTPage> pageToLoad =
      split->leftChild;  // ptr to current page getting loaded
  for (uint islot = 0; islot < slots.size(); islot++) {
    if (islot * 2 >= slots.size() + 1) {  // reached midway
                                          // now append to right buffer
      pageToLoad = split->rightChild;
    }
    // read next slot into entryBuf and then create its Entry object slotEntry
    //		char* entryBuf = new char[PAGE_SIZE]();
    //		memset(entryBuf, 0, PAGE_SIZE);
    //		readEntry(islot, entryBuf);
    slotEntry = getEntry(islot);

    if (!entryInserted) {
      // Compare slot entry with the whose insertion caused a split
      int difference = comparator.compare(*slotEntry, insertEntry);

      if (difference > 0)  // if entry in page is greater than entry to insert
      {
        // insert the smaller entry first
        pageToLoad->appendEntry(insertEntry.getEntryBuffer(),
                                insertEntry.getEntrySize());
        entryInserted = true;
      }
    }

    pageToLoad->appendEntry(slotEntry->getEntryBuffer(), slots[islot].length);

    //		delete slotEntry;
  }
  if (!entryInserted) {
    pageToLoad->appendEntry(insertEntry.getEntryBuffer(),
                            insertEntry.getEntrySize());
    entryInserted = true;
  }

  // prepare the intermediate node to push up
  shared_ptr<char> intermediateEntryBuffer(new char[PAGE_SIZE](),
                                           std::default_delete<char[]>());
  // if the page that got split is a leaf page
  if (this->pageType == LEAF) {
    // transform the leaf page to  an intermediate page
    shared_ptr<Entry> leafEntry = split->rightChild->getEntry(0);
    // choose the first entry from the right page and make it the parent
    prepareIntermediateEntry(intermediateEntryBuffer.get(),
                             leafEntry->getEntryBuffer(),
                             leafEntry->getEntrySize(), NULL_PAGE, NULL_PAGE);
  }
  // if page getting split is an intermediate page
  else {
    // remove the first entry from the right page and make it the parent
    split->rightChild->removeEntry(0, intermediateEntryBuffer.get());
  }

  split->iEntryParent =
      make_shared<IntermediateEntry>(intermediateEntryBuffer, attribute.type);

  return split;
}

RC BTPage::appendEntry(const char *const entry, int length) {
  if (!isSpaceAvailableToInsertEntryOfSize(length)) return failure;
  SlotDirectory slot;
  slot.offset = pri.freeSpacePos;
  slot.length = length;

  memcpy(pageBuffer.get() + slot.offset, entry, length);

  pri.freeSpacePos += length;
  pri.numberOfSlots += 1;

  slots.push_back(slot);
  copySlotsToPage(slots, pageBuffer.get());

  updatePageRecordInfo(pri, pageBuffer.get());
  return success;
}

r_slot Entry::getEntrySize() {
  return getKey()->getKeySize() + sizeof(getRID());
}

r_slot IntermediateEntry::getEntrySize() {
  return sizeof(leftPtr) + getKey()->getKeySize() + sizeof(getRID()) +
         sizeof(rightPtr);
}

char *Entry::getEntryBuffer() { return this->entry.get(); }

void IntermediateEntry::setLeftPtr(PageNum lPg) {
  leftPtr = lPg;
  memcpy(entry.get(), &lPg, sizeof(lPg));
}
void IntermediateEntry::setRightPtr(PageNum rPg) {
  rightPtr = rPg;
  int rightPointerOffset =
      sizeof(leftPtr) + getKey()->getKeySize() + sizeof(getRID());
  memcpy(entry.get() + rightPointerOffset, &rPg, sizeof(rPg));
}

RC BTPage::insertEntryInOrder(Entry &entry) {
  if (!isSpaceAvailableToInsertEntryOfSize(entry.getEntrySize())) {
    return failure;
  }

  r_slot numberOfSlots = this->getNumberOfSlots();
  IntermediateComparator icomp;  // for insert cmp both key,rid in leaf
  int islot = binarySearch(entry);
  if (islot < 0) islot++;
  this->insertEntry(entry.getEntryBuffer(), islot, entry.getEntrySize());
  return success;
}
/**
 * Find the index of the smallest Entry greater than equal to
 * @{code entry}
 * @param entry :
 * @return the index position of the smallest entry greater than entry
 */
int BTPage::binarySearch(Entry &entry) {
  int high = slots.size() - 1;
  int low = 0;
  int mid = 0;
  IntermediateComparator icomp;

  while (high >= low) {
    mid = low + (high - low) / 2;
    shared_ptr<Entry> midEntry = getEntry(mid);

    if (icomp.compare(entry, *midEntry) >= 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return high + 1;
}

char *BTPage::getPage() { return this->pageBuffer.get(); }

PageNum BTPage::getSiblingPageNum() { return this->siblingPage; }

void IndexManager::printBtree(IXFileHandle &ixfileHande, BTPage *root) const {
  if (root->getPageType() == INTERMEDIATE) {
    printf(R"( { "keys" : [%s], "children" : [ )", root->toString().c_str());
    //		IntermediateEntry *entry;
    shared_ptr<IntermediateEntry> entry;
    shared_ptr<char> pageBuffer(new char[PAGE_SIZE](),
                                std::default_delete<char[]>());
    for (int i = 0; i < root->getNumberOfSlots(); i++) {
      entry = dynamic_pointer_cast<IntermediateEntry>(root->getEntry(i));
      ixfileHande.fileHandle.readPage(entry->getLeftPtr(), pageBuffer.get());
      printBtree(ixfileHande, new BTPage(pageBuffer, root->getAttribute()));
      printf(",");
    }
    if (root->getNumberOfSlots() != 0) {
      ixfileHande.fileHandle.readPage(entry->getRightPtr(), pageBuffer.get());
      printBtree(ixfileHande, new BTPage(pageBuffer, root->getAttribute()));
    }
    printf("]}");
    cout << "" << endl;
  } else {
    printf(R"( { "keys" : [%s] } )", root->toString().c_str());
    // if not last page in tree
    cout << "" << endl;
  }

  /*
   * {
   "keys":["P"],
   "children":[
   {"keys":["C","G","M"],
   "children": [
   {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]},
   {"keys": ["D:[(3,1),(3,2)]","E:[(4,1)]","F:[(5,1)]"]},
   {"keys": ["J:[(5,1),(5,2)]","K:[(6,1),(6,2)]","L:[(7,1)]"]},
   {"keys": ["N:[(8,1)]","O:[(9,1)]"]}
   ]},
   {"keys":["T","X"],
   "children": [
   {"keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]},
   {"keys": ["U:[(13,1)]","V:[(14,1)]"]},
   {"keys": ["Y:[(15,1)]","Z:[(16,1)]"]}
   ]}
   ]
   }
   */
}

shared_ptr<Entry> BTPage::getEntry(r_slot slotNum) {
  if (slotNum < 0 || slotNum >= slots.size()) {
    cerr << "Invalid slot num " << slotNum;
    exit(1);
  }
  //	char* slotEntryBuf = new char[slots[slotNum].length];
  shared_ptr<char> slotEntryBuf(new char[slots[slotNum].length],
                                std::default_delete<char[]>());
  readEntry(slotNum, slotEntryBuf.get());
  shared_ptr<Entry> slotEntry =
      Entry::getEntry(slotEntryBuf, attribute.type, pageType);

  return slotEntry;
}

Attribute BTPage::getAttribute() { return attribute; }

PageNum BTPage::getSiblingNode() { return this->siblingPage; }

string BTPage::toString() {
  if (pageType == INTERMEDIATE) {
    string stringEntries = "";
    for (int i = 0; i < slots.size(); i++) {
      string sEntry = "\"" + getEntry(i)->toString() + "\"";
      stringEntries = stringEntries + sEntry;
      if (i != slots.size() - 1) {
        stringEntries = stringEntries + ",";
      }
    }
    return stringEntries;
  }
  if (pageType == LEAF) {
    string stringEntries = "";
    string sEntry = "";
    shared_ptr<Entry> previousEntry = getEntry(0);
    shared_ptr<Key> previousKey = previousEntry->getKey();
    RID rid = previousEntry->getRID();
    std::vector<RID> rids;
    rids.push_back(rid);
    for (int i = 1; i < slots.size(); i++) {
      shared_ptr<Entry> currentEntry = getEntry(i);
      shared_ptr<Key> currentKey = currentEntry->getKey();
      if (currentKey->compare(*previousKey) != 0) {
        string ridString = "";
        int irid = 0;
        for (RID eachRID : rids) {
          ridString += "(" + std::to_string(eachRID.pageNum) + "," +
                       std::to_string(eachRID.slotNum) + ")";
          if (irid != rids.size() - 1) ridString += ",";
          irid++;
        }
        rids.clear();

        sEntry =
            "\"" + previousKey->toString() + " : [" + ridString + " ]" + "\"";
        stringEntries = stringEntries + sEntry;
        stringEntries = stringEntries + ",";
      }
      // string sEntry = "\"" +  getEntry(i)->toString() + "\"";
      previousKey = currentKey;
      rids.push_back(currentEntry->getRID());
    }

    string ridString = "";
    int irid = 0;
    for (RID eachRID : rids) {
      ridString += "(" + std::to_string(eachRID.pageNum) + "," +
                   std::to_string(eachRID.slotNum) + ")";
      if (irid != rids.size() - 1) ridString += ",";
      irid++;
    }
    sEntry = "\"" + previousKey->toString() + " : [" + ridString + " ]" + "\"";
    stringEntries = stringEntries + sEntry;
    return stringEntries;
  }
  return "";
}

string Entry::toString() {
  string sEntry;

  sEntry = getKey()->toString() + ": [(" + std::to_string(getRID().pageNum) +
           "," + std::to_string(getRID().slotNum) + ")]";

  return sEntry;
}

string IntermediateEntry::toString() {
  return getKey()->toString() + ": [(" + std::to_string(getRID().pageNum) +
         "," + std::to_string(getRID().slotNum) + ")]";
}

string FloatKey::toString() { return std::to_string(getData()); }

string IntKey::toString() { return std::to_string(getData()); }

string StringKey::toString() { return getData(); }

// void IndexManager::traverseBtree(IXFileHandle &ixfileHandle, BTPage *btPg,
// const Attribute &attribute, LeafEntry *leafEntry) {

//     PageNum rootPageId = getRootPageID(ixfileHandle);
// 	char* pageData = new char[PAGE_SIZE]();
// 	ixfileHandle.fileHandle.readPage(rootPageId, pageData);
// 	btPg = new BTPage(pageData, attribute);
// 	PageNum pgToTraverse = rootPageId;
// 	char* entry = new char[PAGE_SIZE]();
//     while(btPg->getPageType() != BTPageType::LEAF){
// 		r_slot islot;
// 		IntermediateEntry *ptrIEntry = NULL;
// 		for ( islot = 0; islot < btPg->getNumberOfSlots(); islot++){
// 			memset(entry,0,PAGE_SIZE);
// 			btPg->readEntry(islot, entry);
// 			ptrIEntry = new IntermediateEntry (entry,
// attribute.type); 			IntermediateComparator iComp; 			if(leafEntry == 0 ||
// iComp.compare(*ptrIEntry, *leafEntry) > 0){ // if entry in node greater than
// leaf entry 				break;
// 			}
// 			delete[] ptrIEntry;
// 		}
// 		if (islot < btPg->getNumberOfSlots()){ // islot in range
// 			pgToTraverse = ptrIEntry->getLeftPtr();
// 		}
// 		else{ //islot is out of range
// 			// this means all entries in the file were smaller than
// 			// ientry and that caused all slots to be read
// 			// But it may also mean that there were no entries in the
// file
// 			// In the first case, we have to traverse to the rightPtr of
// iEntry
// 			// Case 2 can never happen. We will never get an empty
// intermediate node
// 			// at this point in the code
// 			pgToTraverse = ptrIEntry->getRightPtr();
//  		}
// 		if(btPg) delete[] btPg;
// 		memset(pageData,0, PAGE_SIZE);
// 		ixfileHandle.fileHandle.readPage(pgToTraverse, pageData);
// 		btPg = new BTPage(pageData, attribute);
// 		delete ptrIEntry;
// 	}
//     free(pageData);
//     free(entry);
// }

/**
 * Returns the slot number where the entry is located
 * If no matching entry is found then return USHRT_MAX
 * @param entry : entry to search
 * @return rslot : slot where the entry resides
 */
r_slot BTPage::isEntryPresent(Entry entry) {
  r_slot islot = 0;
  r_slot numberOfSlots = this->getNumberOfSlots();
  IntermediateComparator icomp;  // for insert cmp both key,rid in leaf
  bool entryFound = false;
  while (islot < numberOfSlots) {
    shared_ptr<Entry> pageLeafEntry = getEntry(islot);
    if (icomp.compare(*pageLeafEntry, entry) == 0) {
      entryFound = true;
      break;
    }
    islot++;
  }
  if (entryFound)
    return islot;
  else
    return USHRT_MAX;
}
