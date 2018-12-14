#ifndef _ix_h_
#define _ix_h_

#include <climits>
#include <memory>
#include <stack>
#include <string>
#include <vector>

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;
struct SplitInfo;
class IntermediateComparator;
class IntermediateEntry;
class Entry;
class BTPage;

enum BTPageType { INTERMEDIATE = 1, LEAF = 0 };

class IndexManager {

    public:
        static IndexManager* instance();

        RC setRootPage(IXFileHandle &ixfileHandle, PageNum rootPageID);

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        

    protected:
        IndexManager();
        ~IndexManager();

    private:
        RC traverse(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, stack<PageNum> &traversal);
        static IndexManager *_index_manager;
        PagedFileManager *pfm;
        const int success = 0;
        const int failure = 1;

    private:
      RC setUpIndexFile(IXFileHandle &ixfileHandle, const Attribute &attribute);
      PageNum getRootPageID(IXFileHandle &ixfileHandle) const;
	char* prepareEmptyBTPageBuffer(int offset);
	void printBtree(IXFileHandle &ixfileHandle, BTPage* root) const;
};


class IX_ScanIterator {
    public:
        IXFileHandle *ixfileHandle;
        Attribute attribute;
        const void* lowKey;
        const void* highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        shared_ptr<Entry> nextLeafEntry;
        shared_ptr<Entry> highLeafEntry;
        int isEOF;
        shared_ptr<BTPage> btPg;
        r_slot islot;


		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    
    FileHandle fileHandle;

};

class Key{
public:
	virtual void setKeyData(char* entry, int offset) = 0;
	virtual r_slot getKeySize() = 0;
	virtual ~Key();
	virtual int compare(Key& other) = 0;
	virtual string toString() = 0;
};

class StringKey : public Key{
private:
	string data;
	r_slot keySize = 0;
public:
	void setKeyData(char* entry, int offset);
	r_slot getKeySize() ;
	int compare(Key& other);
	string getData();
	string toString();
};

class IntKey : public Key{
private:
	int data;
public:
	void setKeyData(char* entry, int offset);
	r_slot getKeySize() ;
	int compare(Key& other);
	int getData();
	string toString();
};

class FloatKey : public Key{
private:
	float data;
public:
	 void setKeyData(char* entry, int offset);
	r_slot getKeySize() ;
	 int compare(Key& other);
	float getData();
	string toString();
};

class Entry{
public:
	Entry(shared_ptr<char> entry, AttrType aType);
	static shared_ptr<Entry> getEntry(shared_ptr<char> entry, AttrType aType, BTPageType pageType);
	virtual shared_ptr<Key> getKey();
	virtual RID getRID();
	virtual r_slot getEntrySize();
	virtual int getKeyOffset();
	virtual int getRIDOffset();
	virtual ~Entry();
	char* getEntryBuffer();
	virtual string toString();
private:
	shared_ptr<Key> key;
	bool isKeyDataSet = false;
protected:
	shared_ptr<char> entry;
	AttrType aType;
//	Key *key;
//	RID rid;
};

class IntermediateEntry : public Entry{
public:
	PageNum getLeftPtr();
	PageNum getRightPtr();
	IntermediateEntry(shared_ptr<char> entry, AttrType aType);
	~IntermediateEntry();
	int getKeyOffset();
	int getRIDOffset();
	r_slot getEntrySize();
	void setLeftPtr(PageNum lPg);
	void setRightPtr(PageNum rPg);
	string toString();
private:
	PageNum leftPtr = 0;
	PageNum rightPtr = 0;


};


class EntryComparator{
public:
	virtual int compare(Entry& a , Entry& b) = 0;
	virtual ~EntryComparator();

};

class IntermediateComparator : public EntryComparator{
public:
	int compare(Entry& a , Entry& b) ;
};

class LeafComparator : public EntryComparator{
public:
	int compare(Entry& a , Entry& b) ;
};



/**
 * Page Format for BTPage :
 * | L/I | SP |    |    |    |    |        |    |
 * |-----+----+----+----+----+----+--------+----|
 * |     |    |    |    |    |    |        |    |
 * |     |    |    |    |    |    |        |    |
 * |     |    |    |    |    |    |        |    |
 * |     |    | L2 | O2 | L1 | O1 | FS = 5 | NS |
 *
 * Legends:
 * L/I = Leaf/Intermediate
 * SP = Sibling Pointer: Will be used only by leaf nodes
 * NS = Number of slots
 * FS = Free space pointer
 *
 * Provides interface to insert entries into a btpage.
 * It's the user's responsibilities to interpret the entries
 * and sort them while inserting
 */
class BTPage {
 public:
  BTPage(shared_ptr<char> page, const Attribute &attribute);
  ~BTPage();
  BTPageType getPageType();
  r_slot getFreeSpaceAvailable();
  bool isSpaceAvailableToInsertEntryOfSize(r_slot size);
  static void prepareEmptyBTPageBuffer(char* pageBuffer, BTPageType pageType);
  void setSiblingNode(PageNum siblingPgNum);  // copy sibling pointer to page buffer
  PageNum getSiblingNode();

  /**
   * Inserts entry buf in BTPage at freespace pointer and stores its
   * offset at the {@code slotNumber}. Pushes other slots ahead by 1
   *
   * @param entry
   * @param slotNumber : index at which entry offset, length are stored.
   * @param length : length of the char buffer.
   * @return
   */
  RC insertEntryInOrder(Entry& entry);
  RC insertEntry(const char *const entry, int slotNumber, int length);
  shared_ptr<Entry> getEntry(r_slot slotNum);
  RC removeEntry(int slotNumber, char * const entryBuf);
  RC readEntry(r_slot slotNum, char * const buf);
  char *getPage();
  PageNum getSiblingPageNum();
  int getNumberOfSlots();
  shared_ptr<SplitInfo> splitNodes(Entry &insertEntry, EntryComparator &comparator);
  Attribute getAttribute();
  const static PageNum NULL_PAGE = UINT_MAX;
  string toString();
  r_slot isEntryPresent(Entry e);
  int binarySearch(Entry &entry);


 private:
  // The read methods will read the pageBuffer into data members
  void readPageType();
  void readAttribute(const Attribute &attribute);
  void readPageRecordInfo();
  void readSlotDirectory();
  void readSiblingNode();
  RC appendEntry(const char *const entry, int length);
  // r_slot getSmallestEntryGreaterThan(char* key, KeyComparator const* key);
  void copySlotsToPage(vector<SlotDirectory> &slots, char *pageBuffer);

  // The set methods will update the pageBuffer*
  void setPageType(BTPageType pgType); // copy pType to pageBuffer
  void setAttribute(const Attribute &attribute);
  void setPageRecordInfo(); // copy pri to page buffer
  void setSlotDirectory(); //copy the slots vector to page buffer
  void printPage();

  RC deleteSlotAndRecord(int slotNumberToDelete, int byBytesToShift);

 private:
  BTPageType pageType;
  PageRecordInfo pri;
  vector<SlotDirectory> slots;
  PageNum siblingPage;  // for leaf pages
  shared_ptr<char> pageBuffer;
  Attribute attribute;
  const int success = 0;
  const int failure = 1;
};

struct SplitInfo{
	shared_ptr<BTPage> leftChild;
	shared_ptr<BTPage> rightChild;
	shared_ptr<IntermediateEntry> iEntryParent;

//	~SplitInfo(){
//		char* lpgBuffer = leftChild->getPage();
//		char* rpgBuffer = rightChild->getPage();
//		char* parentBuffer = iEntryParent->getEntryBuffer();

//		delete [] lpgBuffer;
//		delete [] rpgBuffer;
//		delete [] parentBuffer;

//	}
};

#endif
