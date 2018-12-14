#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <climits>
#include <string>
using namespace std;

class FileHandle;

struct FileStat {
  unsigned numberOfPages;
  unsigned readPageCounter;
  unsigned writePageCounter;
  unsigned appendPageCounter;
};
class PagedFileManager {
 public:
  static PagedFileManager *instance();  // Access to the _pf_manager instance

  RC createFile(const string &fileName);   // Create a new file
  RC destroyFile(const string &fileName);  // Destroy a file
  RC openFile(const string &fileName, FileHandle &fileHandle);  // Open a file
  RC closeFile(FileHandle &fileHandle);                         // Close a file

 protected:
  PagedFileManager();   // Constructor
  ~PagedFileManager();  // Destructor

 private:
  static PagedFileManager *_pf_manager;
  void initializeFileStat(
      FILE *fStream);  // initializes the page stats in a file
  void updateFileStat(FILE *fStream, FileStat &fileStat);
};

class FileHandle {
 private:
  FILE *file;
  const unsigned PAGE_START_COUNTER = sizeof(struct FileStat);

 public:
  // variables to keep the counter for each operation
  unsigned readPageCounter;
  unsigned writePageCounter;
  unsigned appendPageCounter;
  unsigned numberOfPages;
  FileHandle();   // Default constructor
  ~FileHandle();  // Destructor

  RC readPage(PageNum pageNum, void *data);         // Get a specific page
  RC writePage(PageNum pageNum, const void *data);  // Write a specific page
  RC appendPage(const void *data);                  // Append a specific page
  unsigned getNumberOfPages();  // Get the number of pages in the file
  RC collectCounterValues(
      unsigned &readPageCount, unsigned &writePageCount,
      unsigned
          &appendPageCount);  // Put the current counter values into variables
  FILE *getFile();
  RC setFile(FILE *file);
  RC resetFile();
  void loadFileStats();
  void updateFileStat(FileStat &fileStat);
};

#endif
