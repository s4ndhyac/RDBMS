#include <bits/basic_file.h>
#include <rbf/pfm.h>
#include <stddef.h>
#include <unistd.h>
#include <iostream>

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance() {
  if (!_pf_manager) _pf_manager = new PagedFileManager();

  return _pf_manager;
}

int fileExists(const char *fileName) {
  if (access(fileName, F_OK) != -1) {
    return 1;
  } else {
    return 0;
  }
}

PagedFileManager::PagedFileManager() {}

PagedFileManager::~PagedFileManager() {}

RC PagedFileManager::createFile(const string &fileName) {
  //	std::fstream pageFile(fileName, std::ios::app | std::ios::binary |
  // std::ios::in | std::ios::out);
  FILE *pageFile;

  // Check if file exists
  if (fileExists(fileName.c_str())) {
    // cerr << "PagedFileManager::createFile > File already exists: "
    //				<< fileName << endl;
    return -1;
  } else {
    pageFile = std::fopen(fileName.c_str(), "wb+");
  }
  if (pageFile == NULL) {
    // cerr << "PagedFileManager::createFile > Error creating a file";
    return -1;
  }

  initializeFileStat(pageFile);

  int closedSuccessfully = std::fclose(pageFile);
  if (closedSuccessfully == 0) {
    // cerr << "PagedFileManager::createFile > Page Closed successfully";
  } else {
    // cerr
    //				<< "PagedFileManager::createFile > Error
    // encountered while closing page";
    return -1;
  }
  return 0;
}

void PagedFileManager::initializeFileStat(FILE *pageFile) {
  fseek(pageFile, 0, SEEK_SET);
  FileStat initialStat;
  initialStat.numberOfPages = 0;
  initialStat.readPageCounter = 0;
  initialStat.writePageCounter = 0;
  initialStat.appendPageCounter = 0;
  fwrite(&initialStat, sizeof(struct FileStat), 1, pageFile);
}

void PagedFileManager::updateFileStat(FILE *pageFile, FileStat &fileStat) {
  // Move to beginning of file
  fseek(pageFile, 0, SEEK_SET);
  fwrite(&fileStat, sizeof(struct FileStat), 1, pageFile);
}

RC PagedFileManager::destroyFile(const string &fileName) {
  if (!fileExists(fileName.c_str())) {
    // cerr << "PagedFileManager::destroyFile > File does not exists : "
    //				<< fileName << endl;
    return -1;
  }

  if (!remove(fileName.c_str()) == 0) {
    // cerr
    //				<< "PagedFileManager::destroyFile > Error
    // encountered while deleting the file"
    //				<< endl;
    return -1;
  }

  return 0;
}

/**
 * @param fileName : The name of an already existing file to open
 * @param fileHandle : file handle object used to point to fileName. The file
 * handle object should not point to any another file.
 * @return success integer
 */
RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
  // Return error if the fileHandle object is pointing to another file
  if (fileHandle.getFile() != NULL) {  // if file handle is in use
    // cerr << "PagedFileManager::openFile > File handle already in use"
    //				<< endl;
    return -1;
  }

  // Check that the file exists
  if (!fileExists(fileName.c_str())) {  // if file does not exist
    // cerr << "PagedFileManager::openFile > File :" << fileName
    //				<< "does not exist";
    return -1;
  }
  // Open a file and make fileHandle point to it
  FILE *file = fopen(fileName.c_str(), "rb+");
  if (file == NULL) {
    // cerr << "Error encountered while opening file" << fileName << endl;
    return -1;
  }
  fileHandle.setFile(file);
  fileHandle.loadFileStats();
  // If the action is successful return 0;
  return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
  FILE *file = fileHandle.getFile();
  // persist the updated page values and pointers before closing file
  // get the current file stats
  FileStat currentFileStats;
  currentFileStats.numberOfPages = fileHandle.numberOfPages;
  currentFileStats.readPageCounter = fileHandle.readPageCounter;
  currentFileStats.writePageCounter = fileHandle.writePageCounter;
  currentFileStats.appendPageCounter = fileHandle.appendPageCounter;

  // update the file stat section in the file
  updateFileStat(file, currentFileStats);

  // Flush the changes and close the file
  int flushSuccess = fflush(file);
  int closeSuccess = fclose(file);

  // reset the state of the file handle so that it can be used with another file
  fileHandle.resetFile();

  if (flushSuccess == 0 && closeSuccess == 0) {
    return 0;
  } else {
    return -1;
  }
}

FileHandle::FileHandle() {
  numberOfPages = 0;
  readPageCounter = 0;
  writePageCounter = 0;
  appendPageCounter = 0;
  file = NULL;
}

FILE *FileHandle::getFile() { return file; }

RC FileHandle::setFile(FILE *_file) {
  if (file != NULL) {
    // cerr
    //				<< "FileHandle:: PROHIBITED : Attempting to open
    // two streams using a single file handle"
    //				<< endl;
    return -1;
  }
  file = _file;
  return 0;
}

FileHandle::~FileHandle() {}

RC FileHandle::readPage(PageNum pageNum, void *data) {
  if (numberOfPages <= pageNum) {
    // cerr << "FileHandle :: page index out of bounds:" << pageNum;
    return -1;
  }
  fseek(file, PAGE_START_COUNTER + pageNum * PAGE_SIZE, SEEK_SET);
  size_t pagesRead = fread(data, PAGE_SIZE, 1, file);
  if (pagesRead != 1) {
    // cerr
    //				<< "FileHandle :: Unknown error encountered
    // while reading pages from file";
    return -1;
  }
  readPageCounter = readPageCounter + 1;
  return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
  if (pageNum >= numberOfPages) {
    // cerr << "FileHandle::writePage : page number index out of bounds : "
    //				<< pageNum << endl;
    return -1;
  }
  fseek(file, PAGE_START_COUNTER + PAGE_SIZE * pageNum, SEEK_SET);
  int pagesWritten = fwrite(data, PAGE_SIZE, 1, file);
  if (pagesWritten != 1) {
    // cerr << "Unknown error encountered while writing to a page" << endl;
    return -1;
  }
  int success = fflush(file);
  writePageCounter = writePageCounter + 1;
  return 0;
}

RC FileHandle::appendPage(const void *data) {
  fseek(file, PAGE_START_COUNTER + numberOfPages * PAGE_SIZE, SEEK_SET);
  int pagesAppended = fwrite(data, PAGE_SIZE, 1, file);
  int writeSuccess = fflush(file);
  if (pagesAppended != 1) {
    // cerr << "Unknown error encountered while appending page";
    return -1;
  }
  appendPageCounter = appendPageCounter + 1;
  numberOfPages++;

  FileStat currentFileStats;
  currentFileStats.numberOfPages = numberOfPages;
  currentFileStats.readPageCounter = readPageCounter;
  currentFileStats.writePageCounter = writePageCounter;
  currentFileStats.appendPageCounter = appendPageCounter;

  // update the file stat section in the file
  updateFileStat(currentFileStats);

  return 0;
}

unsigned FileHandle::getNumberOfPages() { return numberOfPages; }

RC FileHandle::collectCounterValues(unsigned &readPageCount,
                                    unsigned &writePageCount,
                                    unsigned &appendPageCount) {
  readPageCount = readPageCounter;
  writePageCount = writePageCounter;
  appendPageCount = appendPageCounter;
  return 0;
}

void FileHandle::loadFileStats() {
  if (file == NULL) {
    // cerr
    //				<< "ERROR : Load stats should be called only
    // after opening a file stream"
    //				<< endl;
  }
  FileStat fileStat;
  fseek(file, 0, SEEK_SET);
  fread(&fileStat, sizeof(struct FileStat), 1, file);
  numberOfPages = fileStat.numberOfPages;
  readPageCounter = fileStat.readPageCounter;
  writePageCounter = fileStat.writePageCounter;
  appendPageCounter = fileStat.appendPageCounter;
  // The file read counter will advance after the file stats
}

void FileHandle::updateFileStat(FileStat &fileStat) {
  // Move to beginning of file
  fseek(file, 0, SEEK_SET);
  fwrite(&fileStat, sizeof(struct FileStat), 1, file);
  fflush(file);
}

RC FileHandle::resetFile() {
  file = NULL;
  numberOfPages = 0;
  readPageCounter = 0;
  writePageCounter = 0;
  appendPageCounter = 0;
}
