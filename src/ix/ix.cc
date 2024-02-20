#include "src/include/ix.h"
#include <iostream>
#include <sys/stat.h>

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if(ixFileHandle.fileOpen){
            return -1;
        }
        ixFileHandle.fileOpen = true;
        PagedFileManager& pfm = PagedFileManager::instance();
        FileHandle *fh = new FileHandle();
        ixFileHandle.fileHandle = fh;

        pfm.openFile(fileName, *fh);
        printf("%s opened\n", fh->FileName.c_str());
        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        ixFileHandle.fileOpen = false;
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.closeFile(*ixFileHandle.fileHandle);
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        fileOpen = false;
        FileHandle fileHandle;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        fileHandle->loadFile();
        ixReadPageCounter = fileHandle->readPageCounter;
        ixWritePageCounter= fileHandle->writePageCounter;
        ixAppendPageCounter = fileHandle->appendPageCounter;

        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

} // namespace PeterDB