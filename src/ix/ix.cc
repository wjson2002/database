#include "src/include/ix.h"
#include <iostream>
#include <string.h>

namespace PeterDB {
    int ENTRY_SIZE = 7 * sizeof(int);
    int rootPage = 1;
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

        printf("Num of pages %d\n", ixFileHandle.fileHandle->numOfPages);
        if(ixFileHandle.fileHandle->numOfPages == 0){
            insertFirstEntry(ixFileHandle,attribute,key,rid);
            printf("after insert %d\n", ixFileHandle.fileHandle->numOfPages);
            return 0;
        }
        return -1;
    }

    RC IndexManager::insertFirstEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid){
        printf("Init b+ tree\n");

        int length = 0;
        void* data = malloc(ENTRY_SIZE);
        createIndex(data, attribute,key,rid,length, 0, 0, 0, 0);

        void* temp = malloc(PAGE_SIZE);
        memmove(temp, data, ENTRY_SIZE);

        ixFileHandle.fileHandle->appendPage(temp);
        ixFileHandle.fileHandle->appendPage(temp);
        free(temp);
        free(data);
        ixFileHandle.slotCount += 1;
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        void* pageBuffer = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle->readPage(rootPage, pageBuffer);
        ixFileHandle.fileHandle->readPage(rootPage, pageBuffer);
        int page1,slot1,page2,slot2;
        RID readRid;
        int tempKey;
        IndexManager::instance().readIndex(pageBuffer,tempKey,readRid,page1,slot1,page2,slot2);

        if(tempKey == *(int*)key){
            memset(pageBuffer, -1, ENTRY_SIZE);
            ixFileHandle.fileHandle->writePage(rootPage,pageBuffer);

            return 0;
        }
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {

        ix_ScanIterator.lowKey = lowKey;
        ix_ScanIterator.highKey = highKey;
        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.fileHandle = ixFileHandle.fileHandle;
        ix_ScanIterator.ixFileHandle = ixFileHandle;
        void* pageBuffer = malloc(PAGE_SIZE);
        int key,page1,slot1,page2,slot2;
        PeterDB::RID rid{};
        ixFileHandle.fileHandle->readPage(rootPage, pageBuffer);
        readIndex(pageBuffer,key,rid,page1,slot1,page2,slot2);
        ix_ScanIterator.currentPage = page1;
        ix_ScanIterator.currentSlot = slot1;
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        int key,page1,slot1,page2,slot2;
        PeterDB::RID rid{};
        void* data = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle->readPage(rootPage, data);
        readIndex(data,key,rid,page1,slot1,page2,slot2);

        std::string json_string = "{";
        //root is pointing to root
        if(slot1 == 0 && slot1 != -1){
            json_string += R"("keys":[")" +
                            std::to_string(key) +
                            ":[" + std::to_string(rid.pageNum) + "," + std::to_string(rid.slotNum) + "]\"";
        }
        else{
            json_string += "\"keys\":[";
        }

        printf("Key: %d, RID: (%d, %d), Page 1: %d, Slot 1: %d, Page 2: %d, Slot 2: %d\n",
               key, rid.pageNum, rid.slotNum, page1, slot1, page2, slot2);

        json_string += "]}";

        out << json_string;
        printf("%s\n", json_string.c_str());
        free(data);
        return 0;
    }

    RC IndexManager::createIndex(void *data, const PeterDB::Attribute &attribute, const void *key,
                                 const PeterDB::RID &rid,int &length, int page1, int slot1, int page2, int slot2) {
        memmove(data, key, sizeof(int));
        memmove((char*)data + sizeof(int), &rid.pageNum, sizeof(int));
        memmove((char*)data + 2 * sizeof(int), &rid.slotNum, sizeof(int));
        memmove((char*)data + 3 * sizeof(int), &page1, sizeof(int));
        memmove((char*)data + 4 * sizeof(int), &slot1, sizeof(int));
        memmove((char*)data + 5 * sizeof(int), &page2, sizeof(int));
        memmove((char*)data + 6 * sizeof(int), &slot2, sizeof(int));

        return 0;
    }

    RC IndexManager::readIndex(const void* data, int& key, PeterDB::RID& rid, int& page1, int& slot1, int& page2, int& slot2) const{
        memcpy(&key, data, sizeof(int));
        memcpy(&rid.pageNum, (char*)data + sizeof(int), sizeof(int));
        memcpy(&rid.slotNum, (char*)data + 2 * sizeof(int), sizeof(int));
        memcpy(&page1, (char*)data + 3 * sizeof(int), sizeof(int));
        memcpy(&slot1, (char*)data + 4 * sizeof(int), sizeof(int));
        memcpy(&page2, (char*)data + 5 * sizeof(int), sizeof(int));
        memcpy(&slot2, (char*)data + 6 * sizeof(int), sizeof(int));
        return 0;
    }

    RC IndexManager::readRoot(const void *data, int &key, RID &rid, int &page1, int &slot1, int &page2, int &slot2) const {
        return 0;
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        printf("SLOT COUNT: %d\n",ixFileHandle.slotCount);
        if(currentSlot < ixFileHandle.slotCount){
            void* pageBuffer = malloc(PAGE_SIZE);
            fileHandle->readPage(currentPage, pageBuffer);
            int page1,slot1,page2,slot2;


            int tempKey;
            IndexManager::instance().readIndex(pageBuffer,tempKey,rid,page1,slot1,page2,slot2);
            memmove(key, &tempKey, sizeof(int));
            currentSlot += 1;
            return 0;
        }
        return IX_EOF;

    }

    RC IX_ScanIterator::close() {
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;

        fileOpen = false;
        FileHandle* fileHandle;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        ixReadPageCounter = fileHandle->readPageCounter;
        ixWritePageCounter= fileHandle->writePageCounter;
        ixAppendPageCounter = fileHandle->appendPageCounter;

        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

} // namespace PeterDB