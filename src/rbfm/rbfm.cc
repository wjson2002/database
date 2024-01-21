#include "src/include/rbfm.h"
#include <iostream>
#include <cmath>
#include <cstring>
namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        int result = PagedFileManager::instance().createFile(fileName);

        return result;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        int result = PagedFileManager::instance().destroyFile(fileName);

        return result;
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        int result = PagedFileManager::instance().openFile(fileName, fileHandle);

        return result;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        int result = PagedFileManager::instance().closeFile(fileHandle);

        return result;
    }
    void RecordBasedFileManager::initSlotDirectory(FileHandle &fileHandle, PageNum pageNum){
        short freeSpace = 4000;
        int slotSize = 256;
        char numOfRecords = 0;
        char buffer[PAGE_SIZE];

        memcpy(buffer + PAGE_SIZE - sizeof(short), &freeSpace, sizeof(short));
        buffer[PAGE_SIZE - 3] = numOfRecords;

        for(int i = PAGE_SIZE - 3;i > PAGE_SIZE - slotSize; i-=2){
            buffer[i] = 0;
            buffer[i - 1] = 0;
        }
        fileHandle.writePage(pageNum, buffer);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        printf("Start insertRecord\n");
        printf("RD size: %zu\n", recordDescriptor.size());

        PageNum pageNumber = rid.pageNum;

        if(fileHandle.numOfPages == 0){
            rid.pageNum = 0;
            rid.slotNum = 0;

            void* buffer[PAGE_SIZE];

            fileHandle.appendPage(data);
            initSlotDirectory(fileHandle, rid.pageNum);

            fileHandle.readPage(0, buffer);
            readSlotDirectory(buffer);

            char* slotDirectoryPointer = getSlotDirectoryPointer(buffer);
            short freeSpace = getSlotSize(slotDirectoryPointer);
            char numOfRecords = getSlotElementSize(slotDirectoryPointer);

            int dataSize = getRecordSize(recordDescriptor, data);
            printf("num of records: %d\n", numOfRecords);
            //Update Slot directory by writing to page
            if((int)numOfRecords == 0){
                rid.slotNum = 0;
                std::memcpy(buffer, data, getRecordSize(recordDescriptor, data));\
                fileHandle.writePage(rid.pageNum, buffer);

                addRecordToSlotDirectory(fileHandle, rid,slotDirectoryPointer, dataSize);
                fileHandle.readPage(0, buffer);
                readSlotDirectory(buffer);
            }
            else{
                for(int i = 1;i <= numOfRecords;i++){
                    rid.slotNum = i;
                }
            }
            return 0;

        }
        else{
            return 0;
        }
        return -1;
    }
    int RecordBasedFileManager::getRecordSize( const std::vector<Attribute> &recordDescriptor,
                                               const void *data){
        int size = 0;

        printRecord(recordDescriptor, data, std::cout);
        for(auto record : recordDescriptor){
            int length = record.length;
            size += length;
        }
        printf("SIZE OF RECORD BEING WRITTEN: %d\n", size);
        return size;
    }


    char* RecordBasedFileManager::getSlotDirectoryPointer(void* page){
        char* pointer = (char*)(page) + PAGE_SIZE;
        return pointer;
    }

    short RecordBasedFileManager::getSlotSize(char* slotPointer){
        char* freeSpacePointer = slotPointer - sizeof(short);
        return *(short*)freeSpacePointer;
    }
    char RecordBasedFileManager::getSlotElementSize(char* slotPointer){
        char* numOfRecordsPointer = slotPointer - sizeof(short)  - sizeof(char);
        return *numOfRecordsPointer;
    }

    void  RecordBasedFileManager::writeToBuffer(void* data, void* buffer){

    }

    void RecordBasedFileManager::addRecordToSlotDirectory(FileHandle &fileHandle, RID &rid,
                                                          char* slotPointer, int length) {
        short slotSize = getSlotSize(slotPointer);
        char buffer[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum,buffer);

        char numOfRecords = getSlotElementSize(slotPointer);
        unsigned numOfPages = fileHandle.numOfPages;
        char updatedNumOfRecords = (char)((int)numOfRecords + 1);

        printf("Num of pages: %d UPDATED char value to: %d\n", numOfPages, updatedNumOfRecords);

        FILE* myFile = fileHandle.myFile;
        int offset = (numOfPages - rid.pageNum - 1) * PAGE_SIZE; //Get end of N number of pages
        buffer[PAGE_SIZE - 3] = updatedNumOfRecords;

        printf("done updating new num of records, wrote %lu\n", sizeof(updatedNumOfRecords));

        int differ = 0;
        slotPointer-=3;
        int off = 0;
        int position = (rid.pageNum + 1) * sizeof(int);
        for(int i = 0;i < numOfRecords; i++){
            int l;
            memcpy(&l, buffer+PAGE_SIZE - 3 - position, sizeof(int));
            printf("FOUND LENGTH: %d", l);
            off += buffer[PAGE_SIZE - 3 - position];
        }
        char d = (char)(differ);


        buffer[PAGE_SIZE - 2 - position] = d;
        printf("trying to update to length of%d", length);
        memcpy(buffer + PAGE_SIZE - 3 - position - sizeof(int), &off, sizeof(int));

        // Update the length in the buffer
        memcpy(buffer + PAGE_SIZE - 3 - position, &length, sizeof(int));
        fileHandle.writePage(rid.pageNum, buffer);
        readSlotDirectory(buffer);

        printf("done updating slot directory\n");
    }

    void RecordBasedFileManager::readSlotDirectory(void* page){
        printf("\nOUTPUTING SLOT DIRECTORY TO CHECK:\n");
        char* slothead = getSlotDirectoryPointer(page);
        short freesize = getSlotSize(slothead);
        char n = getSlotElementSize(slothead);

        uint8_t* bytePtr = (uint8_t*)page;
        for (size_t i = 0; i < 100; ++i) {
            printf("%02X ", bytePtr[i]);

            if ((i + 1) % 32 == 0) {
                printf("\n");
            }
        }
        printf("\nFIRST 100 BYTES\n");
        for (size_t i = 3800; i < PAGE_SIZE; ++i) {
            printf("%02X ", bytePtr[i]);

            if ((i + 1) % 32 == 0) {
                printf("\n");
            }
        }

        for (int i = PAGE_SIZE-3; i >= (PAGE_SIZE-3-(n*2)); --i) {
            printf("{%d:%d}\n", i, bytePtr[i]); // Adjust this based on your slot data structure
        }

        printf("Elements in slot{%d}, Free space:{%hd}\n", n,freesize);
        printf("\nDONE CHECK\n");
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        printf("Start readRecord\n");
        printf("Finding PAGENUM:{%d} SLOTNUM:{%d}...\n", rid.pageNum, rid.slotNum);
        void* readBuffer[PAGE_SIZE];
        RC result = fileHandle.readPage(rid.pageNum,readBuffer);
        int length = getRecordSize(recordDescriptor, data);
        char* slotDirectoryPointer = getSlotDirectoryPointer(readBuffer) - 3;
        int totalOffset = 0;
        for(int i = 0;i<rid.slotNum;i++){
            int offset;
            memcpy(&offset, slotDirectoryPointer - sizeof(int), sizeof(int));
            totalOffset+=offset;
        }
        memcpy(data, readBuffer+totalOffset, length);

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    std::vector<int> RecordBasedFileManager::serialize(char* bytes){

        int bits = sizeof(bytes);
        printf("SERIALZING:SIZE OF %lu, %d bit Array: }", sizeof(bytes), bits);
        std::vector<int> bitArray(bits);
        int index = 0;
        for (int i = (sizeof(bytes)) - 1; i >= 0; --i) {
            int bit = ((*bytes & (1 << i)) != 0 ? 1 : 0);
            bitArray[index] = bit;
            printf("%d", bit);
            index+=1;
        }
        printf("\n");
        return bitArray;
    }
    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        printf("start PrintRecord\n");

        int numOfNullBytes = ceil((double)recordDescriptor.size()/8);
        char* dataPointer = (char*)data;
        printf("data: {%s}, NumofNull:%d, %zu\n",dataPointer , numOfNullBytes, recordDescriptor.size());
        char nullIndicators[numOfNullBytes];

        for (int i = 0; i < numOfNullBytes; ++i) {
            nullIndicators[i] = dataPointer [i];
            dataPointer++;
            printf("Null indicator array{%d}\n", nullIndicators[i]);
        }

        std::vector<int> bitArray = serialize(nullIndicators);

        int index = 0;
        for (const Attribute& attribute : recordDescriptor){
            out << attribute.name.c_str();

            if(bitArray[index] == 1){
                out << ": NULL";
                if(index == recordDescriptor.size() - 1){
                    out<< "\n";
                }
                else{
                    out<< ", ";
                }
                index ++;
                continue;
            }

            switch (attribute.type) {
                case TypeInt:
                    out << ": " << *(int*)dataPointer;
                    dataPointer +=4;
                    break;
                case TypeReal:
                    out<< ": " << *(float*)dataPointer;
                    dataPointer +=4;
                    break;
                case TypeVarChar:
                    out << ": ";
                    int* length= (int*)dataPointer;
                    dataPointer += 4;
                    for(int i = 0; i < *length; i++){
                        out<<*dataPointer;
                        dataPointer++;
                    }
                    break;
            }
            if(index == recordDescriptor.size() - 1){
                out<< "\n";
            }
            else{
                out<< ", ";
            }
            index ++;
        }
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

