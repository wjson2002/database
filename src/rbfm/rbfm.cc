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
        printf("init page %d, sd %d\n", pageNum, freeSpace);
        fileHandle.writePage(pageNum, buffer);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        printf("Start insertRecord\n");

        char buffer[PAGE_SIZE];
        PageNum pageNumber = rid.pageNum;
        int totalNumOfPages = fileHandle.numOfPages;
        int recordSize = getRecordSize(recordDescriptor,data);
        int offsetPointer = 0;
        if(recordSize > 4000){

            printf("WARNING RECORD OVERSIZED CANNOT FIT IN PAGE\n");

            return -1;
        }

        if(totalNumOfPages == 0) {
            printf("ADDING FIRST PAGE\n");
            fileHandle.appendPage(data);
            initSlotDirectory(fileHandle, 0);
        }

        for(int i = 0;i < fileHandle.numOfPages;i++){
            printf("Loop Start [%d]\n", i);
            rid.pageNum = i;
            fileHandle.readPage(rid.pageNum, buffer);
            char* slotDirectoryPointer = getSlotDirectoryPointer(buffer);
            short freeSpace = getSlotSize(slotDirectoryPointer);
            char numOfRecords = getSlotElementSize(slotDirectoryPointer);

            if(freeSpace >= recordSize){

                rid.slotNum = addRecordToSlotDirectory(fileHandle, rid,
                                                       slotDirectoryPointer,
                                                       recordSize, buffer, offsetPointer);
                printf("Offset: %d\n",(int)offsetPointer);
                std::memcpy(buffer+offsetPointer, data, getRecordSize(recordDescriptor, data));
                fileHandle.writePage(rid.pageNum, buffer);
                printf("Wrote pgNum{%d} slNum{%d}\n", rid.pageNum, rid.slotNum);
                fileHandle.readPage(0, buffer);
                readSlotDirectory(buffer);
                return 0;
            }
        }
        printf("No free space avail: ADDING NEW PAGE\n");
        fileHandle.appendPage(data);
        rid.pageNum = fileHandle.numOfPages - 1;
        initSlotDirectory(fileHandle, rid.pageNum);
        fileHandle.readPage(rid.pageNum, buffer);
        readSlotDirectory(buffer);
        char* slotDirectoryPointer = getSlotDirectoryPointer(buffer);
        short freeSpace = getSlotSize(slotDirectoryPointer);
        char numOfRecords = getSlotElementSize(slotDirectoryPointer);
        rid.slotNum = addRecordToSlotDirectory(fileHandle, rid,slotDirectoryPointer, recordSize, buffer, offsetPointer);
        printf("Offset: %d\n",(int)offsetPointer);
        std::memcpy(buffer+offsetPointer, data, getRecordSize(recordDescriptor, data));
        fileHandle.writePage(rid.pageNum, buffer);
        printf("FWrote pgNum{%d} slNum{%d}\n", rid.pageNum, rid.slotNum);

        return 0;

    }


    int RecordBasedFileManager::getRecordSize( const std::vector<Attribute> &recordDescriptor,
                                               const void *data){
        int size = 0;

       //printRecord(recordDescriptor, data, std::cout);
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



    unsigned short RecordBasedFileManager::addRecordToSlotDirectory(FileHandle &fileHandle, RID &rid,
                                                          char* slotPointer, int length, char (&buffer)[PAGE_SIZE], int &offsetPointer) {

        char numOfRecords = getSlotElementSize(slotPointer);
        short freeSize = getSlotSize(slotPointer);
        unsigned numOfPages = fileHandle.numOfPages;
        ushort slotNum = numOfRecords;
        char updatedNumOfRecords = (char)((int)numOfRecords + 1);
        short newSize = freeSize - (short)length;
        if(numOfRecords == 0){
            printf("freesize: {%d} no records default update {%d}, {%d}: \n",freeSize ,updatedNumOfRecords, newSize);
            buffer[PAGE_SIZE - 3] = updatedNumOfRecords;
            memcpy(buffer + PAGE_SIZE - 3 - sizeof(int), &length, sizeof(int));
            offsetPointer=0;
        }
        else
        {
            printf("regular update {%d}, {%d}: \n", updatedNumOfRecords, newSize);
            memcpy(buffer + PAGE_SIZE - 2, &newSize, sizeof(short));
            buffer[PAGE_SIZE - 3] = updatedNumOfRecords;

            int totalLength = 0;

            for(int i = 0;i < numOfRecords; i++) {
                int l;
                memcpy(&l, buffer+PAGE_SIZE - 3 - (8 * i) - sizeof(int), sizeof(int));
                printf("FOUND LENGTH: %d\n", l);
                totalLength += l;
            }
            printf("adding pagenum:%d, slotnum:%d\n", rid.pageNum, slotNum);
            //update offset
            memcpy(buffer + PAGE_SIZE - 3 - 2 * sizeof(int) - (slotNum) * 8, &totalLength, sizeof(int));
//            // Update the length of element in the buffer
            memcpy(buffer + PAGE_SIZE - 3 - sizeof(int) - (slotNum) * 8, &length, sizeof(int));
            printf("done updating slot directory: total len{%d}, len{%d}\n", totalLength, length);
            offsetPointer=totalLength;
            printf("SetOFFSET POINTER TO %d", (int)offsetPointer);

        }
        return slotNum;
    }

    void RecordBasedFileManager::readSlotDirectory(void* page){
        printf("\nOUTPUTING SLOT DIRECTORY TO CHECK:\n");
        char* slothead = getSlotDirectoryPointer(page);
        short freesize = getSlotSize(slothead);
        char n = getSlotElementSize(slothead);

        uint8_t* bytePtr = (uint8_t*)page;
        for (size_t i = 0; i < 650; ++i) {
            printf("%02X ", bytePtr[i]);

            if ((i + 1) % 32 == 0) {
                printf("\n");
            }
        }
        printf("\nFIRST 100 BYTES\n");
        for (size_t i = 4000; i < PAGE_SIZE; ++i) {
            printf("%02X ", bytePtr[i]);

            if ((i + 1) % 32 == 0) {
                printf("\n");
            }
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
        readSlotDirectory(readBuffer);
        int length = getRecordSize(recordDescriptor, data);
        char* slotDirectoryPointer = getSlotDirectoryPointer(readBuffer) - 3;
        int totalOffset = 0;
        for(int i = 0;i<rid.slotNum;i++){
            int offset;
            memcpy(&offset, slotDirectoryPointer - sizeof(int), sizeof(int));
            totalOffset+=offset;
        }
        printf("TOTAL OFFSET OF RECORD: %d\n", totalOffset);
        memcpy(data, readBuffer+totalOffset, length);

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    std::vector<int> RecordBasedFileManager::serialize(char* bytes){

        int bits = sizeof(bytes);
        // printf("SERIALZING:SIZE OF %lu, %d bit Array: \n}", sizeof(bytes), bits);
        std::vector<int> bitArray(bits);
        int index = 0;
        for (int i = (sizeof(bytes)) - 1; i >= 0; --i) {
            int bit = ((*bytes & (1 << i)) != 0 ? 1 : 0);
            bitArray[index] = bit;

            index+=1;
        }
        return bitArray;
    }
    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        printf("start PrintRecord\n");

        int numOfNullBytes = ceil((double)recordDescriptor.size()/8);
        char* dataPointer = (char*)data;

        char nullIndicators[numOfNullBytes];

        for (int i = 0; i < numOfNullBytes; ++i) {
            nullIndicators[i] = dataPointer [i];
            dataPointer++;
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

