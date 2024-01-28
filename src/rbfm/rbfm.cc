#include "src/include/rbfm.h"
#include <iostream>
#include <cmath>
#include <cstring>


namespace PeterDB {
    int SLOTSIZE = 203;
    int MAX_ITEMS_IN_SLOT = (SLOTSIZE - 3) / 8;
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

        int slotSize = SLOTSIZE;
        short freeSpace = PAGE_SIZE - slotSize;
        char numOfRecords = 0;
        char* buffer = (char*)malloc(PAGE_SIZE);

        memcpy(buffer, &freeSpace, sizeof(short));
        memcpy(buffer + sizeof(short), &numOfRecords, sizeof(char));



        fileHandle.writePage(pageNum, buffer);
        free(buffer);
        char* temp[PAGE_SIZE];
        fileHandle.readPage(pageNum, temp);

    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

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
            fileHandle.appendPage(data);
            initSlotDirectory(fileHandle, 0);
        }
        //Check last page (newly appended page) first
        int numOfPages = fileHandle.numOfPages - 1;
        for(int i = 0;i < numOfPages;i++){
            rid.pageNum = i;
            fileHandle.readPage(rid.pageNum, buffer);
            short freeSpace;
            std::memmove(&freeSpace, buffer, sizeof(short));

            if(freeSpace >= recordSize){
                rid.slotNum = addRecordToSlotDirectory(fileHandle, rid,
                                                       recordSize, buffer, offsetPointer);

                std::memmove(buffer+offsetPointer, data, recordSize);
                std::memmove(buffer+offsetPointer, data, recordSize);
                fileHandle.writePage(rid.pageNum, buffer);
                return 0;
            }
        }
        //No free space avail: ADDING NEW PAGE
        fileHandle.appendPage(data);
        rid.pageNum = fileHandle.numOfPages - 1;
        initSlotDirectory(fileHandle, rid.pageNum);
        fileHandle.readPage(rid.pageNum, buffer);
        rid.slotNum = addRecordToSlotDirectory(fileHandle, rid, recordSize, buffer, offsetPointer);
        std::memmove(buffer+offsetPointer, data, getRecordSize(recordDescriptor, data));
        fileHandle.writePage(rid.pageNum, buffer);
        return 0;

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
                                                           int length, char (&buffer)[PAGE_SIZE], int &offsetPointer) {
        short freeSize;
        memcpy(&freeSize, buffer, sizeof(short));
        char numOfRecords;
        memcpy(&(numOfRecords), buffer + 2, sizeof(char));

        unsigned numOfPages = fileHandle.numOfPages;
        ushort slotNum = numOfRecords;
        char updatedNumOfRecords = (char)((int)numOfRecords + 1);
        short newSize = freeSize - (short)length;
        if(numOfRecords == 0){

            buffer[2] = updatedNumOfRecords;
            memcpy(buffer, &newSize, sizeof(short));
            memcpy(buffer + 2 + sizeof(int), &length, sizeof(int));
            memcpy(buffer + 2 + 2 * sizeof(int), &SLOTSIZE, sizeof(int));
            offsetPointer=SLOTSIZE;
        }
        else
        {

            memcpy(buffer, &newSize, sizeof(short));
            buffer[2] = updatedNumOfRecords;

            int totalLength = 0;
            int defaultlen = SLOTSIZE;

            //find first open slot where length of record is 0
            for(int i = 0;i < numOfRecords; i++) {
                int l = -1;
                memcpy(&l, buffer + 2 + (8 * i) + sizeof(int), sizeof(int));
                if(l == 0){
                    slotNum = i;
                    break;
                }
            }

            //Find total length of elements to create offset for inserted record
            for(int i = 0;i < numOfRecords; i++) {
                int l;
                memcpy(&l, buffer + 2 + (8 * i) + sizeof(int), sizeof(int));
                totalLength += l;
            }

            //update offset
            offsetPointer=totalLength + SLOTSIZE;
            int total = totalLength + defaultlen;
            memcpy(buffer + 2 + 2 * sizeof(int) + (slotNum) * 8, &total, sizeof(int));
            memcpy(buffer + 2  + sizeof(int) + (slotNum) * 8, &length, sizeof(int));
        }
        return slotNum;
    }

    void RecordBasedFileManager::readSlotDirectory(void* page){
        printf("\nOUTPUTING SLOT DIRECTORY TO CHECK:\n");
        short freeSize;
        memcpy(&freeSize, page, sizeof(short));
        char numOfRecords;
        memcpy(&(numOfRecords), (char*)page + 2, sizeof(char));

        uint8_t* bytePtr = (uint8_t*)page;
        for (size_t i = 0; i < 100; ++i) {
            printf("%02X ", bytePtr[i]);

            if ((i + 1) % 32 == 0) {
                printf("\n");
            }
        }
        printf("\nFIRST 100 BYTES\n");
        for (size_t i = 100; i < 200; ++i) {
            printf("%02X ", bytePtr[i]);

            if ((i + 1) % 32 == 0) {
                printf("\n");
            }
        }

        printf("Elements in slot{%d}, Free space:{%hd}\n", numOfRecords,freeSize);
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        //printf("Start readRecord\n");
        char readBuffer[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum,readBuffer);

        int length;
        memcpy(&length,
               readBuffer + 2 + (sizeof(int)) + (rid.slotNum) * 8 , sizeof(int));

        int totalOffset = 0;

        memcpy(&totalOffset,
               readBuffer + 2 + (2 * sizeof(int)) + (rid.slotNum) * 8 , sizeof(int));

        memcpy(data, readBuffer+totalOffset, length);

        return 0;
    }

    std::vector<int> RecordBasedFileManager::serialize(char* bytes, int size){

        int bits = size * 8;
        // printf("SERIALZING:SIZE OF %lu, %d bit Array: \n}", sizeof(bytes), bits);
        std::vector<int> bitArray(bits);

        int index = 0;
        for (int i = 0; i < size; i++) {

            for (int j = 7; j >= 0; j--) {
                int bit = ((bytes[i] & (1 << j)) != 0 ? 1 : 0);
                bitArray[index] = bit;
                index++;
            }
        }

        return bitArray;
    }
    int RecordBasedFileManager::getRecordSize( const std::vector<Attribute> &recordDescriptor,
                                               const void *data){
        int size = 0;
        int numOfNullBytes = ceil((double)recordDescriptor.size()/8);
        char* dataPointer = (char*)data;
        char nullIndicators[numOfNullBytes];
        for (int i = 0; i < numOfNullBytes; i++) {
            nullIndicators[i] = *dataPointer;
            size += 1;
            dataPointer++;
        }
        std::vector<int> bitArray = serialize(nullIndicators, numOfNullBytes);

        int index = 0;

        for(auto attribute : recordDescriptor){
            if(bitArray[index] == 1){
                // dont increase size if value is null
                index++;
                continue;
            }
            switch (attribute.type) {
                case TypeInt:
                    size += 4;
                    dataPointer +=4;
                    break;
                case TypeReal:
                    size += 4;
                    dataPointer +=4;
                    break;
                case TypeVarChar:
                    int* length = (int*)dataPointer;
                    size += 4;
                    dataPointer += 4;

                    for(int i = 0; i < *length; i++){
                        size += 1;
                        dataPointer++;
                    }
                    break;
            }
            index+=1;
        }
        return size;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {


        int numOfNullBytes = ceil((double)recordDescriptor.size()/8);
        char* dataPointer = (char*)data;

        char nullIndicators[numOfNullBytes];
       // printf("Num of num bytes %d \n", numOfNullBytes);
        for (int i = 0; i < numOfNullBytes; i++) {
            nullIndicators[i] = *dataPointer;
            dataPointer++;
        }

        std::vector<int> bitArray = serialize(nullIndicators, numOfNullBytes);
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
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {

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

