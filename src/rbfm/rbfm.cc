#include "src/include/rbfm.h"
#include <iostream>
#include <cmath>
#include <cstring>


namespace PeterDB {
    int SLOTSIZE = 203;
    int MAX_SLOTS = (SLOTSIZE - 3) / 8;
    int MIN_RECORD_SIZE = 6;
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

//        for(int i = 3;i < slotSize; i++){
//            buffer[i] = 0;
//            buffer[i - 1] = 0;
//        }

        //printf("init page %d, sd %d\n", pageNum, freeSpace);
        fileHandle.writePage(pageNum, buffer);
        free(buffer);
        char* temp[PAGE_SIZE];
        fileHandle.readPage(pageNum, temp);

    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        printf("Start insertRecord:");
        printRecord(recordDescriptor, data, std::cout);
        char buffer[PAGE_SIZE];
        PageNum pageNumber = rid.pageNum;
        int totalNumOfPages = fileHandle.numOfPages;
        int recordSize = getRecordSize(recordDescriptor,data);
        int offsetPointer = 0;
        if(recordSize > 4000){
            printf("WARNING RECORD OVERSIZED CANNOT FIT IN PAGE\n");
            return -1;
        }
        //Minimum record size is 6 bytes
        if(recordSize < 6){
            recordSize = 6;
        }
        //Add new page if no pages.
        if(totalNumOfPages == 0) {
            fileHandle.appendPage(data);
            initSlotDirectory(fileHandle, 0);
        }

        //Check last page (newly appended page) first
        int lastPage = fileHandle.numOfPages - 1;
        rid.pageNum = lastPage;

        fileHandle.readPage(lastPage, buffer);
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

        //check all other pages
        int numOfPages = fileHandle.numOfPages;
        for(int i = 0;i < numOfPages;i++){
            rid.pageNum = i;
            fileHandle.readPage(rid.pageNum, buffer);
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
        //No free space avaliable: Add a new page.
        fileHandle.appendPage(data);
        rid.pageNum = fileHandle.numOfPages - 1;
        initSlotDirectory(fileHandle, rid.pageNum);
        fileHandle.readPage(rid.pageNum, buffer);
        rid.slotNum = addRecordToSlotDirectory(fileHandle, rid, recordSize, buffer, offsetPointer);
        std::memmove(buffer+offsetPointer, data, getRecordSize(recordDescriptor, data));
        fileHandle.writePage(rid.pageNum, buffer);
        return 0;

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
                if(l < 0){
                    printf("TOMBSTONE RECORD DETECTED");
                    l = MIN_RECORD_SIZE;
                }
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

        printf("TRy to read: %d,%d\n", rid.pageNum, rid.slotNum);
        if(rid.pageNum < 0 || rid.pageNum > fileHandle.numOfPages){
            printf("READ FAIL PAGE OUT OF RANGE\n");
            return -1;
        }
        char *readBuffer = (char*) malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum,readBuffer);
        int totalOffset = 0;
        int length;
        memcpy(&length,
               readBuffer + 2 + (sizeof(int)) + (rid.slotNum) * 8 , sizeof(int));
        memcpy(&totalOffset,
               readBuffer + 2 + (2 * sizeof(int)) + (rid.slotNum) * 8 , sizeof(int));


        if(length == 0){
            printf("Record does not exist\n");
            return -1;
        }

        //Read tombstone record
        if(length == -1){
            printf("Record is in tombstone\n");
            RID newRID;
            memcpy(&newRID.pageNum,
                   readBuffer + totalOffset, sizeof(int));
            memcpy(&newRID.slotNum,
                   readBuffer + totalOffset +4, sizeof(short));
            int test = 99;
            //memcpy(readBuffer + totalOffset, &test,3);
            free(readBuffer);
            readRecord(fileHandle, recordDescriptor, newRID, data);

        }else{
            memcpy(data, readBuffer+totalOffset, length);
            printRecord(recordDescriptor, data, std::cout);
            free(readBuffer);
            return 0;
        }

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
        printf("Deleting {%d},{%d}\n",rid.pageNum, rid.slotNum);
        bool isTombstone = false;
        char *buffer = (char*)malloc(4096);
        unsigned pageNumber = rid.pageNum;
        unsigned slotNumber = rid.slotNum;

        fileHandle.readPage(pageNumber, buffer);

        //Get file information
        int currentFileSize = 0;
        int numberOfRecords = 0;
        memcpy(&currentFileSize,buffer, sizeof(short));
        memcpy(&numberOfRecords,buffer + 2,sizeof(char));

        //Get record information
        unsigned offset = 0;
        int length = 0;
        memcpy(&offset, buffer + 2 + 2 * sizeof(int) + (slotNumber) * 8,sizeof(int));
        memcpy(&length,buffer + 2  + sizeof(int) + (slotNumber) * 8, sizeof(int));

        RID tombstoneRID;
        if(length == -1){
            length = MIN_RECORD_SIZE;
            isTombstone = true;
            memmove( &tombstoneRID.pageNum,buffer + offset, 3);
            memmove( &tombstoneRID.slotNum,buffer+offset+3, 3);

        }

        //Erase record, set all bytes to 0 (not necessary)
        memset(buffer + offset, 0, length);

        int newFileSize = currentFileSize + length;
        int updatedNumberOfRecords = numberOfRecords - 1;

        //Update slot directory
        memmove(buffer, &newFileSize, sizeof(short));
        memmove(buffer + 2, &updatedNumberOfRecords, sizeof(char));
        memset(buffer + 2 + 2 * sizeof(int) + (slotNumber) * 8, 0, sizeof(int));
        memset(buffer + 2 + sizeof(int) + (slotNumber) * 8, 0, sizeof(int));


        //Shift slot directory
        printf("SHIFTING dest %d,src %d,len %d\n",offset,offset +length,PAGE_SIZE - offset - length);
        memmove(buffer + offset,
                buffer + offset + length,
                PAGE_SIZE - offset - length);

        //update offsets for shifted slots
        for(int i = rid.slotNum + 1;i < MAX_SLOTS; i++){
            int tempLen = 0;
            memmove(&offset, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
            memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));

            if(tempLen != 0 && tempLen <= currentFileSize){
                unsigned newOffset = offset - length;
                memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                printf("Moving dest %d to src %d\n",offset,offset-length);
            }
        }

        //Write to page with deleted record
        fileHandle.writePage(pageNumber, buffer);
        free(buffer);
        if(isTombstone){
            deleteRecord(fileHandle,recordDescriptor,tombstoneRID);
        }

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        printf("Updating {%d},{%d}\n",rid.pageNum, rid.slotNum);
        char *buffer = (char*) malloc(PAGE_SIZE);
        unsigned pageNumber = rid.pageNum;
        unsigned slotNumber = rid.slotNum;

        fileHandle.readPage(pageNumber, buffer);

        //Get file information
        int currentFreeSpace = 0;
        int numberOfRecords = 0;
        memcpy(&currentFreeSpace,buffer, sizeof(short));
        memcpy(&numberOfRecords,buffer + 2,sizeof(char));

        //Get old record information
        unsigned offset;
        int length;
        memcpy(&offset, buffer + 2 + 2 * sizeof(int) + (slotNumber) * 8,sizeof(int));
        memcpy(&length,buffer + 2  + sizeof(int) + (slotNumber) * 8, sizeof(int));

        //Updated record information
        int updatedRecordSize = getRecordSize(recordDescriptor, data);
        printf("Updating {%d},{%d}, from {size:%d}, to {%d}\n",rid.pageNum, rid.slotNum, length, updatedRecordSize);
        //Scenario 1: Updated Record Size = Old Record Size
        if(length == updatedRecordSize){
            printf("Updating scenario 1\n");
            memcpy(buffer+offset, data, updatedRecordSize);
            fileHandle.writePage(rid.pageNum, buffer);
            free(buffer);
            return 0;
        }
            //Scenario 2: Updated Record Size < Old Record Size
        else if(updatedRecordSize < length){
            printf("Updating scenario 2\n");
            memcpy(buffer+offset, data, updatedRecordSize);
            //shift slot directory
            memmove(buffer  + offset + updatedRecordSize,
                    buffer  + offset + length,
                    PAGE_SIZE  - offset - length);
            //update free space
            int newFreeSpace = currentFreeSpace + length - updatedRecordSize;
            memcpy(buffer, &newFreeSpace, sizeof(short));
            fileHandle.writePage(rid.pageNum, buffer);

            free(buffer);
            return 0;
        }
            //Scenario 3: Updated Record Size > Old Record Size
        else if(updatedRecordSize > length){
            int growSize = updatedRecordSize - length;
            //Scenario 3.1: no free space left on page
            if(growSize > currentFreeSpace){
                printf("Updating scenario 3.1\n");

                free(buffer);
                RID newRID;
                insertRecord(fileHandle, recordDescriptor, data, newRID);

                buffer = (char*) malloc(PAGE_SIZE);
                fileHandle.readPage(rid.pageNum, buffer);

                //Convert record to tombstone
                printf("Converted record to {%d},{%d}\n", newRID.pageNum, newRID.slotNum);
                memcpy(buffer + offset, &newRID.pageNum, sizeof(int));
                memcpy(buffer + offset + 4, &newRID.slotNum, sizeof(short));

                int lengthDifference = abs(6 - length);
                // shift all other records
                memmove(buffer + offset + MIN_RECORD_SIZE,
                        buffer + offset + length,
                        PAGE_SIZE - offset - lengthDifference);

                //update offsets for shifted slots
                for(int i = rid.slotNum + 1;i < MAX_SLOTS; i++){
                    int tempLen = 0;
                    memmove(&offset, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
                    memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));

                    if(tempLen != 0 && tempLen <= currentFreeSpace){
                        unsigned newOffset = offset - lengthDifference;
                        memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                    }
                }
                //Update slot directory length to -1 to indicate tombstone record
                int tombstone = -1;
                memcpy(buffer + 2  + sizeof(int) + (slotNumber) * 8,&tombstone, sizeof(int));

                fileHandle.writePage(rid.pageNum, buffer);

                //free(buffer);
                return 0;
            }
                //Scenario 3.2: shift all slots to add space to updated slot
            else{

                int dest = updatedRecordSize;
                int src = offset;
                int size = PAGE_SIZE - offset - length;
                printf("Updating scenario 3.2 dest{%d}, src{%d}, size{%d}\n", dest, src, size);
                //Shift Slot Directory to make space
                memmove(buffer + offset + updatedRecordSize,
                        buffer  + offset + length,
                        PAGE_SIZE - offset - updatedRecordSize);
                //update offsets
                for(int i = rid.slotNum + 1;i < MAX_SLOTS; i++){
                    int tempOff = 0;
                    int tempLen = 0;
                    memmove(&tempOff, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
                    memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));

                    if(tempLen != 0 && tempLen <= currentFreeSpace){
                        unsigned newOffset = tempOff + growSize;
                        memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                    }
                }

                // copy data to buffer
                memcpy(buffer  + offset, data, updatedRecordSize);

                //Update size of slot directory
                int newFreeSpace = currentFreeSpace - growSize;
                memcpy(buffer, &newFreeSpace, sizeof(short));

                //Update slot directory
                memcpy(buffer + 2 + (rid.slotNum * 8) + sizeof(int) , &updatedRecordSize, sizeof(short));

                fileHandle.writePage(rid.pageNum, buffer);
                free(buffer);
                return 0;
            }
        }

        return 0;

    }


    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {

        printf("read attri{%d},{%d}\n",rid.pageNum, rid.slotNum);
        char buffer[PAGE_SIZE];

        unsigned pageNumber = rid.pageNum;
        unsigned slotNumber = rid.slotNum;

        fileHandle.readPage(pageNumber, buffer);
        int size = 0;
        for(auto attr : recordDescriptor){
            size += attr.length;
        }
        //Get record
        int offset = -1;
        int length;
        memcpy(&offset, buffer + 2 + 2 * sizeof(int) + (slotNumber) * 8, sizeof(int));
        memcpy(&length, buffer + 2 + sizeof(int) + (slotNumber) * 8, sizeof(int));

        void* temp[size];
        memcpy(&temp, buffer + offset, length);
        int numOfNullBytes = ceil((double)recordDescriptor.size()/8);
        char* dataPointer = (char*)temp;

        char nullIndicators[numOfNullBytes];

        for (int i = 0; i < numOfNullBytes; i++) {
            nullIndicators[i] = *dataPointer;
            dataPointer++;
        }

        std::vector<int> bitArray = serialize(nullIndicators, numOfNullBytes);
        int index = 0;
        for (const Attribute& attribute : recordDescriptor){
            if(attribute.name == attributeName){
                if(bitArray[index] == 1){
                    index ++;
                    return 0;
                }
                switch (attribute.type) {
                    case TypeInt:
                        memcpy(data, dataPointer, sizeof(int));
                        break;
                    case TypeReal:
                        memcpy(data, dataPointer, sizeof(int));
                        break;
                    case TypeVarChar:
                        int *length = (int *) dataPointer;
                        dataPointer += 4;
                        memcpy(data, dataPointer, *length);
                        for (int i = 0; i < *length; i++) {
                            dataPointer++;
                        }
                        break;
                }
                return 0;
            }
            if(bitArray[index] == 1){
                index ++;
                continue;
            }
            switch (attribute.type) {
                case TypeInt:
                    dataPointer +=4;
                    break;
                case TypeReal:
                    dataPointer +=4;
                    break;
                case TypeVarChar:
                    int* length= (int*)dataPointer;
                    dataPointer += 4;
                    for(int i = 0; i < *length; i++){
                        dataPointer++;
                    }
                    break;
            }
            index ++;
        }

        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        int numOfPages = fileHandle.numOfPages;
        int size = 0;
        AttrType type;
        for(auto attr : recordDescriptor){
            size += attr.length;
            if(attr.name == conditionAttribute){
                type = attr.type;
            }
        }
        for(int i = 0;i < numOfPages;i++){
            RID tempRID;
            tempRID.pageNum = i;
            for(int j = 0;j < MAX_SLOTS;j++){
                void* data[size];
                tempRID.slotNum = j;
                int read = readRecord(fileHandle, recordDescriptor, tempRID, &data);
                if(read == 0) {
                    void *d[size];
                    readAttribute(fileHandle, recordDescriptor, tempRID, conditionAttribute, d);
                    printf("ATTR READ:{%d}\n", *(int *) d);
                    switch (type) {
                        case TypeInt:
                            if (*(int *) value == *(int *) d) {
                                rbfm_ScanIterator.recordRIDS.push_back(tempRID);
                            }
                            break;
                        case TypeReal:
                            if (*(float *) value == *(float *) d) {
                                rbfm_ScanIterator.recordRIDS.push_back(tempRID);
                            }
                            break;
                        case TypeVarChar:
                            if (*(char *) value == *(char *) d) {
                                rbfm_ScanIterator.recordRIDS.push_back(tempRID);
                            }
                            break;
                    }
                }
            }
        }
        for(auto RID : rbfm_ScanIterator.recordRIDS){
            printf("ROD:{%d}{%d}\n", RID.pageNum,RID.slotNum);
        }

        return 0;
    }

} // namespace PeterDB

