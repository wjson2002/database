#include "src/include/rbfm.h"
#include <iostream>
#include <cmath>
#include <cstring>


namespace PeterDB {
    int SLOTSIZE = 325;
    int MAX_SLOTS = ((SLOTSIZE - 3) / 8) - 1;
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
        char buffer[PAGE_SIZE];
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
        char numOfRecords;
        memmove(&(numOfRecords), buffer + 2, sizeof(char));
        if(freeSpace >= recordSize && (int)numOfRecords < MAX_SLOTS){
            rid.slotNum = addRecordToSlotDirectory(fileHandle, rid,
                                                   recordSize, buffer, offsetPointer);

            std::memmove(buffer+offsetPointer, data, recordSize);
            std::memmove(buffer+offsetPointer, data, recordSize);
            fileHandle.writePage(rid.pageNum, buffer);
            //printf("Inserted to Last{%d},{%d} ",rid.pageNum, rid.slotNum);
            return 0;
        }

        //check all other pages
        int numOfPages = fileHandle.numOfPages;
        for(int i = 0;i < numOfPages;i++){
            rid.pageNum = i;
            fileHandle.readPage(rid.pageNum, buffer);
            std::memmove(&freeSpace, buffer, sizeof(short));
            memmove(&(numOfRecords), buffer + 2, sizeof(char));

            if(freeSpace >= recordSize && (int)numOfRecords < MAX_SLOTS){
                rid.slotNum = addRecordToSlotDirectory(fileHandle, rid,
                                                       recordSize, buffer, offsetPointer);

                std::memmove(buffer+offsetPointer, data, recordSize);
                std::memmove(buffer+offsetPointer, data, recordSize);
                fileHandle.writePage(rid.pageNum, buffer);
                //printf("Inserted to mid{%d},{%d} ", rid.pageNum, rid.slotNum);
                return 0;
            }
        }
        //No free space avaliable or no slots left: Add a new page.
        fileHandle.appendPage(data);
        rid.pageNum = fileHandle.numOfPages - 1;
        initSlotDirectory(fileHandle, rid.pageNum);
        fileHandle.readPage(rid.pageNum, buffer);
        rid.slotNum = addRecordToSlotDirectory(fileHandle, rid, recordSize, buffer, offsetPointer);
        std::memmove(buffer+offsetPointer, data, getRecordSize(recordDescriptor, data));
        fileHandle.writePage(rid.pageNum, buffer);
        // printf("Insert to New{%d},{%d} ", rid.pageNum, rid.slotNum);
        return 0;

    }

    unsigned short RecordBasedFileManager::addRecordToSlotDirectory(FileHandle &fileHandle, RID &rid,
                                                                    int length, char (&buffer)[PAGE_SIZE], int &offsetPointer) {
        short freeSize;
        memmove(&freeSize, buffer, sizeof(short));
        char numOfRecords;
        memmove(&(numOfRecords), buffer + 2, sizeof(char));

        unsigned numOfPages = fileHandle.numOfPages;
        ushort slotNum = numOfRecords;
        char updatedNumOfRecords = (char)((int)numOfRecords + 1);
        short newSize = freeSize - (short)length;
        if(numOfRecords == 0){

            buffer[2] = updatedNumOfRecords;
            memmove(buffer, &newSize, sizeof(short));
            memmove(buffer + 2 + sizeof(int), &length, sizeof(int));
            memmove(buffer + 2 + 2 * sizeof(int), &SLOTSIZE, sizeof(int));
            offsetPointer=SLOTSIZE;
        }
        else
        {
            memmove(buffer, &newSize, sizeof(short));
            buffer[2] = updatedNumOfRecords;

            int totalLength = 0;
            int defaultlen = SLOTSIZE;

            //find first open slot where length of record is 0
            for(int i = 0;i < numOfRecords; i++) {
                int l = -1;
                memmove(&l, buffer + 2 + (8 * i) + sizeof(int), sizeof(int));
                if(l == 0){
                    slotNum = i;
                    break;
                }
            }

            //Find total length of elements to create offset for inserted record
            int iter = numOfRecords;
            for(int i = 0;i < iter; i++) {
                int l;
                memmove(&l, buffer + 2 + (8 * i) + sizeof(int), sizeof(int));
                if(l == -1){
                    l = MIN_RECORD_SIZE;
                }
                if(l > 0 && l < PAGE_SIZE){
                    totalLength += l;
                }
                else{
                    iter+=1;
                }

            }

            //update offset
            offsetPointer=totalLength + SLOTSIZE;
            int total = totalLength + defaultlen;
            memmove(buffer + 2 + 2 * sizeof(int) + (slotNum) * 8, &total, sizeof(int));
            memmove(buffer + 2  + sizeof(int) + (slotNum) * 8, &length, sizeof(int));
        }
        return slotNum;
    }


    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {

        //printf("TRy to read: %d,%d\n", rid.pageNum, rid.slotNum);
        if(rid.pageNum < 0){
            printf("READ FAIL PAGE OUT OF RANGE\n");
            return -1;
        }
        char *readBuffer = (char*) malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum,readBuffer);
        char numOfRecords;
        memmove(&(numOfRecords), readBuffer + 2, sizeof(char));
        int totalOffset = 0;
        int length;

        memcpy(&length,
               readBuffer + 2 + (sizeof(int)) + (rid.slotNum) * 8 , sizeof(int));
        memcpy(&totalOffset,
               readBuffer + 2 + (2 * sizeof(int)) + (rid.slotNum) * 8 , sizeof(int));


        if(length == 0 || length >= PAGE_SIZE || length < -1){
            // printf("Record does not exist\n");
            return -1;
        }

        //Read tombstone record
        if(length == -1){

            RID newRID;
            memcpy(&newRID.pageNum,
                   readBuffer + totalOffset, sizeof(int));
            memcpy(&newRID.slotNum,
                   readBuffer + totalOffset +4, sizeof(short));
            //printf("Record is in tombstone, going to {%d}, {%d}\n", newRID.pageNum, newRID.slotNum);
            free(readBuffer);
            readRecord(fileHandle, recordDescriptor, newRID, data);
            // printf("Record found\n");

            return 0;

        }else if (rid.slotNum <= (int)numOfRecords){
            memcpy(data, readBuffer+totalOffset, length);
//            printf("Reading {%d},{%d} ", rid.pageNum, rid.slotNum);
//            printf("Offset: {%d}\n", totalOffset);
            free(readBuffer);
            return 0;
        }
        else{
            return -1;
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
        printf("Deleting {%d},{%d}, {%d},{%d}: numRecords:%d \n",rid.pageNum, rid.slotNum, offset, length, numberOfRecords);
        RID tombstoneRID;
        if(length == -1){
            length = MIN_RECORD_SIZE;
            isTombstone = true;
            memmove( &tombstoneRID.pageNum,buffer + offset, 4);
            memmove( &tombstoneRID.slotNum,buffer+offset+4, 2);
            //printf("Record is tombstone:{%d},{%d}\n",tombstoneRID.pageNum, tombstoneRID.slotNum);

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
        //printf("SHIFTING dest %d,src %d,len %d\n",offset,offset +length,PAGE_SIZE - offset - length);
        memmove(buffer + offset,
                buffer + offset + length,
                PAGE_SIZE - offset - length);

        //update offsets for shifted slots
        int start = slotNumber + 1;
        for(int i = start;i < (numberOfRecords + slotNumber);i++){

            if(i >= MAX_SLOTS){
                break;
            }
            int tempLen = 0;
            memmove(&offset, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
            memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));
            printf("Num of Records%d: shifting %d off%d len%d",numberOfRecords, i, offset, length);
            if(offset <= SLOTSIZE || offset >= PAGE_SIZE - SLOTSIZE){
                continue;
            }
            if((tempLen > 0 && tempLen <= PAGE_SIZE) || tempLen == -1){
                unsigned newOffset = offset - length;
                printf("->newoffset:%d\n", newOffset);
                memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                //printf("Moving slot{%d}of size{%d} dest %d to src %d\n",i+slotNumber,tempLen,offset,offset-length);
            }
            if(tempLen == 0){
                numberOfRecords += 1;
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
        printf("Updating {%d},{%d}: ",rid.pageNum, rid.slotNum);
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
        //printf("Updating {%d},{%d}, from {size:%d}, to {%d}\n",rid.pageNum, rid.slotNum, length, updatedRecordSize);
        //Scenario 1: Updated Record Size = Old Record Size
        if(length == updatedRecordSize){
            // printf("Updating scenario 1");
            memcpy(buffer+offset, data, updatedRecordSize);
            fileHandle.writePage(rid.pageNum, buffer);
            free(buffer);
            return 0;
        }
            //Scenario 2: Updated Record Size < Old Record Size
        else if(updatedRecordSize < length){
            // printf("Updating scenario 2");
            memcpy(buffer+offset, data, updatedRecordSize);
            //shift slot directory
            memmove(buffer + offset + updatedRecordSize,
                    buffer + offset + length,
                    PAGE_SIZE  - offset - length);

            int start = slotNumber + 1;
            for(int i = start;i < (numberOfRecords + slotNumber);i++){

                if(i >= MAX_SLOTS){
                    break;
                }
                int tempLen = 0;
                memmove(&offset, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
                memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));

                if((tempLen > 0 && tempLen <= PAGE_SIZE) || tempLen == -1){
                    unsigned newOffset = offset - length + updatedRecordSize;
                    memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                    // printf("Moving slot{%d} of size{%d} from %d to  %d\n",i,tempLen, offset,newOffset);
                }
                if(tempLen == 0){
                    numberOfRecords++;
                }
            }

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
                //printf("Scenario 3.1: Add new Page->");

                free(buffer);
                RID newRID;
                insertRecord(fileHandle, recordDescriptor, data, newRID);

                buffer = (char*) malloc(PAGE_SIZE);
                fileHandle.readPage(rid.pageNum, buffer);

                //Convert record to tombstone
                // printf("Converted record to {%d},{%d}", newRID.pageNum, newRID.slotNum);
                memcpy(buffer + offset, &newRID.pageNum, sizeof(int));
                memcpy(buffer + offset + 4, &newRID.slotNum, sizeof(short));
                if(slotNumber < numberOfRecords){
                    int lengthDifference = abs(6 - length);
                    // shift all other records
                    memmove(buffer + offset + MIN_RECORD_SIZE,
                            buffer + offset + length,
                            PAGE_SIZE - offset - lengthDifference);

                    //update offsets for shifted slots
                    int start = slotNumber + 1;
                    for(int i = start;i < (numberOfRecords+ slotNumber);i++){

                        if(i >= MAX_SLOTS){
                            break;
                        }

                        int tempLen = 0;
                        memmove(&offset, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
                        memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));

                        if(offset <= SLOTSIZE || offset >= PAGE_SIZE - SLOTSIZE){
                            continue;
                        }

                        if(tempLen != 0 && tempLen <= PAGE_SIZE){
                            unsigned newOffset = offset - lengthDifference;
                            memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                            printf("shifting slot{%d}len{%d} from{%d}to{%d}\n", i, tempLen,offset, newOffset);
                        }
                        if(tempLen == 0){
                            numberOfRecords++;
                        }
                    }
                }
                //Update slot directory length to -1 to indicate tombstone record
                int tombstone = -1;
                memcpy(buffer + 2  + sizeof(int) + (slotNumber) * 8,&tombstone, sizeof(int));
                printf("Set Slot num%d to -1\n", slotNumber);
                fileHandle.writePage(rid.pageNum, buffer);

                //free(buffer);
                return 0;
            }
                //Scenario 3.2: shift all slots to add space to updated slot
            else{
                int src = offset;
                //printf("Updating scenario 3.2:Make more Space-> offset{%d}, size{%d}",src, updatedRecordSize);
                //Shift Slot Directory to make space
                memmove(buffer + offset + updatedRecordSize,
                        buffer + offset + length,
                        PAGE_SIZE - offset - updatedRecordSize);
                //update offsets
                for(int i = rid.slotNum + 1;i < numberOfRecords; i++){
                    int tempOff = 0;
                    int tempLen = 0;
                    memmove(&tempOff, buffer + 2 + 2 * sizeof(int) + (i) * 8,sizeof(int));
                    memmove(&tempLen, buffer + 2 + sizeof(int) + (i) * 8,sizeof(int));

                    if((tempLen > 0 && tempLen < PAGE_SIZE) || tempLen == -1){
                        unsigned newOffset = tempOff + growSize;
                        memmove(buffer + 2 + 2 * sizeof(int) + (i) * 8,&newOffset,sizeof(int));
                    }
                    if(tempLen == 0){
                        numberOfRecords ++;
                    }
                }

                // copy data to buffer
                memcpy(buffer + offset, data, updatedRecordSize);

                //Update size of slot directory
                int newFreeSpace = currentFreeSpace - growSize;
                memcpy(buffer, &newFreeSpace, sizeof(short));

                //Update slot directory
                memcpy(buffer + 2 + (rid.slotNum * 8) + sizeof(int) , &updatedRecordSize, sizeof(short));
                // printf("3.1 Page info {%d} Updated to: {%d}\n", newFreeSpace, updatedRecordSize);
                fileHandle.writePage(rid.pageNum, buffer);
                free(buffer);
                return 0;
            }
        }

        return 0;

    }


    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        if(rid.pageNum < 0){
            //printf("READATTR FAIL PAGE OUT OF RANGE\n");
            return -1;
        }

        char *readBuffer = (char*) malloc(PAGE_SIZE);
        unsigned pageNumber = rid.pageNum;
        unsigned slotNumber = rid.slotNum;
        fileHandle.readPage(pageNumber, readBuffer);

        int size = 0;
        for(auto attr : recordDescriptor){
            size += attr.length;
        }
        //Get record
        int offset = -1;
        int length;
        memcpy(&length, readBuffer + 2 + sizeof(int) + (slotNumber) * 8, sizeof(int));
        memcpy(&offset, readBuffer + 2 + (2 * sizeof(int)) + (slotNumber) * 8, sizeof(int));

        char numOfRecords;
        memmove(&(numOfRecords), readBuffer + 2, sizeof(char));


        if(length == 0 || length >= PAGE_SIZE || length < -1){
            // printf("Record does not exist\n");
            return -1;
        }

        //Read tombstone record
        if(length == -1){
            RID newRID;
            memcpy(&newRID.pageNum,
                   readBuffer + offset, sizeof(int));
            memcpy(&newRID.slotNum,
                   readBuffer + offset + 4, sizeof(short));

            free(readBuffer);
            readAttribute(fileHandle, recordDescriptor, newRID, attributeName, data);

            return 0;

        }else if (rid.slotNum <= (int)numOfRecords){
            void* temp[size];
            memcpy(&temp, readBuffer + offset, length);
            int numOfNullBytes = ceil((double)recordDescriptor.size()/8);

            char* dataPointer = (char*)temp;

            char nullIndicators[numOfNullBytes];

            for (int i = 0; i < numOfNullBytes; i++) {
                nullIndicators[i] = *dataPointer;
                dataPointer++;
            }

            std::vector<int> bitArray = serialize(nullIndicators, numOfNullBytes);
            int index = 0;
            float floatValue;
            for (const Attribute& attribute : recordDescriptor){
                if(attribute.name == attributeName){
                    //Allocate 1 byte for null
                    if(bitArray[index] == 1){
                        index ++;
                        memset(data, 128u, 1);
                        return 0;
                    }
                    else{
                        memset(data, 0, 1);
                    }
                    switch (attribute.type) {
                        case TypeInt:
                            memcpy((char*)data + 1, dataPointer, sizeof(int));
                            break;
                        case TypeReal:
                            memcpy((char*)data + 1, dataPointer, sizeof(int));
                            break;
                        case TypeVarChar:
                            int *len= (int *) dataPointer;
                            dataPointer += 4;
                            memcpy((char*)data + 1, len, sizeof(int));
                            memcpy((char*)data + 5, dataPointer, *len);
                            for (int i = 0; i < *len; i++) {
                                dataPointer++;
                            }
                            break;
                    }
                    free(readBuffer);
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
            free(readBuffer);
            return 0;
        }
        else{
            return -1;
        }

    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {


        RID start;
        start.pageNum = 0;
        start.slotNum = 0;
        rbfm_ScanIterator.fileHandle = fileHandle;
        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
        rbfm_ScanIterator.currentRID = start;
        rbfm_ScanIterator.compOp = compOp;
        rbfm_ScanIterator.numOfPages = fileHandle.numOfPages;
        rbfm_ScanIterator.conditionAttribute = conditionAttribute;
        rbfm_ScanIterator.value = value;
        rbfm_ScanIterator.fileName = fileHandle.FileName;
        rbfm_ScanIterator.attributeNames = attributeNames;

        for(auto attr : recordDescriptor) {
            if(attr.name == conditionAttribute) {
                rbfm_ScanIterator.attrType = attr.type;
                rbfm_ScanIterator.attrLength = attr.length;
            }
        }
        return 0;

    }


    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rbfm.openFile(fileName, fileHandle);
        unsigned pageNum = currentRID.pageNum;
        if(pageNum >= numOfPages){
           // printf("return -1\n");
            rbfm.closeFile(fileHandle);
            return RBFM_EOF;
        }
        else{
            while (currentRID.pageNum < numOfPages){

                char buffer[PAGE_SIZE];
                fileHandle.readPage(pageNum, buffer);
                char numOfRecords;
                memmove(&(numOfRecords), buffer + 2, sizeof(char));

                while(currentRID.slotNum < (int)numOfRecords){
                    void* d[attrLength + 1];
                    memset(d, -1, attrLength + 1);
                    int read = rbfm.readAttribute(fileHandle, recordDescriptor, currentRID,conditionAttribute,d);

                    if(read == 0) {
                        char temp;
                        memmove(&temp, &d, 1);
                        char *pointer;
                        if (temp == 1) {
                            pointer = nullptr;
                        } else {
                            pointer = (char *) d + 1;
                        }
                        if (attrType == TypeInt) {
                            //printf("Read Int:{%d}, {%d}, {%d}, {%d}\n", currentRID.pageNum, currentRID.slotNum, *(int *)pointer, *(int *)value);
                            if (rbfm.compareNums((int *) pointer, (int *) value, compOp, TypeInt)) {
                                //printf("Macth found:{%d}, {%d}}\n", *(int*)pointer, *(int*)value);
                                rbfm.readRecord(fileHandle, recordDescriptor, currentRID, data);
                                rid = currentRID;
                                currentRID.slotNum += 1;
                                rbfm.closeFile(fileHandle);

                                return 0;
                            }
                        } else if (attrType == TypeReal) {
                            // printf("Read Float:{%d}, {%d}\n", currentRID.pageNum, currentRID.slotNum);
                            if (rbfm.compareNums((float *) pointer, (float *) value, compOp, TypeReal)) {
                                // printf("Macth found:{%f}, {%f}}\n", *(float*)pointer, *(float*)value);

                                rbfm.readRecord(fileHandle, recordDescriptor, currentRID, data);
                                rid = currentRID;
                                currentRID.slotNum += 1;
                                rbfm.closeFile(fileHandle);

                                return 0;
                            }

                        } else if (attrType == TypeVarChar) {
                            char* valuePointer = (char*)value;
                            if(pointer == nullptr){
                                return true;
                            }
                            char* pointerPointer = pointer;
                            int strLen;
                            memcpy(&strLen, pointer, sizeof(int));
                            printf("Real len{%d} ",strLen);
                            char* string = new char[strLen + 1];
                            valuePointer+=4;
                            pointerPointer+=4;
                            if(strLen > 0){
                                memmove(string, pointerPointer, strLen);
                                string[strLen] = '\0';

                            }
                            else{
                                string[strLen] = '\0';
                            }

                            if (rbfm.compareString((char *) string, (char *) valuePointer, compOp)) {
                                printf("getting rid {%d,%d}", currentRID.pageNum,currentRID.slotNum);
                                rbfm.readRecord(fileHandle, recordDescriptor, currentRID, data);
                                rid = currentRID;
                                currentRID.slotNum += 1;
                                rbfm.closeFile(fileHandle);
                                //free(string);
                                return 0;
                            }
                            //printf("FALSE\n");
                        }
                    }
                    currentRID.slotNum += 1;

                }
                currentRID.pageNum += 1;
                currentRID.slotNum = 0;
            }
            rbfm.closeFile(fileHandle);
            return RBFM_EOF;
        }
    };


    template <typename T>
    bool RecordBasedFileManager::compareNums(T* value1, T* value2, CompOp compOp, AttrType type){
        if(compOp == NO_OP){
            return true;
        }
        if(type == TypeInt){
            if(value1 == nullptr || value2 == nullptr){
                return false;
            }
            else{
                int a = *value1;
                int b = *value2;
                if (compOp == EQ_OP){return (a == b);}
                else if (compOp == LT_OP){return (a < b);}
                else if (compOp == LE_OP){return (a <= b);}
                else if (compOp == GT_OP){return (a > b);}
                else if (compOp == GE_OP){return (a >= b);}
                else if (compOp == NE_OP){return (a != b);}
                else if (compOp == NO_OP){return true;}
                else {return false;}
            }
        }
        else if(type == TypeReal){
            if(value1 == nullptr || value2 == nullptr){
                return false;
            }
            else{
                float a = *value1;
                float b = *value2;
                if (compOp == EQ_OP){return (a == b);}
                else if (compOp == LT_OP){return (a < b);}
                else if (compOp == LE_OP){return (a <= b);}
                else if (compOp == GT_OP){return (a > b);}
                else if (compOp == GE_OP){return (a >= b);}
                else if (compOp == NE_OP){return (a != b);}
                else if (compOp == NO_OP){return true;}
                else {return false;}
            }
        }
        return false;
    }

    bool RecordBasedFileManager::compareString(char* value1, char* value2, CompOp compOp){
        // Simplify null checks
        if (value1 == nullptr || value2 == nullptr) {
            return (compOp == NO_OP);
        }
        if(strlen(value1) == 0){
            printf("empty string");
            return false;
        }

        // Print as String
        int result = strcmp(value1, value2);
        printf("comparing {%s}, {%s} , {%d}: %zu", value1, value2,result, strlen(value1));
        if(checkString(value1, strlen(value1))){
            return false;
        }

        switch (compOp) {
            case EQ_OP: return (result == 0);
            case LT_OP: return (result < 0);
            case LE_OP: return (result <= 0);
            case GT_OP: return (result > 0);
            case GE_OP: return (result >= 0);
            case NE_OP: return (result != 0);
            case NO_OP: return true;
            default: return false;
        }

    }

    bool RecordBasedFileManager::checkString(const char* string, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            if (string[i] != '\xFF') {
                return false;
            }
        }
        return true;
    }
} // namespace PeterDB