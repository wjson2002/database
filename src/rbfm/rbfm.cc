#include "src/include/rbfm.h"
#include <iostream>
#include <cmath>

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

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        printf("Start insertRecord\n");
        printf("RD size: %zu\n", recordDescriptor.size());


        return -1;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        printf("Start readRecord\n");
        return -1;
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
        printf(" bitArray: ");
        for(auto i : bitArray){
            printf("%d", i);
        }

        int index = 0;
        for (const Attribute& attribute : recordDescriptor){
            std::cout << attribute.name.c_str();
            out << attribute.name.c_str();
            printf("value of bit arrry:%d \n",bitArray[index]);
            if(bitArray[index] == 1){
                std::cout << ": NULL";
                out << ": NULL";\
                if(index == recordDescriptor.size() - 1){
                    std::cout<< "\n";
                    out<< "\n";
                }
                else{
                    std::cout<< ", ";
                    out<< ", ";
                }
                index ++;
                continue;
            }

            switch (attribute.type) {
                case TypeInt:
                    std::cout << ": " << *(int*)dataPointer;
                    out << ": " << *(int*)dataPointer;
                    dataPointer +=4;
                    break;
                case TypeReal:
                    std::cout<< ": " << *(float*)dataPointer;
                    out<< ": " << *(float*)dataPointer;
                    dataPointer +=4;
                    break;
                case TypeVarChar:
                    std::cout << ": ";
                    out << ": ";
                    int* length= (int*)dataPointer;
                    dataPointer += 4;
                    for(int i = 0; i < *length; i++){
                        std::cout<<*dataPointer;
                        out<<*dataPointer;
                        dataPointer++;
                    }
                    break;
            }

            if(index == recordDescriptor.size() - 1){
                std::cout<< "\n";
                out<< "\n";
            }
            else{
                std::cout<< ", ";
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

