#include "src/include/rm.h"
#include <map>
#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

namespace PeterDB {
    std::string DEFAULT_TABLES_NAME = "Tables";
    std::vector<Attribute> tableRecordDescriptor = {{"ID", TypeInt, sizeof(int)},
                                                    {"Name", TypeVarChar, 50},
                                                    {"Filename",TypeVarChar, 80}};
    std::string DEFAULT_ATTRIBUTE_NAME = "Attributes";
    std::vector<Attribute> attributeRecordDescriptor = {{"ID", TypeInt, sizeof(int)},
                                                    {"Name", TypeVarChar, 50},
                                                    {"Type",TypeInt, sizeof(int)},
                                                    {"Position",TypeInt, sizeof(int)}};

    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();

        bool CatalogActive = false;
        FileHandle tableFileHandle;
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        //Create File for tables & attributes
        printf("Creating Catalog\n");
        if(this->CatalogActive){
            printf("Catalog already created\n");
            return -1;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        this->CatalogActive = true;
        rbfm.createFile(DEFAULT_TABLES_NAME);
        rbfm.openFile(DEFAULT_TABLES_NAME,tableFileHandle);

        RID rid;
        std::string data[] = {"0", "Tables", "Table.bin"};
        auto result = convert(tableRecordDescriptor, data);
        rbfm.insertRecord(tableFileHandle, tableRecordDescriptor, result, rid);
        std::string attributesData[] = {"1", "Attributes", "Attributes.bin"};
        auto attributeBytes = convert(tableRecordDescriptor, attributesData);
        rbfm.insertRecord(tableFileHandle, tableRecordDescriptor, attributeBytes, rid);


        //create attribute file
        RecordBasedFileManager::instance().createFile(DEFAULT_ATTRIBUTE_NAME);

        RecordBasedFileManager::instance().openFile(DEFAULT_ATTRIBUTE_NAME,
                                                    attributeFileHandle);
        std::string tableAttr0[] = {"0", "id", "0", "0"};
        auto bytes = convert(attributeRecordDescriptor,  tableAttr0);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);

        std::string tableAttr1[] = {"0", "name", "2", "1"};
        bytes = convert(attributeRecordDescriptor,  tableAttr1);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);

        std::string tableAttr2[] = {"0", "filename", "2", "2"};
        bytes = convert(attributeRecordDescriptor,  tableAttr2);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);

        std::string AttrAttr0[] = {"1", "table_id", "0", "0"};
        bytes = convert(attributeRecordDescriptor,   AttrAttr0);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);

        std::string AttrAttr1[] = {"1", "name", "0", "1"};
        bytes = convert(attributeRecordDescriptor,   AttrAttr1);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);
        std::string AttrAttr2[] = {"1", "type", "0", "2"};
        bytes = convert(attributeRecordDescriptor,   AttrAttr2);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);
        std::string AttrAttr3[] = {"1", "pos", "0", "3"};
        bytes = convert(attributeRecordDescriptor,   AttrAttr3);
        rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        if(CatalogActive == false){
            printf("Catalog not active\n");
            return -1;
        }
        this->CatalogActive = false;
        int deleteTables = RecordBasedFileManager::instance().destroyFile(DEFAULT_TABLES_NAME);
        int deleteAttributes = RecordBasedFileManager::instance().destroyFile(DEFAULT_ATTRIBUTE_NAME);
        if(deleteTables != 0){
            printf("Failed to delete Table table");
        }
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if(!this->CatalogActive){
            printf("Catalog not active\n");
            return -1;
        }

        int createRBFM = RecordBasedFileManager::instance().createFile(tableName);
        if(createRBFM != 0){
            return -1;
        }

        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        int deleteRBFMfile = RecordBasedFileManager::instance().destroyFile(tableName);
        if(deleteRBFMfile != 0){
            return -1;
        }

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        void* value[100];
        RBFM_ScanIterator Iterator = RBFM_ScanIterator();
        std::vector<std::string> attributeNames;
        rbfm.scan(tableFileHandle, tableRecordDescriptor,
                  "ID", EQ_OP, value, attributeNames,
                  Iterator);
        std::vector<Attribute> recordDescriptor = getRecordDescriptor(1);
        for(auto RID : Iterator.recordRIDS){
            void* temp[100];
            printf("ROD:{%d}{%d}:", RID.pageNum,RID.slotNum);
            rbfm.readRecord(attributeFileHandle, attributeRecordDescriptor, RID, temp);
            Attribute tempAttr = convertBytesToAttributes(attributeRecordDescriptor, temp);
            printf("ATTRIBUTE {%s} {%d} \n",tempAttr.name.c_str(),tempAttr.type);
        }

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        int openFile = RecordBasedFileManager::instance().openFile(tableName, currentFileHandle);
        if(openFile != 0){
            printf("Open file error for insertTuple\n");
            return -1;
        }
        //RecordBasedFileManager::instance().insertRecord(currentFileHandle, )


        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }
    void* RelationManager::convert(std::vector<Attribute>& recordDescriptor, const std::string data[]) {
        int numOfNullBytes = ceil((double)recordDescriptor.size() / 8);
        int totalSize = 0;
        int index = 0;

        for (const auto& attribute : recordDescriptor) {
            switch (attribute.type) {
                case TypeInt:
                    totalSize += sizeof(int);
                    break;
                case TypeReal:
                    totalSize += sizeof(float);
                    break;
                case TypeVarChar:
                    totalSize += sizeof(int) + data[index].length();
                    break;
            }
            index++;
        }

        char* result = new char[totalSize +  numOfNullBytes];
        char* resultPointer = result;

        for (int i = 0; i < numOfNullBytes; i++) {
            int temp = 0;
            memcpy(resultPointer, &temp, 1);
            resultPointer+= 1;
        }

        index = 0;
        for (const auto& attribute : recordDescriptor) {
            switch (attribute.type) {
                case TypeInt: {
                    int converted = stoi(data[index]);
                    memcpy(resultPointer, &converted, sizeof(int));
                    resultPointer += sizeof(int);
                }
                    break;
                case TypeReal: {
                    float converted = stof(data[index]);
                    memcpy(resultPointer, &converted, sizeof(float));
                    resultPointer += sizeof(float);
                }
                    break;
                case TypeVarChar: {
                    int length = (int)(data[index].length());
                    memcpy(resultPointer, &length, sizeof(int));
                    resultPointer += sizeof(int);

                    memcpy(resultPointer, data[index].c_str(), length);
                    resultPointer += length;
                }
                    break;
            }
            index++;
        }
        return result;
    }
    Attribute RelationManager::convertBytesToAttributes(std::vector<Attribute>& recordDescriptor, void* data){
        Attribute result;

        int numOfNullBytes = ceil((double)recordDescriptor.size()/8);
        char* dataPointer = (char*)data;
        char nullIndicators[numOfNullBytes];
        for (int i = 0; i < numOfNullBytes; i++) {
            nullIndicators[i] = *dataPointer;

            dataPointer++;
        }
        std::vector<int> bitArray = RecordBasedFileManager::instance().serialize(nullIndicators,
                                                                                 numOfNullBytes);
        dataPointer += sizeof(int); // table id

        int length = *(int*)dataPointer;
        char name[length + 1];
        dataPointer += sizeof(int);
        memcpy(&name, dataPointer, length);
        name[length] = '\0';
        dataPointer += length;
        result.name = std::string(name);
        printf("ATTR NAME: {%s}\n", result.name.c_str());
        int type = *(int*)dataPointer;
        AttrType attrtype;
        dataPointer += 4;
        switch (type) {
            case 0:
                attrtype = TypeInt;
                break;
            case 1:
                attrtype = TypeReal;
                break;
            case 2:
                attrtype = TypeVarChar;
                break;
        }
        result.type = attrtype;
        result.length = 30;

        return result;
    }


    std::vector<Attribute> RelationManager::getRecordDescriptor(int table_id){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int value = table_id;
        RBFM_ScanIterator Iterator = RBFM_ScanIterator();
        std::vector<std::string> attributeNames;
        rbfm.scan(tableFileHandle, attributeRecordDescriptor,
                  "ID", EQ_OP, &value, attributeNames,
                  Iterator);
        std::vector<Attribute> result;
        for(auto RID : Iterator.recordRIDS){
            void* temp[100];
            printf("RD ROD:{%d}{%d}:", RID.pageNum,RID.slotNum);
            rbfm.readRecord(attributeFileHandle, attributeRecordDescriptor, RID, temp);
            Attribute tempAttr = convertBytesToAttributes(attributeRecordDescriptor, temp);
            printf("ATTRIBUTE {%s} {%d} \n",tempAttr.name.c_str(),tempAttr.type);
            result.push_back(tempAttr);
        }
        return result;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB