#include "src/include/rm.h"

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
        initCatalogTables();

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        printf("Deleting Catalog\n");

//        if(CatalogActive == false){
//            printf("Catalog not active\n");
//            return -1;
//        }
        this->CatalogActive = false;
        RecordBasedFileManager::instance().destroyFile(DEFAULT_TABLES_NAME);
        RecordBasedFileManager::instance().destroyFile(DEFAULT_ATTRIBUTE_NAME);


        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        printf("Creating new Table: {%s}\n", tableName.c_str());
        if(!this->CatalogActive){
            printf("Catalog not active\n");
            return -1;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int index = tableIDmap.size();
        if(tableName == DEFAULT_TABLES_NAME) {
            index = 0;

        }
        else if(tableName == DEFAULT_ATTRIBUTE_NAME){
            index = 1;
        }
        else{
            FileHandle newTableFileHandle;
            rbfm.createFile(tableName);
            rbfm.openFile(tableName,newTableFileHandle);
            tableIDmap.insert({index, newTableFileHandle});
        }
        tableNameToIdMap.insert({tableName, index});
        RID rid;
        // Insert records into Table
        std::string tableIndex = std::to_string(index);
        std::string data[] = {tableIndex, tableName, tableName+".bin"};
        auto result = convert(tableRecordDescriptor, data);
        rbfm.insertRecord(tableFileHandle, tableRecordDescriptor, result, rid);

        // Insert Record into Attributes
        int position = 0;
        for(auto attr: attrs){
            std::string attrData[] = {tableIndex, attr.name,
                                  std::to_string(attr.type),
                                  std::to_string(position)};
            auto bytes = convert(attributeRecordDescriptor, attrData);
            rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);
            position += 1;
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
        //printf("Loojing for {%s}", tableName.c_str());
        rbfm.scan(tableFileHandle, tableRecordDescriptor,
                  "Name", EQ_OP, tableName.c_str(), attributeNames,
                  Iterator);

        RID rid = Iterator.recordRIDS[0];

        rbfm.readRecord(tableFileHandle, tableRecordDescriptor, rid, &value);
        char* pointer = (char*)value;
        int tableID = *(int*)(pointer + 1);

        //Need to get table ID
        //printf("GEtting attribute of tableID: {%d}\n", tableID);
        std::vector<Attribute> recordDescriptor = getRecordDescriptor(tableID);

        attrs = recordDescriptor;
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::vector<std::string> attributeNames;
        void* value[100];


        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);

        rbfm.insertRecord(fh, recordD, data, rid);

        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);

        rbfm.deleteRecord(fh, recordD, rid);

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);

        rbfm.updateRecord(fh, recordD, data, rid);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);

        int result = rbfm.readRecord(fh, recordD, rid, data);
        if(result == 0){
            return 0;
        }
        else{
            return -1;
        }
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rbfm.printRecord(attrs, data, out);
        return 0;
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

    RC RelationManager::initCatalogTables(){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rbfm.createFile(DEFAULT_TABLES_NAME);
        rbfm.openFile(DEFAULT_TABLES_NAME,tableFileHandle);
        tableIDmap.insert({0, tableFileHandle});


        rbfm.createFile(DEFAULT_ATTRIBUTE_NAME);
        rbfm.openFile(DEFAULT_ATTRIBUTE_NAME,attributeFileHandle);
        tableIDmap.insert({1, attributeFileHandle});

        createTable(DEFAULT_TABLES_NAME, tableRecordDescriptor);
        createTable(DEFAULT_ATTRIBUTE_NAME, attributeRecordDescriptor);
        return 0;
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
        //printf("ATTR NAME: {%s}\n", result.name.c_str());
        int type = *(int*)dataPointer;
        AttrType attrtype;
        dataPointer += 4;
        switch (type) {
            case 0:
                attrtype = TypeInt;
                result.length = 4;
                break;
            case 1:
                attrtype = TypeReal;
                result.length = 4;
                break;
            case 2:
                attrtype = TypeVarChar;
                result.length = 50;
                break;
        }
        result.type = attrtype;


        return result;
    }


    std::vector<Attribute> RelationManager::getRecordDescriptor(int table_id){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int value = table_id;
        RBFM_ScanIterator Iterator = RBFM_ScanIterator();
        std::vector<std::string> attributeNames;

        rbfm.scan(attributeFileHandle, attributeRecordDescriptor,
                  "ID", EQ_OP, &value, attributeNames,
                  Iterator);
        std::vector<Attribute> result;

        for(auto RID : Iterator.recordRIDS){
            void* temp[100];
            //printf("RD ROD:{%d}{%d}:", RID.pageNum,RID.slotNum);
            rbfm.readRecord(attributeFileHandle, attributeRecordDescriptor, RID, temp);
            Attribute tempAttr = convertBytesToAttributes(attributeRecordDescriptor, temp);
            //printf("ATTRIBUTE {%s} {%d} \n",tempAttr.name.c_str(),tempAttr.type);
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