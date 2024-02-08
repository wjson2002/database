#include "src/include/rm.h"

#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

namespace PeterDB {
    std::string DEFAULT_TABLES_NAME = "Tables";
    std::vector<Attribute> tableRecordDescriptor = {{"table-id", TypeInt, sizeof(int)},
                                                    {"table-name", TypeVarChar, 50},
                                                    {"file-name",TypeVarChar, 80}};
    std::string DEFAULT_ATTRIBUTE_NAME = "Columns";
    std::vector<Attribute> attributeRecordDescriptor = {{"table-id", TypeInt, sizeof(int)},
                                                    {"column-name", TypeVarChar, 50},
                                                    {"column-type",TypeInt, sizeof(int)},
                                                    {"column-length",TypeInt, sizeof(int)},
                                                        {"column-position",TypeInt, sizeof(int)}};

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
            //printf("Catalog already created\n");
            return -1;
        }
        this->CatalogActive = true;
        initCatalogTables();

        return 0;
    }

    RC RelationManager::deleteCatalog() {

        printf("Deleting Catalog\n");

        if(CatalogActive == false){
            //printf("Catalog not active\n");
            return -1;
        }
        for(const auto& i : tableNameToIdMap){
            RecordBasedFileManager::instance().destroyFile(i.first);
        }
        this->CatalogActive = false;
        tableNameToIdMap.clear();
        tableIDmap.clear();

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        printf("Creating new Table: {%s}\n", tableName.c_str());
        if(this->CatalogActive == false){
            return -1;
        }
        if(TableExists(tableName)){
            return -1;
        }
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int index;
        if(tableName == DEFAULT_TABLES_NAME) {
            index = 0;
        }
        else if(tableName == DEFAULT_ATTRIBUTE_NAME){
            index = 1;
        }
        else{
            index = tableIDmap.size();
            if(index < 2){
                index += 2;
            }

            FileHandle newTableFileHandle;
            rbfm.createFile(tableName);
            rbfm.openFile(tableName,newTableFileHandle);

            tableIDmap.insert({index, newTableFileHandle});
            rbfm.closeFile(newTableFileHandle);
        }
        tableNameToIdMap.insert({tableName, index});
        RID rid;
        // Insert records into Table
        std::string tableIndex = std::to_string(index);
        std::string data[] = {tableIndex, tableName, tableName+".bin"};
        auto result = convert(tableRecordDescriptor, data);
        rbfm.insertRecord(tableFileHandle, tableRecordDescriptor, result, rid);

        // Insert Record into Attributes
        rbfm.openFile(DEFAULT_ATTRIBUTE_NAME, attributeFileHandle);
        int position = 0;
        for(auto attr: attrs){
            std::string attrData[] = {  tableIndex,
                                        attr.name,
                                        std::to_string(attr.type),
                                        std::to_string(position),
                                        std::to_string(attr.length)};

            auto bytes = convert(attributeRecordDescriptor, attrData);
            rbfm.insertRecord(attributeFileHandle, attributeRecordDescriptor, bytes, rid);
            position += 1;
        }
        rbfm.closeFile(attributeFileHandle);
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        printf("Delete Table:{%s}\n", tableName.c_str());
        if(TableExists(tableName)){
            RecordBasedFileManager::instance().destroyFile(tableName);
            int tableInt = tableNameToIdMap[tableName];
            tableNameToIdMap.erase(tableName);
            tableIDmap.erase(tableInt);
            return 0;
        }
        else{
            printf("Tablename not found in map");
            return -1;
        }

    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        printf("Getting table {%s}\n", tableName.c_str());

        if(tableName == DEFAULT_TABLES_NAME){
            attrs = tableRecordDescriptor;
            return 0;
        }

        if(tableName == DEFAULT_ATTRIBUTE_NAME){
            attrs = attributeRecordDescriptor;
            return 0;
        }

        if(TableExists(tableName)){
            int tableID = tableNameToIdMap[tableName];
            //Need to get table ID
            std::vector<Attribute> recordDescriptor = getRecordDescriptor(tableID);
            attrs = recordDescriptor;
            return 0;
        }
        else
        {
            return -1;
        }
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        auto it = tableNameToIdMap.find(tableName);

        if(it == tableNameToIdMap.end()){
            return -1;
        }

        std::vector<std::string> attributeNames;

        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);

        rbfm.openFile(tableName, fh);
        rbfm.insertRecord(fh, recordD, data, rid);
        rbfm.closeFile(fh);
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(TableExists(tableName))
        {
            RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
            int tableID = tableNameToIdMap[tableName];
            FileHandle fh = tableIDmap[tableID];
            std::vector<Attribute> recordD = getRecordDescriptor(tableID);
            rbfm.openFile(tableName, fh);
            rbfm.deleteRecord(fh, recordD, rid);
            rbfm.closeFile( fh);
            return 0;
        }
        else{
            printf("Delete tuple Error");
            return -1;
        }

    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);
        rbfm.openFile(tableName, fh);
        rbfm.updateRecord(fh, recordD, data, rid);
        rbfm.closeFile(fh);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {

        if(TableExists(tableName)){
            RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
            int tableID = tableNameToIdMap[tableName];
            FileHandle fh = tableIDmap[tableID];
            std::vector<Attribute> recordD = getRecordDescriptor(tableID);
            printf("Reading {%d},{%d]", rid.pageNum, rid.slotNum);
            rbfm.openFile(tableName, fh);
            int result = rbfm.readRecord(fh, recordD, rid, data);
            rbfm.closeFile(fh);
            //printf("Read Tuple: ");
            //rbfm.printRecord(recordD, data, std::cout);
            if(result == 0){
                return 0;
            }
            else{
                return -1;
            }
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

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int tableID = tableNameToIdMap[tableName];
        FileHandle fh = tableIDmap[tableID];
        std::vector<Attribute> recordD = getRecordDescriptor(tableID);
        rbfm.openFile(tableName,fh);
        rbfm.readAttribute(fh, recordD, rid, attributeName, data);
        rbfm.closeFile(fh);

        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        if(TableExists(tableName)){
            RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
            int tableID = tableNameToIdMap[tableName];
            FileHandle fh = tableIDmap[tableID];
            std::vector<Attribute> recordD = getRecordDescriptor(tableID);
            RBFM_ScanIterator Iterator = RBFM_ScanIterator();
            for(auto attr : recordD){
                rm_ScanIterator.maxSizeOfTuple += attr.length;
            }

            rbfm.scan(fh, recordD, conditionAttribute, compOp, value, attributeNames, Iterator);
            rm_ScanIterator.recordDescriptor = recordD;
            rm_ScanIterator.rbfmIterator = Iterator;

            return 0;
        }
        else{
            return -1;
        }

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

        char* result = new char[totalSize + numOfNullBytes];
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

        //printf("Scan for rd table: {%d}\n", table_id);

        rbfm.scan(attributeFileHandle, attributeRecordDescriptor,
                  "table-id", EQ_OP, &value, attributeNames,
                  Iterator);

        std::vector<Attribute> result;
        RID tempRID;
        void* buffer[150];
        int count  = 0;
        int i = 0;
        while((i = Iterator.getNextRecord(tempRID, buffer)) != RBFM_EOF){
            rbfm.openFile(DEFAULT_ATTRIBUTE_NAME, attributeFileHandle);
            rbfm.readRecord(attributeFileHandle, attributeRecordDescriptor, tempRID, buffer);
            Attribute tempAttr = convertBytesToAttributes(attributeRecordDescriptor, buffer);
            result.push_back(tempAttr);
            count++;
            rbfm.closeFile(attributeFileHandle);
        }

        //printf("Get next record ran {%d} times\n", count);
        return result;
    }

    bool RelationManager::TableExists(std::string tableName){
        auto it = tableNameToIdMap.find(tableName);

        if(it == tableNameToIdMap.end()){
            return false;
        }
        else{
            return true;
        }
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

        int result = rbfmIterator.getNextRecord(rid, data);

        if(result == 0){
            std::vector<std::string> attributeNames = rbfmIterator.attributeNames;
            if(attributeNames.size() == 0){
                return 0;
            }
            else{
                RelationManager& rm = RelationManager::instance();
                char* pointer = (char*)data;
                for(auto attr: attributeNames){
                    rm.readAttribute(rbfmIterator.fileName, rid, attr, pointer);
                    for(auto a : recordDescriptor){
                        if(a.name == attr){
                            pointer += a.length;
                        }
                    }
                }
            }

        }
        else{
            return RM_EOF;
        }
    }

    RC RM_ScanIterator::close() {
        rbfmIterator.close();
        return 0;
    }


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