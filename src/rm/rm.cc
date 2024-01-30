#include "src/include/rm.h"

namespace PeterDB {
    std::string DEFAULT_TABLES_NAME = "Tables";
    std::string DEFAULT_ATTRIBUTE_NAME = "Attributes";
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        bool CatalogActive = false;

        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        //Create File for tables & attributes
        this->CatalogActive = true;
        int createTables = RecordBasedFileManager::instance().createFile(DEFAULT_TABLES_NAME);
        if(createTables != 0){
            return -1;
        }

        int createAttributes = RecordBasedFileManager::instance().createFile(DEFAULT_ATTRIBUTE_NAME);
        if(createAttributes != 0){
            return -1;
        }

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        this->CatalogActive = false;
        int deleteTables = RecordBasedFileManager::instance().destroyFile(DEFAULT_TABLES_NAME);
        int deleteAttributes = RecordBasedFileManager::instance().destroyFile(DEFAULT_ATTRIBUTE_NAME);

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
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
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