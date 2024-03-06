#include "src/include/qe.h"
#include <iostream>
#include <cmath>
#include <string.h>
namespace PeterDB {
    RC Iterator::getAttribute(std::vector<Attribute> &attrs,const std::string& attr, void* data, void* attrData, AttrType& attrType) {
        char* dataPointer = (char*)data;
        int numOfNullBytes = ceil((double)attrs.size()/8);
        char nullIndicators[numOfNullBytes];
        for (int i = 0; i < numOfNullBytes; i++) {
            nullIndicators[i] = *dataPointer;
            dataPointer++;
        }

        for(const auto& a : attrs){
            if(a.name == attr){
                switch (a.type) {
                    case TypeInt:
                        attrType = TypeInt;
                        memcpy(attrData, dataPointer, 4);
                        break;
                    case TypeReal:
                        memcpy(attrData, dataPointer, 4);
                        attrType = TypeReal;
                        break;
                    case TypeVarChar:
                        int* length = (int*)dataPointer;
                        memcpy(attrData, length, 4);
                        char* attrPointer = (char*)attrData;
                        attrPointer += 4;
                        dataPointer += 4;
                        memcpy(attrPointer, dataPointer, *length);
                        attrType = TypeVarChar;
                        break;
                }
                return 0;
            }
            switch (a.type) {
                case TypeInt:
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
        }
        return -1;
    }

    bool Iterator::compare(void *left, void *right, AttrType type, CompOp op) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        switch (type) {
            case TypeInt:
                printf("{%d} to {%d}\n", *(int*)left, *(int*)right);
                return rbfm.compareNums((int*)left,(int*)right,op, type);
            case TypeReal:
                printf("{%f}\n", *(float*)left);
                return rbfm.compareNums((float *)left,(float *)right,op, type);
            case TypeVarChar:
                char* pointer = (char*)left;
                int length = *(int*)left;
                pointer += 4;
                char* varChar = new char[length + 1];
                memcpy(varChar, pointer, length);
                varChar[length] = '\0';
                return rbfm.compareString((char*)left,(char*)right,op);
        }
        return false;
    }

    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        std::vector<Attribute> attrs;
        getAttributes(attrs);

        while(auto i = this->input->getNextTuple(data) != -1){
            RelationManager::instance().printTuple(attrs, data, std::cout);
            void* lhsAttr[PAGE_SIZE];
            Iterator::getAttribute(attrs, condition.lhsAttr, data, lhsAttr, this->type);
            if(condition.bRhsIsAttr == false){
                bool result = Iterator::compare(lhsAttr, condition.rhsValue.data, this->type, condition.op);
                if(result){
                    return 0;
                }
            }
            else{

            }

        }
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        this->input->getAttributes(attrs);
        return 0;
    }




    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {

    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }


} // namespace PeterDB
