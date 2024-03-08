#include "src/include/qe.h"
#include <iostream>
#include <cmath>
#include <algorithm>
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
               // printf("{%d} to {%d}\n", *(int*)left, *(int*)right);
                return rbfm.compareNums((int*)left,(int*)right,op, type);
            case TypeReal:
               // printf("{%f} to {%f}\n", *(float*)left, *(int*)right);
                return rbfm.compareNums((float *)left,(float *)right,op, type);
            case TypeVarChar:
                char* pointer = (char*)left;
                int length = *(int*)left;
                pointer += 4;
                char* varChar = new char[length + 1];
                memcpy(varChar, pointer, length);
                varChar[length] = '\0';
                bool result = rbfm.compareString((char*)left,(char*)right,op);
                free(varChar);
                return result;

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

        while(this->input->getNextTuple(data) != -1){
            void* lhsAttr[PAGE_SIZE];
            Iterator::getAttribute(attrs, condition.lhsAttr, data, lhsAttr, this->type);
            if(condition.bRhsIsAttr == false){
                bool result = Iterator::compare(lhsAttr, condition.rhsValue.data, this->type, condition.op);
                if(result){
                    return 0;
                }
            }
            else{
                void* rhsAttr[PAGE_SIZE];
                Iterator::getAttribute(attrs, condition.rhsAttr, data, rhsAttr, this->type);
                bool result = Iterator::compare(lhsAttr, rhsAttr, this->type, condition.op);
                if(result){
                    return 0;
                }
            }

        }
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        this->input->getAttributes(attrs);
        return 0;
    }


    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->input = input;
        this->attrNames = attrNames;

        this->input->getAttributes(attributes);
//        for (const auto& attrName : attrNames) {
//            printf("Attribute Names: %s\n", attrName.c_str());
//        }
        for(auto a : attrNames){
            for(auto attr : attributes){
                if(a == attr.name){
                    projectedAttrs.push_back(attr);
                    break;
                }

            }
        }

        for(const auto& a : projectedAttrs){
            printf("Projected {%s}\n", a.name.c_str());
        }


    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        void* temp[PAGE_SIZE];

        while(this->input->getNextTuple(temp) != -1){
            char* pointer = (char*)data;

            pointer+=1;
            memset(data, 0, 1);

            for(auto a : projectedAttrs){
                void* lhsAttr = malloc(a.length);

                Iterator::getAttribute(attributes, a.name, temp, lhsAttr, a.type);
                switch (a.type) {
                    case TypeInt:
                       // printf("value {%d}\n", *(int*)lhsAttr);
                        memmove(pointer, lhsAttr, 4);
                        pointer += 4;
                        break;
                    case TypeReal:
                       // printf("value {%f}\n", *(float*)lhsAttr);
                        memmove(pointer, lhsAttr, 4);
                        pointer += 4;
                        break;
                    case TypeVarChar:
                        char* tempPointer = (char*)lhsAttr;
                        int* length = (int*)tempPointer;
                        memmove(pointer, length, 4);
                        pointer += 4;
                        tempPointer += 4;
                        memmove(pointer, tempPointer, *length);
                        pointer += *length;
                        break;

                }
            }

            return 0;
        }

        return QE_EOF;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = projectedAttrs;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIterator = leftIn;
        this->rightIterator = rightIn;
        this->condition = condition;
        this->numPages = numPages;
        index = 0;
        leftAttrs.clear();
        rightAttrs.clear();

        this->leftIterator->getAttributes(leftAttrs);
        this->rightIterator->getAttributes(rightAttrs);
        for(auto attr : leftAttrs){
            combinedAttrs.push_back(attr);
        }
        for(auto attr : rightAttrs){
            combinedAttrs.push_back(attr);
        }

        printf("Compare left %s to right %s with %d\n", condition.lhsAttr.c_str(), condition.rhsAttr.c_str(), condition.op);
        for(auto a : leftAttrs){
            printf("%s ", a.name.c_str());
        }
        printf("\n");
        for(auto a : rightAttrs){
            printf("%s ", a.name.c_str());
        }
        printf("\n");
        this->rightMaxSize = 1;
        this->loadRightBlock();
    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        void* leftBlock[PAGE_SIZE];

        AttrType leftType;
        AttrType rightType;

        while(leftIterator->getNextTuple(leftBlock) != -1){
            RelationManager::instance().printTuple(leftAttrs, leftBlock, std::cout);
            void* leftAttr[PAGE_SIZE];
            Iterator::getAttribute(leftAttrs, condition.lhsAttr, leftBlock, leftAttr, leftType);
            for(auto rightA : rightBlock){
                void* rightAttr[this->rightMaxSize];

                Iterator::getAttribute(rightAttrs, condition.rhsAttr, rightA, rightAttr, rightType);
                if(compare(leftAttr, rightAttr, leftType, condition.op)){
                    printf("Joining left:\n");
                    RelationManager::instance().printTuple(leftAttrs, leftBlock, std::cout);
                    RelationManager::instance().printTuple(rightAttrs, rightA, std::cout);
                    int length = RecordBasedFileManager::instance().getRecordSize(leftAttrs,leftBlock);
                    char* dataPointer = (char*)data;
                    memmove(data, leftBlock, length);
                    char* rightPointer = (char*)rightA;
                    memmove(dataPointer + length, rightPointer+1, (int)(this->rightMaxSize));

                    return 0;
                }
            }

        }
        return QE_EOF;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = combinedAttrs;

        return 0;
    }

    void BNLJoin::loadRightBlock() {
        for(auto s : rightAttrs){
            this->rightMaxSize += s.length;
        }
        void* rightData[this->rightMaxSize];
        while (rightIterator->getNextTuple(rightData) != -1) {
            char* tupleData = new char[this->rightMaxSize];
            memcpy(tupleData, rightData, this->rightMaxSize);
            rightBlock.push_back(tupleData);
            //RelationManager::instance().printTuple(rightAttrs, rightData, std::cout);
        }
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
