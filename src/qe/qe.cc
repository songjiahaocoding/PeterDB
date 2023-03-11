#include <cmath>
#include <iostream>
#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->iter = input;
        this->condition = condition;
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        while(iter->getNextTuple(data)!=-1){
            if(isMatch((char*)data))return 0;
        }
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        return iter->getAttributes(attrs);
    }

    bool Filter::isMatch(char *data) {
        std::vector<Attribute> attrs;
        iter->getAttributes(attrs);

        int pivot = ceil((double)attrs.size() / 8);
        char* nullBytes = new char [pivot];
        memset(nullBytes, 0, pivot);
        memcpy(nullBytes, data, pivot);
        // Found such condition attribute in data or not

        if(condition.bRhsIsAttr == false){
            for(int i = 0 ; i < attrs.size(); i++){
                if(isNull(i, nullBytes))
                    continue;
                if(attrs[i].name == condition.lhsAttr) {
                    break;
                }
                if(attrs[i].type == TypeVarChar){
                    int strLength;
                    memcpy(&strLength, data + pivot, sizeof(int));
                    pivot += (sizeof(int) + strLength);
                }
                else{
                    pivot += sizeof(int);
                }
            }
            // save the key
            char* key = new char [PAGE_SIZE];
            memset(key, 0, PAGE_SIZE);
            if(condition.rhsValue.type == TypeVarChar){
                int strLength;
                memcpy(&strLength, data + pivot, sizeof(int));
                memcpy(key, data + pivot, strLength + sizeof(int));
            }
            else{
                memcpy(key, data + pivot, sizeof(int));
            }
            bool satisfy = isCompareSatisfy(key);
            int k;
            memcpy(&k, key, sizeof(int));
            free(key);
            free(nullBytes);
            return satisfy;
        }

        return false;
    }

    bool Filter::isNull(int i, char *data) {
        int bytePosition = i / 8;
        int bitPosition = i % 8;
        char b = data[bytePosition];
        return ((b >> (7 - bitPosition)) & 0x1);
    }

    bool Filter::isCompareSatisfy(char *key) {
        if (condition.op == NO_OP) return true;

        switch (condition.rhsValue.type) {
            case TypeInt:
            {
                int val = *(int*)key;
                int cond = *(int*)this->condition.rhsValue.data;
                switch (this->condition.op) {
                    case EQ_OP: return val == cond;
                    case LT_OP: return val < cond;
                    case LE_OP: return val <= cond;
                    case GT_OP: return val > cond;
                    case GE_OP: return val >= cond;
                    case NE_OP: return val != cond;
                    default: return false;
                }
            }
            case TypeReal:
            {
                float val = *(float *)key;
                float cond = *(float *)this->condition.rhsValue.data;
                switch (this->condition.op) {
                    case EQ_OP: return fabs(val-cond)<FLOAT_DIFF;
                    case LT_OP: return val < cond;
                    case LE_OP: return val <= cond;
                    case GT_OP: return val > cond;
                    case GE_OP: return val >= cond;
                    case NE_OP: return (val-cond)>FLOAT_DIFF;
                    default: return false;
                }
            }
            case TypeVarChar:
            {
                auto res = strcmp((char*)this->condition.rhsValue.data+4, key+4);
                switch (this->condition.op) {
                    case EQ_OP: return res==0;
                    case LT_OP: return res<0;
                    case LE_OP: return res<=0;
                    case GT_OP: return res>0;
                    case GE_OP: return res>=0;
                    case NE_OP: return res!=0;
                }
            }
            default:
                std::cout<<"Error with the attribute type"<< std::endl;
                return false;
        }
        return false;
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
