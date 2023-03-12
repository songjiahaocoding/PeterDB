#include <cmath>
#include <iostream>
#include "src/include/qe.h"
#include <cstring>
#include <climits>
#include <algorithm>

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

        int pivot = std::ceil( static_cast<double>(attrs.size()) /CHAR_BIT);
        char* nullBytes = new char [pivot];
        memset(nullBytes, 0, pivot);
        memcpy(nullBytes, data, pivot);
        // Found such condition attribute in data or not

        if(condition.bRhsIsAttr == false){
            for(int i = 0 ; i < attrs.size(); i++){
                if(Tool::isNull(i, nullBytes))
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
            delete [] key;
            delete [] nullBytes;
            return satisfy;
        }

        return false;
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
                unsigned keyLen = *(unsigned* )key;
                unsigned condLen = *(unsigned *)this->condition.rhsValue.data;
                char* keycmp = new char [keyLen+1];
                char* condcmp = new char [condLen+1];
                memcpy(keycmp, key+sizeof(int), keyLen);
                memcpy(condcmp, (char*)this->condition.rhsValue.data+sizeof(int), condLen);
                keycmp[keyLen] = '\0';
                condcmp[condLen] = '\0';


                auto res = strcmp(keycmp, condcmp);
                delete [] keycmp;
                delete [] condcmp;
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
        iter = input;
        this->attrNames = attrNames;
        iter->getAttributes(attrs);
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        char* tuple = new char [PAGE_SIZE];
        memset(tuple, 0, PAGE_SIZE);
        auto rc = iter->getNextTuple(tuple);
        if(rc==-1){
            delete [] tuple;
            return -1;
        }
        ProjectAttr* proAttrs = new ProjectAttr [attrNames.size()];
        int pivot = std::ceil( static_cast<double>(attrs.size()) /CHAR_BIT);
        char* nullBytes = new char [pivot];
        memset(nullBytes, 0, pivot);
        memcpy(nullBytes, data, pivot);

        int namePivot = std::ceil( static_cast<double>(attrNames.size()) /CHAR_BIT);
        char* nameNullBytes = new char [namePivot];
        memset(nameNullBytes, 0, namePivot);
        memcpy(nameNullBytes, data, namePivot);
        for (int i = 0; i < attrs.size(); ++i) {
            auto it = std::find(attrNames.begin(), attrNames.end(), attrs[i].name);
            int dis = std::distance(attrNames.begin(), it);
            if(Tool::isNull(i, nullBytes)){
                if(it!=attrNames.end()){
                    Tool::setNull(dis, nameNullBytes);
                }
                continue;
            }
            if(it!=attrNames.end()){
                proAttrs[dis] = {attrs[i], (unsigned)pivot};
            }

            int len = sizeof(int);
            if(attrs[i].type == TypeVarChar){
                memcpy(&len, tuple + pivot, sizeof(int));
                len += sizeof(int);
            }
            pivot += len;
        }

        for(int i=0;i<attrNames.size();i++){
            auto item = proAttrs[i];
            int len = sizeof(int);
            if(item.attr.type==TypeVarChar){
                memcpy(&len, tuple+item.pos, sizeof(int));
                len+=sizeof(int);
            }
            memcpy((char*)data+namePivot, tuple+item.pos, len);
            namePivot+=len;
        }

        memcpy(data, nameNullBytes, std::ceil( static_cast<double>(attrNames.size()) /CHAR_BIT));
        delete [] proAttrs;
        delete [] tuple;
        delete [] nullBytes;
        delete [] nameNullBytes;
        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        std::vector<Attribute> temp;
        iter->getAttributes(temp);
        for (std::string s : this->attrNames){
            for(Attribute attr : temp){
                if (attr.name == s){
                    attrs.push_back(attr);
                    break;
                }
            }
        }
        return 0;
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
