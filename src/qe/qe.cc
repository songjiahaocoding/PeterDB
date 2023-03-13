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
        memcpy(nullBytes, tuple, pivot);

        int namePivot = std::ceil( static_cast<double>(attrNames.size()) /CHAR_BIT);
        char* nameNullBytes = new char [namePivot];
        memset(nameNullBytes, 0, namePivot);
        memcpy(nameNullBytes, tuple, namePivot);
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
        this->leftIter = leftIn;
        this->rightIter = rightIn;
        this->condition = condition;
        this->pageNum = numPages;
        this->mapSize = 0;

        this->leftIter->getAttributes(this->leftAttrs);
        this->rightIter->getAttributes(this->rightAttrs);

        this->leftData = new char [PAGE_SIZE];
        this->rightData = new char [PAGE_SIZE];
    }

    BNLJoin::~BNLJoin() {
        delete [] this->leftData;
        delete [] this->rightData;
    }

    RC BNLJoin::getNextTuple(void *data) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        while(true) {
            if(tupleBuffer.size() != 0) {
                auto pair = tupleBuffer.back();
                char* tuple1 = new char [pair.first.getSize()];
                char* tuple2 = new char [pair.second.getSize()];
                pair.first.getData(leftAttrs, tuple1);
                pair.second.getData(rightAttrs, tuple2);
                mergeTwoTuple(leftAttrs, tuple1, pair.first.getSize(), rightAttrs, tuple2, pair.second.getSize(), data);
                tupleBuffer.pop_back();
                return 0;
            }

            while(mapSize < pageNum * PAGE_SIZE) {
                if(leftIter->getNextTuple(leftData) != QE_EOF) {
                    RID rid{0, 0};
                    Record record(leftAttrs, leftData, rid);

                    char* attrData = new char [PAGE_SIZE];
                    memset(attrData, 0, PAGE_SIZE);
                    record.getAttribute(condition.lhsAttr, leftAttrs, attrData);
                    char nullIndicator = attrData[0];
                    if(nullIndicator!=-128) {
                        Attribute targetAttr;
                        for(auto attr:leftAttrs){
                            if(attr.name==condition.lhsAttr){
                                targetAttr=attr;
                                break;
                            }
                        }
                        Key key(attrData+1, targetAttr.type);
                        map[key].push_back(record);
                        mapSize += record.getSize();
                    }
                    delete[] attrData;
                }
                else if(mapSize == 0) {
                    return QE_EOF;
                }
                else {
                    break;
                }
            }

            while(rightIter->getNextTuple(rightData) != QE_EOF) {
                RID rid{0,0};
                Record rightRecord(rightAttrs, rightData, rid);

                char* attrData = new char [PAGE_SIZE];
                memset(attrData, 0, PAGE_SIZE);
                rightRecord.getAttribute(condition.rhsAttr, rightAttrs, attrData);
                char nullIndicator = attrData[0];
                if(nullIndicator!=-128) {
                    Attribute targetAttr;
                    for(auto attr:leftAttrs){
                        if(attr.name==condition.lhsAttr){
                            targetAttr=attr;
                            break;
                        }
                    }
                    Key key(attrData+1, targetAttr.type);
                    if(map.find(key) != map.end()) {
                        for(auto r : map[key]) {
                            tupleBuffer.push_back({r, rightRecord});
                        }
                    }
                }
            }
        }

    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();

        for(auto &attr : this->rightAttrs) {
            attrs.push_back(attr);
        }

        for(auto &attr : this->leftAttrs) {
            attrs.push_back(attr);
        }
    }

    void BNLJoin::buildNullBytes(std::vector<Attribute> attrs, char* tuple, int offset, char* nullIndicator){
        for(int i = 0; i < attrs.size(); i++) {
            uint8_t nullIndicatorBuffer = 0;
            int byteOffset = (offset + i) / CHAR_BIT;
            int bitOffset = (offset + i) % CHAR_BIT;
            memcpy(&nullIndicatorBuffer, nullIndicator + byteOffset, sizeof(uint8_t));

            if(Tool::isNull(i, tuple)) {
                nullIndicatorBuffer = nullIndicatorBuffer >> (CHAR_BIT - bitOffset - 1);
                nullIndicatorBuffer = nullIndicatorBuffer | 1;
                nullIndicatorBuffer = nullIndicatorBuffer << (CHAR_BIT - bitOffset - 1);
                memcpy(nullIndicator + byteOffset, &nullIndicatorBuffer, sizeof(uint8_t));
            }
            else {
                nullIndicatorBuffer = nullIndicatorBuffer >> (CHAR_BIT - bitOffset);
                nullIndicatorBuffer = nullIndicatorBuffer << (CHAR_BIT - bitOffset);
                memcpy(nullIndicator + byteOffset, &nullIndicatorBuffer, sizeof(uint8_t));
            }
        }
    }

    void BNLJoin::mergeTwoTuple(std::vector<Attribute> attr1, char *tuple1, int size1, std::vector<Attribute> attr2,
                                char *tuple2, int size2, void *data) {
        int leftNullIndicatorSize = ceil(attr1.size() / 8.0);
        int rightNullIndicatorSize = ceil(attr2.size() / 8.0);
        int totalNullIndicatorSize = ceil((attr1.size() + attr2.size()) / 8.0);

        char* nullIndicator = new char [totalNullIndicatorSize];
        memset(nullIndicator, 0, totalNullIndicatorSize);

        buildNullBytes(attr1, tuple1, 0, nullIndicator);
        buildNullBytes(attr2, tuple2, attr1.size(), nullIndicator);

        unsigned mergedTupleSize = totalNullIndicatorSize + size1 + size2;

        unsigned offset = 0;
        memset(data, 0, mergedTupleSize);

        memcpy((char*)data + offset, nullIndicator, totalNullIndicatorSize);
        offset += totalNullIndicatorSize;

        memcpy((char*)data + offset, tuple1 + leftNullIndicatorSize, size1);
        offset += size1;

        memcpy((char*)data + offset, tuple2 + rightNullIndicatorSize, size2);
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
