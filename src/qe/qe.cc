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
        if(iter->getNextTuple(tuple)==-1){
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
                memset(tuple1, 0, pair.first.getSize());
                memset(tuple2, 0, pair.second.getSize());
                pair.first.getData(leftAttrs, tuple1);
                pair.second.getData(rightAttrs, tuple2);
                Tool::mergeTwoTuple(leftAttrs, tuple1, pair.first.getSize(), rightAttrs, tuple2, pair.second.getSize(), data);
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
                        Key* key = new Key(attrData+1, targetAttr.type);
                        map[*key].push_back(record);
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
                        for(int i=0;i<map[key].size();i++){
                            auto pair = new std::pair<Record, Record>(map[key][i], rightRecord);
                            tupleBuffer.push_back(*pair);
                        }
                    }
                }
                delete [] attrData;
            }

            this->map.clear();
            this->mapSize = 0;
            this->rightIter->setIterator();
        }
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for(auto &attr : this->leftAttrs) {
            attrs.push_back(attr);
        }

        for(auto &attr : this->rightAttrs) {
            attrs.push_back(attr);
        }
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->leftIter = leftIn;
        this->rightIter = rightIn;
        this->condition = condition;
        this->firstScan = true;

        this->leftIter->getAttributes(this->leftAttrs);
        this->rightIter->getAttributes(this->rightAttrs);

        this->leftData = new char [PAGE_SIZE];
        this->rightData = new char [PAGE_SIZE];
    }

    INLJoin::~INLJoin() {
        delete [] this->leftData;
        delete [] this->rightData;
    }

    RC INLJoin::getNextTuple(void *data) {
        memset(rightData, 0, PAGE_SIZE);
        if(!this->firstScan && rightIter->getNextTuple(rightData) != QE_EOF) {
            RID rid;
            Record left(leftAttrs, leftData, rid);
            Record right(rightAttrs, rightData, rid);
            Tool::mergeTwoTuple(leftAttrs, leftData, left.getSize(), rightAttrs, rightData, right.getSize(), data);
            return 0;
        }

        memset(this->leftData, 0, PAGE_SIZE);
        do{
            if(leftIter->getNextTuple(leftData) == QE_EOF) {
                return QE_EOF;
            }

            RID rid;
            char* leftAttrData = new char [PAGE_SIZE];
            memset(leftAttrData, 0, PAGE_SIZE);
            Record left(leftAttrs, leftData, rid);
            left.getAttribute(condition.lhsAttr, leftAttrs, leftAttrData);

            rightIter->setIterator(leftAttrData + 1, leftAttrData + 1, true, true);
            delete[] leftAttrData;
        }while(rightIter->getNextTuple(rightData) == QE_EOF);

        RID rid;
        Record left(leftAttrs, leftData, rid);
        Record right(rightAttrs, rightData, rid);
        Tool::mergeTwoTuple(leftAttrs, leftData, left.getSize(), rightAttrs, rightData, right.getSize(), data);
        this->firstScan = false;

        return 0;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for(auto attr:leftAttrs) {
            attrs.push_back(attr);
        }

        for(auto attr:rightAttrs) {
            attrs.push_back(attr);
        }
        return 0;
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
        this->iter = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->done = false;
        this->count = 0;
        this->num = (op==MIN? std::numeric_limits<float>::max() : 0);
        this->groupAttr.length = -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->iter = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->done = false;
        this->count = 0;
        this->num = (op==MIN? std::numeric_limits<float>::max() : 0);
        this->groupAttr = groupAttr;
        this->getAttributes(attributes);
        this->initGroupBy();
    }

    Aggregate::~Aggregate() {}

    RC Aggregate::getNextTuple(void *data) {
        if(this->groupAttr.length==-1){
            return this->getNext(data);
        } else {
            return this->getNextWithGroup(data);
        }
    }

    RC Aggregate::getNextWithGroup(void* data) {
        static auto iter = groupMap.begin();
        char* val = new char [PAGE_SIZE];
        while(iter!=groupMap.end()){
            memset(val, 0, PAGE_SIZE);
            int len = sizeof(int);
            if(groupAttr.type==TypeVarChar){
                memcpy(&len, iter->first.data, sizeof(int));
                len+=sizeof(int);
            }
            memcpy(val+1, iter->first.data, len);

            switch (op) {
                case SUM:
                case MIN:
                case MAX:
                {

                    char* aggVal = new char [sizeof(int)+1];
                    memset(aggVal, 0, sizeof(int)+1);
                    if(aggAttr.type==TypeInt){
                        int intNum = iter->second.second;
                        memcpy(aggVal+1, &intNum, sizeof(int));
                    }
                    else memcpy(aggVal+1, &iter->second.second, sizeof(int));
                    Tool::mergeTwoTuple({groupAttr}, val, len+1, {aggAttr}, aggVal, sizeof(int)+1, data);
                    delete [] val;
                    delete [] aggVal;
                    iter++;
                    return 0;
                }
                case COUNT:
                {
                    char* aggVal = new char [sizeof(int)+1];
                    memset(aggVal, 0, sizeof(int)+1);
                    memcpy(aggVal+1, &iter->second.first, sizeof(int));
                    Tool::mergeTwoTuple({groupAttr}, val, len+1, {aggAttr}, aggVal, sizeof(int)+1, data);
                    delete [] val;
                    delete [] aggVal;
                    iter++;
                    return 0;
                }
                case AVG:
                {
                    if(iter->second.first==0){
                        char* aggVal = new char [sizeof(int)+1];
                        memset(aggVal, 0, sizeof(int)+1);
                        memcpy(aggVal+1, &iter->second.second, sizeof(int));
                        Tool::mergeTwoTuple({groupAttr}, val, len+1, {aggAttr}, aggVal, sizeof(int)+1, data);
                        delete [] aggVal;
                        delete [] val;
                        iter++;
                        return 0;
                    }
                    float avg = iter->second.second / iter->second.first;
                    char* temp = new char [sizeof(float) + 1];
                    memset(temp, 0, sizeof(float) + 1);
                    memcpy(temp+1, &avg, sizeof(float));
                    Tool:: mergeTwoTuple({groupAttr}, val, len+1, {aggAttr}, (char*)temp, sizeof(float)+1, data);
                    delete[] temp;
                    delete[] val;
                    iter++;
                    return 0;
                }
            }
        }
        delete [] val;
        return -1;
    }

    RC Aggregate::getNext(void* data){
        if (this->done) return QE_EOF;

        char* tuple = new char [PAGE_SIZE];
        char* key = new char [PAGE_SIZE];
        float floatNum = 0;
        int intNum = 0;
        std::vector<Attribute> attrs;
        iter->getAttributes(attrs);
        memset(tuple, 0, PAGE_SIZE);

        while(iter->getNextTuple(tuple) != QE_EOF){
            this->count++;
            RID rid;
            Record record(attrs, tuple, rid);
            memset(key, 0, PAGE_SIZE);
            record.getAttribute(this->aggAttr.name, attrs, key);
            char nullIndicator = key[0];
            if(nullIndicator==-128)continue;
            memcpy(&intNum, key+1, sizeof(int));
            memcpy(&floatNum, key+1, sizeof(float));

            switch (this->op) {
                case MIN:
                    if(this->aggAttr.type==TypeInt)this->num = std::min(this->num, (float)intNum);
                    else this->num = std::min(this->num, floatNum);
                    break;
                case MAX:
                    if(this->aggAttr.type==TypeInt)this->num = std::max(this->num, (float)intNum);
                    else this->num = std::max(this->num, floatNum);
                    break;
                case AVG:
                case SUM:
                    if(this->aggAttr.type==TypeInt)this->num += intNum;
                    else this->num += floatNum;
                    break;
            }
        }

        char nullIndicator = 0;
        if(this->count==0){
            nullIndicator = -128;
        }
        memcpy(data, &nullIndicator, sizeof(char));

        switch(op){
            // Use this.num to store all intermediate result
            case MAX:
            case MIN:
            case SUM:
                if(this->aggAttr.type==TypeInt) {
                    int res = this->num;
                    memcpy((char*)data + sizeof(char), &res, sizeof(int));
                }
                else memcpy((char*)data + sizeof(char), &this->num, sizeof(float));
                break;
            case COUNT:
            {
                float res = this->count;
                memcpy((char*)data + sizeof(char), &res, sizeof(float));
                break;
            }
            case AVG:
            {
                float avg = (this->count == 0 ? 0 : this->num / this->count);
                memcpy((char*)data + sizeof(char), &avg, sizeof(float));
                break;
            }
        }

        this->done = true;
        delete [] tuple;
        delete [] key;
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        if(groupAttr.length!=-1){
            attrs.push_back(groupAttr);
        }
        std::string name;
        switch(this->op){
            case MIN:
                name = "MIN";
                break;
            case MAX:
                name = "MAX";
                break;
            case COUNT:
                name = "COUNT";
                break;
            case SUM:
                name = "SUM";
                break;
            case AVG:
                name = "AVG";
                break;
        }
        std::string attrName = name + "(" + this->aggAttr.name + ")";
        Attribute attr;
        attr.name = attrName;
        attr.type = this->aggAttr.type;
        if(this->op==AVG){
            attr.type = TypeReal;
        }
        attr.length = this->aggAttr.length;
        attrs.push_back(attr);
        return 0;
    }

    void Aggregate::initGroupBy() {
        char* attrData = new char [PAGE_SIZE];
        char* tuple = new char [PAGE_SIZE];
        char* val = new char [PAGE_SIZE];
        memset(tuple, 0, PAGE_SIZE);
        while(iter->getNextTuple(tuple) != QE_EOF) {
            memset(attrData, 0, PAGE_SIZE);
            RID rid;
            Record nextRecord(attributes, tuple, rid);
            nextRecord.getAttribute(groupAttr.name, attributes, attrData);
            char nullIndicator = attrData[0];
            if (nullIndicator == -128)continue;
            Key* key = new Key(attrData + 1, groupAttr.type);

            if (groupMap.find(*key) == groupMap.end()) {
                float num = (op==MIN? std::numeric_limits<float>::max() : 0);
                groupMap.insert({*key, {0, num}});
            }
            int id = 0;
            for(;id<attributes.size();id++){
                if(attributes[id].name==groupAttr.name)break;
            }
            int intNum = 0;
            float floatNum = 0;
            memset(val, 0, PAGE_SIZE);
            std::string attrName = getAggName();
            nextRecord.getAttribute(attrName, attributes, val);
            memcpy(&intNum, val+1, sizeof(int));
            memcpy(&floatNum, val+1, sizeof(int));
            nullIndicator = val[0];
            switch(op) {
                case MIN:
                    if(nullIndicator!=-128){
                        if(aggAttr.type==TypeInt)groupMap[*key].second = std::min(groupMap[*key].second, (float)intNum);
                        else groupMap[*key].second = std::min(groupMap[*key].second, floatNum);
                    }
                    break;
                case MAX:
                    if(nullIndicator!=-128){
                        if(aggAttr.type==TypeInt)groupMap[*key].second = std::max(groupMap[*key].second, (float)intNum);
                        else groupMap[*key].second = std::max(groupMap[*key].second, floatNum);
                    }
                    break;
                case COUNT:
                    if(nullIndicator!=-128){
                        groupMap[*key].first++;
                    }
                    break;
                case SUM:
                case AVG:
                    if(nullIndicator!=-128){
                        if(aggAttr.type==TypeInt)groupMap[*key].second+=intNum;
                        else groupMap[*key].second+=floatNum;
                        groupMap[*key].first++;
                    }
                    break;
            }
        }
        delete [] attrData;
        delete [] tuple;
        delete [] val;
    }

    std::string Aggregate::getAggName() {
        std::string name;
        switch(this->op){
            case MIN:
                name = "MIN";
                break;
            case MAX:
                name = "MAX";
                break;
            case COUNT:
                name = "COUNT";
                break;
            case SUM:
                name = "SUM";
                break;
            case AVG:
                name = "AVG";
                break;
        }
        return name+"("+aggAttr.name+")";
    }
} // namespace PeterDB
