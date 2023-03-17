#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <cstring>
#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    struct ProjectAttr{
        Attribute attr;
        unsigned pos;
    };

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class Key {
    public:
        char* data;
        AttrType type;
        unsigned len;
        Key() = default;
        Key(char* ptr, AttrType type){
            this->type = type;
            this->len = sizeof(int);
            if(type==TypeVarChar){
                memcpy(&this->len, ptr, sizeof(int));
                this->len+=sizeof(int);
            }
            data = new char [len];
            memcpy(data, ptr, len);
        }

        ~Key(){
            delete [] data;
        }


        bool operator < (const Key &k) const {
            switch (type) {
                case TypeInt:
                {
                    int int1 = *(int*)data;
                    int int2 = *(int*)k.data;
                    return int1<int2;
                }
                case TypeReal:
                {
                    float ft1 = *(float*)data;
                    float ft2 = *(float*)k.data;
                    return ft1<ft2;
                }
                case TypeVarChar:
                {
                    std::string s1(data+4);
                    std::string s2(k.data+4);
                    return s1<s2;
                }
            }
        }

        bool operator == (const Key &k) const {
            switch (type) {
                case TypeInt:
                {
                    int int1 = *(int*)data;
                    int int2 = *(int*)k.data;
                    return int1==int2;
                }
                case TypeReal:
                {
                    float ft1 = *(float*)data;
                    float ft2 = *(float*)k.data;
                    return ft1==ft2;
                }
                case TypeVarChar:
                {
                    std::string s1(data+4);
                    std::string s2(k.data+4);
                    return s1==s2;
                }
            }
        }

        void operator = (const Key &k){
            this->len = k.len;
            int tmp = sizeof(int);
            if(k.type==TypeVarChar){
                tmp+=k.len;
            }
            this->data = new char [tmp];
            memcpy(this->data, k.data, tmp);
            this->type = k.type;
        }
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator* iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *this->iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *this->iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter->getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~IndexScan() override {
            iter->close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        Iterator* iter;
        Condition condition;

        bool isMatch(char *data);

        bool isCompareSatisfy(char *key);
    };

    class Project : public Iterator {
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        Iterator* iter;
        std::vector<std::string> attrNames;
        std::vector<Attribute> attrs;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        Iterator* leftIter;
        TableScan* rightIter;
        Condition condition;
        unsigned pageNum;
        unsigned mapSize;
        char* leftData;
        char* rightData;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;

        std::map<Key, std::vector<Record>> map;
        std::vector<std::pair<Record, Record>> tupleBuffer;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        Iterator* leftIter;
        IndexScan* rightIter;
        Condition condition;
        char* leftData;
        char* rightData;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;
        bool firstScan;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        Iterator* iter;
        Attribute aggAttr;
        Attribute groupAttr;
        std::vector<Attribute> attributes;
        std::map<Key, std::pair<int, float>> groupMap;
        int count = 0;
        float num = 0;
        AggregateOp op;
        bool done;

        RC getNext(void *data);

        RC getNextWithGroup(void *data);

        void initGroupBy();
    };


} // namespace PeterDB

#endif // _qe_h_
