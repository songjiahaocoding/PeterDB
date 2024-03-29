#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include "src/include/ix.h"
#include "src/include/rbfm.h"

namespace PeterDB {
    #define RM_EOF (-1)  // end of a scan operator
    const std::vector<Attribute> Tables_Descriptor = {
            {
                    "table-id",
                    TypeInt,
                    (AttrLength)4
            },
            {
                    "table-name",
                    TypeVarChar,
                    (AttrLength)50
            },
            {
                    "file-name",
                    TypeVarChar,
                    (AttrLength)50
            }
    };

    const std::vector<Attribute> Columns_Descriptor = {
            {
                    "table-id",
                    TypeInt,
                    (AttrLength) 4
            },
            {
                    "column-name",
                    TypeVarChar,
                    (AttrLength) 50
            },
            {
                    "column-type",
                    TypeInt,
                    (AttrLength) 4
            },
            {
                    "column-length",
                    TypeInt,
                    (AttrLength) 4
            },
            {
                    "column-position",
                    TypeInt,
                    (AttrLength) 4
            }
    };

    const std::vector<Attribute> Index_Descriptor = {
            {
                    "table-id",
                    TypeInt,
                    (AttrLength)4
            },
            {
                    "table-name",
                    TypeVarChar,
                    (AttrLength)50
            },
            {
                    "attr-name",
                    TypeVarChar,
                    (AttrLength)50
            }
    };
    const std::vector<Attribute> Variables_Descriptor{
            {
                "count",
                TypeInt,
                (AttrLength)4
            }
    };
    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator() = default;

        ~RM_ScanIterator() = default;

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();

        RBFM_ScanIterator rbfmScanIterator;

        FileHandle fileHandle;

        bool init = false;
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan

        IX_ScanIterator iter;
        IXFileHandle ixFileHandle;
    };

    #define TABLES_TUPLE_SIZE 50*2+4*3+1
    #define COLUMNS_TUPLE_SIZE 50+4*5+1
    #define INDEX_TUPLE_SIZE 50*2+4*3+1
    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment
        void buildTablesTuple(int i, std::string tableName, std::string fileName, char *tuple);

        void buildColumnsTuple(int tableId, Attribute attribute, int columnPos, char *tuple);

        int getTableCount();

        void addTableCount();

        int getTableID(const std::string &tableName);

        bool containAttribute(const std::string &tableName, const std::string &attrbuteName);

        std::string getIndexName(const std::string &tableName, const std::string &attrName);

        std::string getIndexTableName(const std::string &tableName);

        void buildIndexTuple(int id, const std::string &tableName, const std::string &attrName, char *tuple);

        void insertIndex(const std::string &tableName, RID &rid);

        void deleteIndex(const std::string &tableName, RID &rid);
    };

} // namespace PeterDB

#endif // _rm_h_