#include "src/include/rm.h"
#include <cstring>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rbfm.createFile("Tables");
        rbfm.createFile("Columns");
        rbfm.createFile("Variables");

        RID rid;
        FileHandle fileHandle;
        // Insert some metadata about the initialization for the tables table
        rbfm.openFile("Tables", fileHandle);
        char* tuple = new char [TABLES_TUPLE_SIZE];
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        buildTablesTuple(1, "Tables", "Tables", tuple);
        rbfm.insertRecord(fileHandle, Tables_Descriptor, tuple, rid);
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        buildTablesTuple(2, "Columns", "Columns", tuple);
        rbfm.insertRecord(fileHandle, Tables_Descriptor, tuple, rid);
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        fileHandle.closeFile();
        delete [] tuple;

        // The same thing for columns table
        rbfm.openFile("Columns", fileHandle);
        tuple = new char [COLUMNS_TUPLE_SIZE];
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        buildColumnsTuple(1, {"tableID", TypeInt, 4}, 1, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(1, {"tableName", TypeVarChar, 50}, 2, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(1, {"fileName", TypeVarChar, 50}, 3, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"tableID", TypeInt, 4}, 1, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"columnName", TypeVarChar, 50}, 2, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"columnType", TypeInt, 4}, 3, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"columnLength", TypeInt, 4}, 4, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"columnPosition", TypeInt, 4}, 5, tuple);
        rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        fileHandle.closeFile();
        // Initialize variable table
        char* countData = new char [sizeof(int)+1];
        int initCount = 3;
        memset(countData, 0, sizeof(int));
        memcpy(countData+1, &initCount, sizeof(int));
        rbfm.openFile("Variables",fileHandle);
        rbfm.insertRecord(fileHandle, {{"count", TypeInt, 4}}, countData, rid);

        fileHandle.closeFile();
        delete [] countData;
        delete [] tuple;
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rbfm.destroyFile("Tables");
        rbfm.destroyFile("Columns");
        rbfm.destroyFile("Variables");
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RID rid;
        // Insert Tables data
        if(rbfm.openFile("Tables", fileHandle)!=0)return -1;
        if(rbfm.createFile(tableName)!=0)return -1;
        char* tuple = new char [TABLES_TUPLE_SIZE];
        int tableID = getTableCount() + 1;

        buildTablesTuple(tableID, tableName, tableName, tuple);
        rbfm.insertRecord(fileHandle, Tables_Descriptor, tuple, rid);
        fileHandle.closeFile();
        // Insert Columns data
        delete [] tuple;
        tuple = new char [COLUMNS_TUPLE_SIZE];
        rbfm.openFile("Columns", fileHandle);
        for(int i = 0 ; i < attrs.size() ; i++) {
            memset(tuple, 0, COLUMNS_TUPLE_SIZE);
            buildColumnsTuple(tableID, attrs[i], i+1, tuple);
            rbfm.insertRecord(fileHandle, Columns_Descriptor, tuple, rid);
        }
        fileHandle.closeFile();
        delete [] tuple;
        this->addTableCount();
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        if(rbfm.destroyFile(tableName)==0)return 0;
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        char* tableData = new char [TABLES_TUPLE_SIZE];
        char* columnData = new char [COLUMNS_TUPLE_SIZE];
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator tableIterator;
        char* tableID = new char [sizeof (int)];
        RID rid;
        FileHandle tableFileHandle;
        FileHandle columnFileHandle;

        if (rbfm.openFile("Tables", tableFileHandle) != 0 || rbfm.openFile("Columns", columnFileHandle) != 0) {
            return -1;
        }
        int size = tableName.size();
        char* condition = new char [sizeof(int)+size];
        memcpy(condition, &size, sizeof(int));
        memcpy(condition+sizeof(int), tableName.c_str(), size);
        rbfm.scan(tableFileHandle, Tables_Descriptor, "tableName", EQ_OP, condition, {"tableID"}, tableIterator);
        if(tableIterator.getNextRecord(rid, tableData)==-1){
            return -1;
        }
        memcpy(tableID, tableData+1, sizeof(int));

        RBFM_ScanIterator columnsIterator;

        rbfm.scan(columnFileHandle, Columns_Descriptor, "tableID", EQ_OP, tableID, {"columnName","columnType","columnLength"}, columnsIterator);

        while(columnsIterator.getNextRecord(rid, columnData)!=-1){
            char* column = columnData;
            column+=1;
            unsigned nameSize;
            memcpy(&nameSize, column, sizeof(int));
            column+=sizeof(int);
            char* attrName = new char [nameSize];
            memset(attrName, 0, nameSize+1);
            memcpy(attrName, column, nameSize);
            column+=nameSize;
            unsigned attrType;
            memcpy(&attrType, column, sizeof(int));
            column+=sizeof(int);
            unsigned attrSize;
            memcpy(&attrSize, column, sizeof(int));
            std::string name(attrName);
            attrs.push_back({name, (AttrType)attrType, (AttrLength)attrSize});
            delete [] attrName;
        }

        delete [] tableData;
        delete [] condition;
        columnFileHandle.closeFile();
        tableFileHandle.closeFile();
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        if( tableName == "Tables" || tableName == "Columns") {
            return -1;
        }

        std::vector<Attribute> attrs;
        getAttributes(tableName,attrs);
        if (rbfm.openFile(tableName, fileHandle) == 0) {
            rbfm.insertRecord(fileHandle, attrs, data, rid);
            fileHandle.closeFile();
            return 0;
        }
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> attrs;
        getAttributes(tableName,attrs);
        if (rbfm.openFile(tableName, fileHandle) == 0) {
            if(rbfm.deleteRecord(fileHandle, attrs, rid) != 0) {
                return -1;
            }
            fileHandle.closeFile();
            return 0;
        }
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> attrs;
        getAttributes(tableName,attrs);

        if (rbfm.openFile(tableName, fileHandle) == 0) {
            if(rbfm.updateRecord(fileHandle, attrs, data, rid) != 0) {
                return -1;
            }
            fileHandle.closeFile();
            return 0;
        }
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> attrs;
        getAttributes(tableName,attrs);
        if (rbfm.openFile(tableName, fileHandle) != 0) {
            return -1;
        }
        if (rbfm.readRecord(fileHandle, attrs, rid, data) != 0 ) {
            return -1;
        }
        fileHandle.closeFile();
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rbfm.printRecord(attrs, data, out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> attrs;
        getAttributes(tableName,attrs);
        if (rbfm.openFile(tableName, fileHandle) == 0 &&
            rbfm.readAttribute(fileHandle, attrs, rid, attributeName, data) == 0 ) {
            fileHandle.closeFile();
            return 0;
        }
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        std::vector<Attribute> attrs;
        this->getAttributes(tableName,attrs);

        RC rc;
        FileHandle fileHandle;
        rc = rbfm.openFile(tableName, fileHandle);
        if( rc != 0) {
            return -1;
        }

        rc = rbfm.scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmScanIterator);
        return rc;
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return rbfmScanIterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        return rbfmScanIterator.close();
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

    void RelationManager::buildTablesTuple(int id, std::string tableName, std::string fileName, char *tuple) {
        unsigned tableNameSize = tableName.size();
        unsigned fileNameSize = fileName.size();

        char nullpart = 0;
        memcpy(tuple, &nullpart, 1);
        tuple+=1;
        memcpy(tuple, &id, sizeof(int));
        tuple+=sizeof(int);
        memcpy(tuple, &tableNameSize, sizeof(int));
        tuple+=sizeof(int);
        memcpy(tuple, tableName.c_str(), tableNameSize);
        tuple+=tableNameSize;
        memcpy(tuple, &fileNameSize, sizeof(int));
        tuple+=sizeof(int);
        memcpy(tuple, fileName.c_str(), fileNameSize);
    }

    void RelationManager::buildColumnsTuple(int tableId, Attribute attribute, int columnPos, char* tuple){
        char* ptr = tuple;
        AttrType columnType = attribute.type;
        unsigned columnLength = attribute.length;
        unsigned columnNameSize =  attribute.name.size();

        char* nullpart = 0;
        memcpy(ptr, &nullpart, 1);
        ptr+=1;
        memcpy(ptr, &tableId, sizeof(int));
        ptr+=sizeof(int);
        memcpy(ptr, &columnNameSize, sizeof(int));
        ptr+=sizeof(int);
        memcpy(ptr, attribute.name.c_str(), columnNameSize);
        ptr+=columnNameSize;
        memcpy(ptr, &columnType, sizeof(AttrType));
        ptr+=sizeof(AttrType);
        memcpy(ptr, &columnLength, sizeof(unsigned));
        ptr+=sizeof(unsigned);
        memcpy(ptr, &columnPos, sizeof(int));
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

    void RelationManager::addTableCount() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle varFile;
        int count = getTableCount() + 1;
        char* countData = new char [sizeof(int)+1];
        memset(countData, 0, sizeof(int)+1);
        memcpy(countData+1, &count, sizeof(int));
        rbfm.openFile("Variables",varFile);
        rbfm.updateRecord(varFile, {{"count", TypeInt, 4}}, countData, {0,0});
        varFile.closeFile();
    }

    int RelationManager::getTableCount() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        int count = 0;
        char* countData = new char [sizeof(int)+1];
        rbfm.openFile("Variables",fileHandle);
        rbfm.readRecord(fileHandle, {{"count", TypeInt, 4}}, {0,0}, countData);
        fileHandle.closeFile();
        memcpy(&count, countData+1, sizeof(int));
        delete[] countData;
        return count;
    }
} // namespace PeterDB