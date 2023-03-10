#include "src/include/rm.h"
#include <cstring>
#include <iostream>

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
        FileHandle tablesHandle;
        // Insert some metadata about the initialization for the tables table
        rbfm.openFile("Tables", tablesHandle);
        char* tuple = new char [TABLES_TUPLE_SIZE];
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        buildTablesTuple(1, "Tables", "Tables", tuple);
        rbfm.insertRecord(tablesHandle, Tables_Descriptor, tuple, rid);
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        buildTablesTuple(2, "Columns", "Columns", tuple);
        rbfm.insertRecord(tablesHandle, Tables_Descriptor, tuple, rid);
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        rbfm.closeFile(tablesHandle);
        delete [] tuple;

        // The same thing for columns table
        FileHandle columnHandle;
        rbfm.openFile("Columns", columnHandle);
        tuple = new char [COLUMNS_TUPLE_SIZE];
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        buildColumnsTuple(1, {"table-id", TypeInt, 4}, 1, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(1, {"table-name", TypeVarChar, 50}, 2, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(1, {"file-name", TypeVarChar, 50}, 3, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"table-id", TypeInt, 4}, 1, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"column-name", TypeVarChar, 50}, 2, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"column-type", TypeInt, 4}, 3, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"column-length", TypeInt, 4}, 4, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);

        buildColumnsTuple(2, {"column-position", TypeInt, 4}, 5, tuple);
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        rbfm.closeFile(columnHandle);

        // Initialize variable table
        FileHandle variableHandle;
        char* countData = new char [sizeof(int)+1];
        int initCount = 3;
        memset(countData, 0, sizeof(int));
        memcpy(countData+1, &initCount, sizeof(int));
        rbfm.openFile("Variables",variableHandle);
        rbfm.insertRecord(variableHandle, Variables_Descriptor, countData, rid);
        rbfm.closeFile(variableHandle);

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
        FileHandle tablesHandle;
        FileHandle columnHandle;
        RID rid;

        if(rbfm.openFile("Tables", tablesHandle)!=0 || rbfm.openFile("Columns", columnHandle))return -1;
        if(rbfm.createFile(tableName)!=0){
            rbfm.closeFile(tablesHandle);
            rbfm.closeFile(columnHandle);
            return -1;
        }

        char* tuple = new char [TABLES_TUPLE_SIZE];
        memset(tuple, 0, TABLES_TUPLE_SIZE);
        int tableID = getTableCount() + 1;
        buildTablesTuple(tableID, tableName, tableName, tuple);
        rbfm.insertRecord(tablesHandle, Tables_Descriptor, tuple, rid);

        // Insert Columns data
        delete [] tuple;
        tuple = new char [COLUMNS_TUPLE_SIZE];
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        for(int i = 0 ; i < attrs.size() ; i++) {
            memset(tuple, 0, COLUMNS_TUPLE_SIZE);
            buildColumnsTuple(tableID, attrs[i], i+1, tuple);
            rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        }

        tablesHandle.closeFile();
        columnHandle.closeFile();
        delete [] tuple;
        this->addTableCount();
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(tableName=="Tables"||tableName=="Columns")return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        std::string indexTableName = getIndexTableName(tableName);
        RBFM_ScanIterator iter;
        FileHandle fileHandle;
        if(rbfm.openFile(indexTableName, fileHandle)!=0)return -1;
        auto id = getTableID(tableName);
        rbfm.scan(fileHandle, Index_Descriptor, "table-id", EQ_OP, &id, {"attr-name"}, iter);
        RID rid;
        char* data = new char [INDEX_TUPLE_SIZE];
        memset(data, 0, INDEX_TUPLE_SIZE);


        while(iter.getNextRecord(rid, data)!=-1){
            std::string attrName(data+sizeof(int)+1);
            rbfm.destroyFile(getIndexName(tableName, attrName));
        }

        if(rbfm.destroyFile(tableName)!=0 || rbfm.destroyFile(indexTableName)!=0){
            iter.close();
            delete [] data;
            return -1;
        }
        iter.close();
        delete [] data;
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        char* columnData = new char [COLUMNS_TUPLE_SIZE];
        memset(columnData, 0, COLUMNS_TUPLE_SIZE);
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        RID rid;
        FileHandle columnFileHandle;

        if (rbfm.openFile("Columns", columnFileHandle) != 0) {
            return -1;
        }
        auto tableID = getTableID(tableName);

        RBFM_ScanIterator columnsIterator;

        rbfm.scan(columnFileHandle, Columns_Descriptor, "table-id", EQ_OP, &tableID, {"column-name","column-type","column-length"}, columnsIterator);

        while(columnsIterator.getNextRecord(rid, columnData)!=-1){
            char* column = columnData;
            column+=1;
            unsigned nameSize;
            memcpy(&nameSize, column, sizeof(int));
            column+=sizeof(int);
            char* attrName = new char [nameSize];
            memset(attrName, 0, nameSize);
            memcpy(attrName, column, nameSize);
            column+=nameSize;
            unsigned attrType;
            memcpy(&attrType, column, sizeof(int));
            column+=sizeof(int);
            unsigned attrSize;
            memcpy(&attrSize, column, sizeof(int));
            std::string name(attrName, nameSize);
            attrs.push_back({name, (AttrType)attrType, (AttrLength)attrSize});
            delete [] attrName;
        }

        delete [] columnData;
        columnsIterator.close();
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
            insertIndex(tableName, rid);
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
            deleteIndex(tableName, const_cast<RID &>(rid));
            if(rbfm.deleteRecord(fileHandle, attrs, rid) != 0) {
                fileHandle.closeFile();
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
            deleteIndex(tableName, const_cast<RID &>(rid));
            if(rbfm.updateRecord(fileHandle, attrs, data, rid) != 0) {
                return -1;
            }
            insertIndex(tableName, const_cast<RID &>(rid));
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
        rbfm.closeFile(fileHandle);
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
        rc = rbfm.openFile(tableName, rm_ScanIterator.fileHandle);
        if( rc != 0) {
            return -1;
        }

        rc = rbfm.scan(rm_ScanIterator.fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmScanIterator);
        rm_ScanIterator.init = true;
        return rc;
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return rbfmScanIterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        if(!init)return 0;
        return rbfmScanIterator.close();
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        FileHandle columnHandle;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfmScanIterator;
        rbfm.openFile("Columns", columnHandle);
        int size = attributeName.size();
        char* condition = new char [sizeof(int)+size];
        memset(condition, 0, sizeof(int)+size);
        memcpy(condition, &size, sizeof(int));
        memcpy(condition+sizeof(int), attributeName.c_str(), size);
        rbfm.scan(columnHandle, Columns_Descriptor, "column-name", EQ_OP, condition, {{"table-id"}}, rbfmScanIterator);
        RID rid;
        char* tuple = new char [COLUMNS_TUPLE_SIZE];
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        rbfmScanIterator.getNextRecord(rid, tuple);

        if(rbfm.deleteRecord(columnHandle, Columns_Descriptor, rid)){
            std::cout <<"Error when deleting"<< std::endl;
        }
        rbfmScanIterator.close();
        return 0;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        auto id = getTableID(tableName);
        FileHandle columnHandle;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rbfm.openFile("Columns", columnHandle);
        char* tuple = new char [COLUMNS_TUPLE_SIZE];
        memset(tuple, 0, COLUMNS_TUPLE_SIZE);
        buildColumnsTuple(id, attr, 0, tuple);
        RID rid;
        rbfm.insertRecord(columnHandle, Columns_Descriptor, tuple, rid);
        columnHandle.closeFile();

        delete [] tuple;
        return 0;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        if(!containAttribute(tableName, attributeName)){
            std::cout<< "No such attribute in "<< tableName << std::endl;
            return -1;
        }
        std::string indexName = getIndexName(tableName, attributeName);
        IndexManager& indexManager = IndexManager::instance();
        indexManager.createFile(indexName);

        IXFileHandle ixFileHandle;
        indexManager.openFile(indexName, ixFileHandle);

        RM_ScanIterator iter;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        Attribute attribute;
        for(int i = 0; i < attrs.size(); i++){
            if(attrs[i].name == attributeName){
                attribute = attrs[i];
                break;
            }
        }
        // TODO: Reflect existence in the catalog
        FileHandle handle;
        std::string indexTableName = getIndexTableName(tableName);
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(rbfm.openFile(indexTableName, handle)!=0){
            rbfm.createFile(indexTableName);
            rbfm.openFile(indexTableName, handle);
        }
        RID rid;
        char* indexTuple = new char [INDEX_TUPLE_SIZE];
        memset(indexTuple, 0, INDEX_TUPLE_SIZE);
        buildIndexTuple(getTableID(tableName), tableName, attributeName, indexTuple);
        rbfm.insertRecord(handle, Index_Descriptor, indexTuple, rid);

        std::vector<std::string> attrNames;
        attrNames.push_back(attribute.name);
        scan(tableName, "", NO_OP, NULL, attrNames, iter);

        char* data = new char [PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        char* key = new char [PAGE_SIZE];
        while(iter.getNextTuple(rid, data) != RM_EOF){
            memset(key, 0, PAGE_SIZE);
            int keyLength = attribute.length + sizeof(int);
            memcpy(key, data + sizeof(char), keyLength);
            indexManager.insertEntry(ixFileHandle, attribute, key, rid);
        }

        delete [] data;
        delete [] key;
        delete [] indexTuple;
        indexManager.closeFile(ixFileHandle);
        iter.close();
        return 0;
    }
    // Deletion remaining work to do
    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        if(!containAttribute(tableName, attributeName)){
            std::cout<< "No such attribute in "<< tableName << std::endl;
            return -1;
        }
        std::string indexName = getIndexName(tableName, attributeName);
        IndexManager& indexManager = IndexManager::instance();
        indexManager.destroyFile(indexName);

        std::string indexTableName = getIndexTableName(tableName);
        indexManager.destroyFile(indexTableName);
        return 0;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);

        Attribute targetAttr;
        for(auto attr : attrs) {
            if(attr.name == attributeName) {
                targetAttr = attr;
            }
        }

        IndexManager &indexManager = IndexManager::instance();
        std::string indexFileName = getIndexName(tableName, attributeName);
        indexManager.openFile(indexFileName, rm_IndexScanIterator.ixFileHandle);
        indexManager.scan(rm_IndexScanIterator.ixFileHandle, targetAttr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.iter);
        return 0;
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

    void RelationManager::buildIndexTuple(int id, const std::string &tableName, const std::string &attrName, char *tuple) {
        unsigned tableNameSize = tableName.size();
        unsigned attrNameSize = attrName.size();
        char nullpart = 0;
        auto offset = 0;

        memcpy(tuple+offset, &nullpart, 1);
        offset+=1;
        memcpy(tuple+offset, &id, sizeof(int));
        offset+=sizeof(int);
        memcpy(tuple+offset, &tableNameSize, sizeof(int));
        offset+=sizeof(int);
        memcpy(tuple+offset, tableName.c_str(), tableNameSize);
        offset+=tableNameSize;
        memcpy(tuple+offset, &attrNameSize, sizeof(int));
        offset+=sizeof(int);
        memcpy(tuple+offset, attrName.c_str(), attrNameSize);
    }

    int RelationManager::getTableID(const std::string &tableName){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle tablesHandle;
        rbfm.openFile("Tables", tablesHandle);
        int tableID = 0;
        RBFM_ScanIterator rbfmScanIterator;

        int size = tableName.size();
        char* condition = new char [sizeof(int)+size];
        memset(condition, 0, sizeof(int)+size);
        memcpy(condition, &size, sizeof(int));
        memcpy(condition+sizeof(int), tableName.c_str(), size);
        rbfm.scan(tablesHandle, Tables_Descriptor, "table-name", EQ_OP, condition, {{"table-id"}}, rbfmScanIterator);
        RID rid;
        char* data = new char [sizeof(int)+1];
        memset(data, 0, sizeof(int)+1);
        rbfmScanIterator.getNextRecord(rid, data);
        tableID = *(int*)(data+1);

        rbfmScanIterator.close();
        delete [] condition;
        delete [] data;
        return tableID;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        if(iter.getNextEntry(rid, key) != 0) {
            return RM_EOF;
        }
        return 0;
    }

    RC RM_IndexScanIterator::close(){
        return iter.close();
    }

    void RelationManager::addTableCount() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle varFile;
        int count = getTableCount() + 1;
        char* countData = new char [sizeof(int)+1];
        memset(countData, 0, sizeof(int)+1);
        memcpy(countData+1, &count, sizeof(int));
        rbfm.openFile("Variables",varFile);
        rbfm.updateRecord(varFile, Variables_Descriptor, countData, {0,0});
        varFile.closeFile();
        delete [] countData;
    }

    int RelationManager::getTableCount() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        int count = 0;
        char* countData = new char [sizeof(int)+1];
        memset(countData, 0, sizeof(int)+1);
        rbfm.openFile("Variables",fileHandle);
        rbfm.readRecord(fileHandle, Variables_Descriptor, {0,0}, countData);
        fileHandle.closeFile();
        memcpy(&count, countData+1, sizeof(int));
        delete[] countData;
        return count;
    }

    bool RelationManager::containAttribute(const std::string &tableName, const std::string &attrbuteName) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        for(int i=0;i<attrs.size();i++){
            if(attrs.at(i).name==attrbuteName){
                return true;
            }
        }
        return false;
    }

    std::string RelationManager::getIndexName(const std::string &tableName, const std::string &attrName) {
        return tableName + "_" + attrName + ".idx";;
    }

    std::string RelationManager::getIndexTableName(const std::string &tableName) {
        return tableName+"Indices";
    }

    void RelationManager::insertIndex(const std::string &tableName, RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& indexManager = IndexManager::instance();

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        std::string indexTable = getIndexTableName(tableName);
        RBFM_ScanIterator iter;
        FileHandle fileHandle;
        rbfm.openFile(indexTable, fileHandle);
        auto id = getTableID(tableName);
        rbfm.scan(fileHandle, Index_Descriptor, "table-id", EQ_OP, &id, {"attr-name"}, iter);
        char* data = new char [INDEX_TUPLE_SIZE];
        memset(data, 0, INDEX_TUPLE_SIZE);


        while(iter.getNextRecord(rid, data)!=-1){
            std::string attrName(data+sizeof(int)+1);
            std::string indexName = getIndexName(tableName, attrName);
            IXFileHandle ixFileHandle;
            indexManager.openFile(indexName, ixFileHandle);

            Attribute targetAttr;
            for(auto attr : attrs) {
                if(attr.name == attrName) {
                    targetAttr = attr;
                }
            }
            char* keyData = new char [targetAttr.length+sizeof(int)+1];
            memset(keyData, 0, targetAttr.length+sizeof(int)+1);
            readAttribute(tableName, rid, attrName, keyData);
            char nullId = keyData[0];
            if(nullId!=-128){
                indexManager.insertEntry(ixFileHandle, targetAttr, keyData+1, rid);
            }
            delete [] keyData;
        }

        delete [] data;
        iter.close();
    }

    void RelationManager::deleteIndex(const std::string &tableName, RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& indexManager = IndexManager::instance();

        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        std::string indexTable = getIndexTableName(tableName);
        RBFM_ScanIterator iter;
        FileHandle fileHandle;
        rbfm.openFile(indexTable, fileHandle);
        auto id = getTableID(tableName);
        rbfm.scan(fileHandle, Index_Descriptor, "table-id", EQ_OP, &id, {"attr-name"}, iter);
        char* data = new char [INDEX_TUPLE_SIZE];
        memset(data, 0, INDEX_TUPLE_SIZE);


        while(iter.getNextRecord(rid, data)!=-1){
            std::string attrName(data+sizeof(int)+1);
            std::string indexName = getIndexName(tableName, attrName);
            IXFileHandle ixFileHandle;
            indexManager.openFile(indexName, ixFileHandle);

            Attribute targetAttr;
            for(auto attr : attrs) {
                if(attr.name == attrName) {
                    targetAttr = attr;
                }
            }
            char* keyData = new char [targetAttr.length+sizeof(int)+1];
            memset(keyData, 0, targetAttr.length+sizeof(int)+1);
            readAttribute(tableName, rid, attrName, keyData);
            char nullId = keyData[0];
            if(nullId!=-128){
                indexManager.deleteEntry(ixFileHandle, targetAttr, keyData+1, rid);
            }
            delete [] keyData;
        }

        delete [] data;
        iter.close();
    }
} // namespace PeterDB