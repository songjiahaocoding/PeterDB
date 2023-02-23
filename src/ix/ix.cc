#include <iostream>
#include <queue>
#include "src/include/ix.h"

namespace PeterDB {
    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }
    IXFileHandle::~IXFileHandle() {
        fileHandle.closeFile();
    }
    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    }
    int IXFileHandle::getRoot() {
        return fileHandle.getRoot();
    }
    void IXFileHandle::setRoot(unsigned num){
        fileHandle.setRoot(num);
    }
    RC IXFileHandle::readPage(PageNum pageNum, void *data) {
        memset(data, 0, PAGE_SIZE);
        return fileHandle.readPage(pageNum, data);
    }
    RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
        return fileHandle.writePage(pageNum, data);
    }
    RC IXFileHandle::appendPage(const void *data) {
        return fileHandle.appendPage(data);
    }
    unsigned IXFileHandle::getNumberOfPages() {
        return fileHandle.getNumberOfPages();
    }
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }
    RC IndexManager::createFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }
    RC IndexManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }
    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.openFile(fileName, ixFileHandle.fileHandle);
    }
    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.closeFile(ixFileHandle.fileHandle);
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void* key, const RID &rid) {
        auto root = ixFileHandle.getRoot();
        keyEntry entry;
        entry.key = (char*)key;
        if(root==-1){
            // Initialization
            char* data = new char [PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            Leaf::createLeaf(data, NULL, NULL, NULL);
            Leaf::insertEntry(data, attribute, entry, const_cast<RID &>(rid));
            ixFileHandle.appendPage(data);
            ixFileHandle.setRoot(root+1);
            delete [] data;
        } else {
            Node::insertEntry(ixFileHandle, root, const_cast<Attribute &>(attribute), entry, const_cast<RID &>(rid), nullptr);
        }
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void* key, const RID &rid) {
        int root = ixFileHandle.getRoot();
        if (root == -1) {return -1;}
        keyEntry entry;
        entry.key = (char*)key;
        Node::deleteEntry(ixFileHandle, NULL, root, attribute, entry, nullptr);
        return 0;
    }
    /*
     * highKey == null -> upper limit is +infinity
     * lowKey == null -> lower limit is -infinity
     */
    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    // Construct the B+ tree in a recursive way, print in JSON format
    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        int root = ixFileHandle.getRoot();
        if (root == -1) {std::cout<<"ERROR: No root"<<std::endl;}
        Node::print(ixFileHandle, attribute, root, 0);
        std::cout<<std::endl;
        return 0;
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }


    void Node::getInfo(int *info, char *data) {
        memset(info, 0, sizeof(int)*NODE_SIZE);
        auto base = data+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<NODE_SIZE;i++){
            memcpy(&info[i], base-offset, sizeof(int));
            offset += sizeof(int);
        }
    }

    void Node::insertEntry(IXFileHandle &ixFileHandle, int pageNum, Attribute &attr, keyEntry& entry, RID& rid,
                      childEntry *child) {
        char* data = new char [PAGE_SIZE];
        ixFileHandle.readPage(pageNum, data);
        if(Node::isNode(data)){
            auto num = Node::findKey(data, attr, entry.key);
            insertEntry(ixFileHandle, num, attr, entry, rid, child);
            if(child != nullptr){
                keyEntry newEntry;
                newEntry.key = child->key;
                newEntry.right = child->pageNum;
                if(Node::haveSpace(data, entry.key, attr)){
                    Node::appendKey(ixFileHandle, pageNum, newEntry, attr);
                    child = nullptr;
                }
                else {
                    char* newPage = new char [PAGE_SIZE];
                    int* info = new int [NODE_SIZE];

                    // Node will send the middleKey to upper layer
                    Node::getInfo(info, data);
                    Node::createNode(newPage, NODE, info[PARENT]);
                    char* middleKey = new char [PAGE_SIZE];
                    memset(middleKey, 0, PAGE_SIZE);
                    Node::split(data, newPage, middleKey);

                    int newPageNum = ixFileHandle.getNumberOfPages();
                    ixFileHandle.appendPage(newPage);
                    ixFileHandle.writePage(pageNum, data);

                    if(Tool::compare(child->key, middleKey, attr)<0){
                        Node::insertEntry(ixFileHandle, pageNum, attr, newEntry, rid, nullptr);
                    } else {
                        Node::insertEntry(ixFileHandle, newPageNum, attr, newEntry, rid, nullptr);
                    }

                    childEntry newChild = {middleKey, newPageNum};
                    child = &newChild;

                    if(info[NODE_TYPE]==ROOT){
                        char* root = new char [PAGE_SIZE];
                        Node::createNode(root, ROOT, NULL);
                        keyEntry entry1;
                        entry1.left = pageNum;
                        entry1.right = newPageNum;
                        entry1.key = Node::getKey(newPage, 0);
                        Node::appendKey(ixFileHandle, ixFileHandle.getNumberOfPages(), entry1, attr);
                        delete [] root;
                    }

                    delete [] newPage;
                    delete [] info;
                }
            }
        }
        else {
            if(Leaf::haveSpace(data, attr, entry.key)){
                Leaf::insertEntry(data, attr, entry, rid);
                child = nullptr;
            }
            else {
                char* newPage = new char [PAGE_SIZE];
                int* info = new int [LEAF_SIZE];

                Leaf::getInfo(info, data);
                Leaf::createLeaf(newPage, info[PARENT], pageNum, info[NEXT]);
                Leaf::split(data, newPage);

                info[NEXT] = ixFileHandle.getNumberOfPages();
                childEntry newChild = {Leaf::getKey(newPage, 0), info[NEXT]};
                child = &newChild;

                Leaf::writeInfo(data, info);
                ixFileHandle.writePage(pageNum, data);
                ixFileHandle.appendPage(newPage);

                delete [] newPage;
                delete [] info;
            }
        }
        delete [] data;
    }

    void Node::deleteEntry(IXFileHandle &ixFileHandle, int paPageNum, int pageNum, const Attribute &attr, keyEntry &entry,
                           childEntry *child) {
        char* data = new char [PAGE_SIZE];
        ixFileHandle.readPage(pageNum, data);
        if(Node::isNode(data)){
            auto num = Node::findKey(data, attr, entry.key);
            deleteEntry(ixFileHandle, pageNum, num, attr, entry, child);
            // No use of child because of the lazy delete
//            keyEntry newEntry;
//            newEntry.key = child->key;
//            Node::removeKey(ixFileHandle, pageNum, newEntry, attr);
//            ixFileHandle.writePage(pageNum, data);
        }
        else {
            Leaf::deleteEntry(data, attr, entry);
            ixFileHandle.writePage(pageNum, data);
        }
        delete [] data;
    }

    void Node::writeInfo(char *data, int *info) {
        auto base = data+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<NODE_SIZE;i++){
            memcpy(base-offset, &info[i], sizeof(int));
            offset += sizeof(int);
        }
    }

    char* Node::getKey(char *page, int i) {

    }

    bool Node::isNode(char *pageData) {
        int* info = new int [NODE_SIZE];
        getInfo(info, pageData);
        return info[NODE_TYPE] != LEAF;
    }

    void Node::print(IXFileHandle &ixFileHandle, const Attribute &attribute, int root, int depth) {
        char* data = new char [PAGE_SIZE];
        ixFileHandle.readPage(root, data);
        if(!isNode(data)){
            Leaf::print(data, attribute);
            delete [] data;
            return;
        }

        std::cout<<"{\"keys\": [";

        std::queue<int> children;
    }
    // Binary search to find the key
    int Node::findKey(char *data, const Attribute &attr, const char *key) {
        int* info = new int [NODE_SIZE];
        getInfo(info, data);
        int l = 0, r = info[SLOT_NUM], mid;
        while(l<r){
            mid = l+(r-l)/2;

        }

        return 0;
    }

    void Node::createNode(char *data, int type, int parent) {
        memset(data, 0, PAGE_SIZE);
        int* info = new int [NODE_SIZE];
        info[INFO_OFFSET] = sizeof(int)*NODE_SIZE;
        info[PARENT] = parent;
        info[NODE_TYPE] = type;
        writeInfo(data, info);
    }

    bool Node::haveSpace(char *data, const char *key, Attribute& attr) {
        int* info = new int [NODE_SIZE];
        getInfo(info, data);
        auto empty = PAGE_SIZE - info[DATA_OFFSET] - info[INFO_OFFSET];
        int space = 0;
        switch (attr.type) {
            case TypeInt:
            case TypeReal:
                space+=4;
                break;
            case TypeVarChar:
                int len = 0;
                memcpy(&len, key, sizeof(int));
                space+=len+sizeof(int);
                break;
        }
        space += SLOT_SIZE + sizeof(int);   // + the size of an index
        return empty>=space;
    }

    void Leaf::getInfo(int *info, char *leafData) {
        memset(info, 0, sizeof(int)*LEAF_SIZE);
        auto base = leafData+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<LEAF_SIZE;i++){
            memcpy(&info[i], base-offset, sizeof(int));
            offset += sizeof(int);
        }
    }

    void Leaf::createLeaf(char *page, int parent, int pre, int next) {
        memset(page, 0, PAGE_SIZE);
        int* info = new int [LEAF_SIZE];
        memset(info, 0, sizeof(int)*LEAF_SIZE);
        info[INFO_OFFSET] = sizeof(int)*LEAF_SIZE;
        info[PARENT] = parent;
        info[PRE] = pre;
        info[NODE_TYPE] = LEAF;
        info[NEXT] = next;
        writeInfo(page, info);
    }

    void Leaf::split(char *data, char *newData) {

    }

    char* Leaf::getKey(char *page, int i) {
//        return nullptr;
    }

    // Only write info to in-memory data, don't flush back to disk
    void Leaf::writeInfo(char *data, int *info) {
        auto base = data+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<LEAF_SIZE;i++){
            memcpy(base-offset, &info[i], sizeof(int));
            offset += sizeof(int);
        }
    }

    void Leaf::print(char *data, const Attribute &attr) {

    }

    bool Leaf::haveSpace(char *data, const Attribute &attr, const char *key) {
        int* info = new int [LEAF_SIZE];
        getInfo(info, data);
        auto empty = PAGE_SIZE - info[DATA_OFFSET] - info[INFO_OFFSET];
        int space = 0;
        switch (attr.type) {
            case TypeInt:
            case TypeReal:
                space+=4;
                break;
            case TypeVarChar:
                int len = 0;
                memcpy(&len, key, sizeof(int));
                space+=len+sizeof(int);
                break;
        }
        space += SLOT_SIZE + sizeof(RID);   // + the size of an index
        return empty>=space;
    }

    float Tool::compare(char *key1, char *key2, Attribute &attr) {
        switch (attr.type) {
            case TypeInt:
            {
                int intVal1 = *(int*)key1;
                int intVal2 = *(int*)key2;
                return intVal1-intVal2;
            }
            case TypeReal:
            {
                float floatVal1 = *(float*)key1;
                float floatVal2 = *(float*)key2;
                if((floatVal1-floatVal2)<FLOAT_DIFF)return 0;
                else return floatVal1-floatVal2;
            }
            case TypeVarChar:
            {
                int len1 = 0;
                int len2 = 0;

                memcpy(&len1, key1, sizeof(int));
                memcpy(&len2, key2, sizeof(int));

                char* val1 = new char [len1+1];
                memset(val1, 0, len1+1);
                char* val2 = new char [len2+1];
                memset(val2, 0, len2+1);

                memcpy(val1, key1, len1);
                memcpy(val2, key2, len2);

                auto res = strcmp(val1, val2);
                delete [] val1;
                delete [] val2;
                return res;
            }
        }
        return 0;
    }
} // namespace PeterDB