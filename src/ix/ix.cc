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
            Leaf::buildLeaf(data, NULL, NULL, NULL);
            Leaf::insertEntry(data, attribute, entry);
            ixFileHandle.appendPage(data);
            ixFileHandle.setRoot(root+1);
            delete [] data;
        } else {
            Node::insertEntry(ixFileHandle, root, attribute, entry, const_cast<RID &>(rid), nullptr);
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
        return -1;
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
        memcpy(info, data+PAGE_SIZE-sizeof(int)*NODE_SIZE, sizeof(int)*NODE_SIZE);
    }

    void Node::insertEntry(IXFileHandle &ixFileHandle, int pageNum, const Attribute &attr, keyEntry& entry, RID& rid,
                      childEntry *child) {
        char* data = new char [PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        ixFileHandle.readPage(pageNum, data);
        if(Node::isNode(data)){
            auto num = Node::findKey(data, attr, entry.key);
            insertEntry(ixFileHandle, num, attr, entry, rid, child);
            if(child!= nullptr){
                if(Node::haveSpave(data, entry.key)){
                    Node::appendKey(ixFileHandle, pageNum, entry);
                    child = nullptr;
                } else {
                    /*
                     * 1. Split this node
                     * 2. set the childEntry
                     * 3. if root, create a new node as the root pointing to two newly created nodes
                     */
                    char* newPage = new char [PAGE_SIZE];
                    memset(newPage, 0, PAGE_SIZE);
                    int* info = new int [LEAF_SIZE];
                    memset(info, 0, sizeof(int)*NODE_SIZE);

                    Node::getInfo(info, data);
                    Node::createNode(newPage, ROOT-1, info[PARENT]);
                    Node::split(data, newPage);

                    auto key = Leaf::getKey(newPage, 0);
                    int newPageNum = ixFileHandle.getNumberOfPages();

                    childEntry newChild = {key, newPageNum};
                    child = &newChild;

                    ixFileHandle.appendPage(newPage);
                    ixFileHandle.writePage(pageNum, data);

                    if(info[NODE_TYPE]==ROOT){
                        char* root = new char [PAGE_SIZE];
                        Node::createNode(newPage, ROOT, NULL);
                        keyEntry entry1 = {};
                        Node::appendKey(ixFileHandle, ixFileHandle.getNumberOfPages(), )
                    }
                }
            }
        } else {
            if(Leaf::haveSpace(data, attr, entry.key)){
                Leaf::insertEntry(data, attr, entry, rid);
                child = nullptr;
            } else {
                /*
                 * 1. Split this leaf node
                 * 2. set the childEntry
                 * 3. set sibling pointer
                 */
                char* newPage = new char [PAGE_SIZE];
                memset(newPage, 0, PAGE_SIZE);
                int* info = new int [LEAF_SIZE];
                memset(info, 0, sizeof(int)*LEAF_SIZE);

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

    void Node::split(char *data, char *page) {

    }

    void Node::writeInfo(char *data, int *info) {

    }

    char* Node::getKey(char *page, int i) {

    }

    void Leaf::getInfo(int *info, char *leafData) {
        memcpy(info, leafData+PAGE_SIZE-sizeof(int)*LEAF_SIZE, sizeof(int)*LEAF_SIZE);
    }

    void Leaf::buildLeaf(char *data, int parent, int previous, int next) {

    }

    void Leaf::createLeaf(char *page, int parent, int pre, int next) {

    }

    void Leaf::split(char *data, char *newData) {

    }

    char* Leaf::getKey(char *page, int i) {
//        return nullptr;
    }

    void Leaf::writeInfo(char *data, int *info) {

    }

} // namespace PeterDB