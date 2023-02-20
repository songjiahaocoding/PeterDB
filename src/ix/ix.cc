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

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        auto root = ixFileHandle.getRoot();
        if(root==-1){
            // Initialization
            char* data = new char [PAGE_SIZE];
            memset(data, 0, PAGE_SIZE);
            Leaf::buildLeaf(data, NULL, NULL, NULL);
            Leaf::insertEntry(data, attribute, static_cast<const char *>(key), rid);
            ixFileHandle.appendPage(data);
            ixFileHandle.setRoot(root+1);
            delete [] data;
            return 0;
        } else {
            return insertInPage(ixFileHandle, attribute, key, rid, root);
        }
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int root = ixFileHandle.getRoot();
        if (root == -1) {return -1;}
        return deleteInPage(ixFileHandle, attribute, key, rid, root);
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

    RC IndexManager::insertInPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid,
                               int pageNum) {


        return 0;
    }

    RC IndexManager::deleteInPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid,
                               int pageNum) {
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
        memcpy(info, data+PAGE_SIZE-sizeof(int)*NODE_SIZE, sizeof(int)*NODE_SIZE);
    }

    void Leaf::getInfo(int *info, char *leafData) {
        memcpy(info, leafData+PAGE_SIZE-sizeof(int)*LEAF_SIZE, sizeof(int)*LEAF_SIZE);
    }

    void Leaf::buildLeaf(char *data, int parent, int previous, int next) {

    }
} // namespace PeterDB