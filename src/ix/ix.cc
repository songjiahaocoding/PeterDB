#include <iostream>
#include <queue>
#include "src/include/ix.h"
#include <cstring>
#include <climits>
#include <cmath>

namespace PeterDB {
    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        rootPage = new char [PAGE_SIZE];
    }
    IXFileHandle::~IXFileHandle() {
        fileHandle.closeFile();
        delete [] rootPage;
    }
    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    }
    int IXFileHandle::getRoot() {
        if(getNumberOfPages()==0)return -1;
        int root = 0;
        readPage(0, rootPage);
        memcpy(&root, rootPage, sizeof(int));
        return root;
    }
    void IXFileHandle::setRoot(unsigned num){
        memcpy(rootPage, &num, sizeof(int));
        writePage(0, rootPage);
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
            root = 1;
            memcpy(data, &root, sizeof(int));
            ixFileHandle.appendPage(data);

            Leaf::createLeaf(data, NULL_NODE, NULL_NODE, NULL_NODE);
            int* info = new int [TREE_NODE_SIZE];
            Leaf::getInfo(info, data);
            info[NODE_TYPE] = ROOT;
            Tool::writeInfo(data, info);
            Leaf::insertEntry(data, attribute, entry, const_cast<RID &>(rid));
            ixFileHandle.appendPage(data);
            ixFileHandle.setRoot(1);
            delete [] data;
            delete [] info;
        } else {
            keyEntry child;
            child.left = -1;
            Node::insertEntry(ixFileHandle, root, const_cast<Attribute &>(attribute), entry, const_cast<RID &>(rid), &child);
        }
        return 0;
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void* key, const RID &rid) {
        int root = ixFileHandle.getRoot();
        if (root == -1) {return -1;}
        keyEntry entry;
        entry.key = (char*)key;
        return Node::deleteEntry(ixFileHandle, NULL_NODE, root, attribute, entry, const_cast<RID &>(rid), nullptr);
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
        return ix_ScanIterator.init(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    }

    // Construct the B+ tree in a recursive way, print in JSON format
    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        int root = ixFileHandle.getRoot();
        if (root == -1) {std::cout<<"ERROR: No root"<<std::endl;}
        Node::print(ixFileHandle, attribute, root, 0, out);
        return 0;
    }

    IX_ScanIterator::IX_ScanIterator() {

    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if(pageNum==-1)return IX_EOF;

        auto slot = Tool::getSlot(page, slotNum);
        memcpy(key, page+slot.offset, slot.len);
        if(!inRange(static_cast<char *>(key))){
            memset(key, 0, slot.len);
            return IX_EOF;
        }
        memcpy(&rid, page+slot.offset+slot.len+sizeof(RID)*ridNum, sizeof(RID));
        moveToNext();
        return 0;
    }

    RC IX_ScanIterator::close() {
        delete [] page;
        if(low != nullptr)delete [] low;
        if(high != nullptr)delete [] high;
        return 0;
    }

    RC IX_ScanIterator::init(IXFileHandle &handle, const Attribute &attr, const void *low, const void *high,
                             bool lowInclusive, bool highInclusive) {
        if (handle.fileHandle.file== nullptr) return -1;
        if (handle.getRoot()==-1)return -1;
        this->attr = attr;
        this->low = nullptr;
        this->high = nullptr;

        int strLength = 0;
        if (low != nullptr){
            this->low = new char [PAGE_SIZE];
            memset(this->low, 0, PAGE_SIZE);
            switch(attr.type){
                case TypeInt:
                case TypeReal:
                    memcpy(this->low, low, sizeof(int));
                    break;
                case TypeVarChar:
                    memcpy(&strLength, low, sizeof(int));
                    memcpy(this->low, low, sizeof(int) + strLength);
                    break;
            }
        }
        if (high != nullptr){
            this->high = new char [PAGE_SIZE];
            memset(this->high, 0, PAGE_SIZE);
            switch(attr.type){
                case TypeInt:
                case TypeReal:
                    memcpy(this->high, high, sizeof(int));
                    break;
                case TypeVarChar:
                    memcpy(&strLength, high, sizeof(int));
                    memcpy(this->high, high, sizeof(int) + strLength);
                    break;
            }
        }

        this->lowInclusive = lowInclusive;
        this->highInclusive = highInclusive;
        this->pageNum = handle.getRoot();
        this->slotNum = 0;
        this->ridNum = 0;
        this->fileHandle = &handle;
        this->page = new char [PAGE_SIZE];

        moveToLeft();
        return 0;
    }

    void IX_ScanIterator::moveToLeft() {
        if (pageNum == -1) return;

        fileHandle->readPage(pageNum, page);
        if(Node::isNode(page) && fileHandle->getNumberOfPages()!=2){
            if (nullptr == this->low){
                memcpy(&pageNum, page, sizeof(int));
            } else {
                pageNum = Node::searchPage(page, attr, low);
            }
            moveToLeft();
        } else {
            int* info = new int [TREE_NODE_SIZE];
            Leaf::getInfo(info, page);
            curCount = info[SLOT_NUM];
            auto slot = Tool::getSlot(page, 0);
            curRIDNum = slot.rid_num;
            // when lowKey is nullptr, the slot number should be 0, which default value is 0 already
            if (low == nullptr){
                //skip if the current slot is empty
                while(ridNum >= curRIDNum && pageNum!=-1) {
                    slotNum++;
                    while (slotNum >= curCount){
                        Leaf::getInfo(info, page);
                        pageNum = info[NEXT];
                        if (pageNum == -1)break;
                        fileHandle->readPage(pageNum, page);
                        slotNum = 0;
                        Leaf::getInfo(info, page);
                        curCount = info[SLOT_NUM];
                    }
                    ridNum = 0;
                    slot = Tool::getSlot(page, slotNum);
                    curRIDNum = slot.rid_num;
                }
                delete [] info;
                return;
            }
            char* oldKey = new char [PAGE_SIZE];
            float diff = 0;
            int id = 0, offset = 0, len = 0;
            Tool::search(page, attr, low, offset, id, len);
            Tool::getKey(page, offset, len, oldKey);
            diff = Tool::compare(low, oldKey, attr);
            slotNum = id;
            slot = Tool::getSlot(page, slotNum);
            curRIDNum = slot.rid_num;
            //when the lowkey is not inclusive, scan until the value satisfy
            while (!lowInclusive && diff == 0){
                moveToNext();
                slot = Tool::getSlot(page, slotNum);
                Tool::getKey(page, slot.offset, slot.len, oldKey);
                diff = Tool::compare(low, oldKey, attr);
            }
            //the key does not exist in this leaf page
            if (slotNum == curCount){
                moveToNext();
            }
            delete [] oldKey;
            delete [] info;
        }
    }

    void IX_ScanIterator::moveToNext() {
        auto info = new int [TREE_NODE_SIZE];
        ridNum++;
        Leaf::getInfo(info, page);
        while(ridNum >= curRIDNum && pageNum!=-1) {
            slotNum++;
            while (slotNum >= curCount){
                Leaf::getInfo(info, page);
                pageNum = info[NEXT];
                if (pageNum == -1)break;
                fileHandle->readPage(pageNum, page);
                slotNum = 0;
                Leaf::getInfo(info, page);
                curCount = info[SLOT_NUM];
            }
            ridNum = 0;
            auto slot = Tool::getSlot(page, slotNum);
            curRIDNum = slot.rid_num;
        }
        delete [] info;
    }

    bool IX_ScanIterator::inRange(char* key) {
        if(high==nullptr)return true;

        float diff = Tool::compare(key, high, attr);

        return highInclusive ? diff<=0:diff<0;
    }


    void Node::getInfo(int *info, char *data) {
        memset(info, 0, sizeof(int)*TREE_NODE_SIZE);
        auto base = data+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<TREE_NODE_SIZE;i++){
            memcpy(&info[i], base-offset, sizeof(int));
            offset += sizeof(int);
        }
    }

    void Node::insertEntry(IXFileHandle &ixFileHandle, int pageNum, Attribute &attr, keyEntry& entry, RID& rid,
                      keyEntry *child) {
        char* data = new char [PAGE_SIZE];
        ixFileHandle.readPage(pageNum, data);
        if(Node::isNode(data) && ixFileHandle.getNumberOfPages()!=2){
            auto num = Node::searchPage(data, attr, entry.key);
            insertEntry(ixFileHandle, num, attr, entry, rid, child);
            if(child->left!=-1){
                if(Node::haveSpace(data, entry.key, attr)){
                    Node::insertKey(data, *child, attr);
                    child->left = -1;
                    delete [] child->key;
                }
                else {
                    char* newPage = new char [PAGE_SIZE];
                    int* info = new int [TREE_NODE_SIZE];

                    // Node will send the middleKey to upper layer
                    Node::getInfo(info, data);
                    Node::createNode(newPage, NODE, info[PARENT]);
                    char* middleKey = new char [PAGE_SIZE];
                    memset(middleKey, 0, PAGE_SIZE);
                    Node::split(data, newPage, middleKey);
                    getInfo(info, data);

                    int newPageNum = ixFileHandle.getNumberOfPages();
                    info[NEXT] = newPageNum;
                    Tool::writeInfo(data, info);
                    ixFileHandle.appendPage(newPage);

                    if(Tool::compare(child->key, middleKey, attr)<0){
                        Node::insertKey(data, *child, attr);
                    } else {
                        Node::insertKey(newPage, *child, attr);
                    }

                    int* newInfo = new int [TREE_NODE_SIZE];
                    getInfo(newInfo, newPage);
                    getInfo(info, data);

                    if(info[NODE_TYPE] == ROOT){
                        int root_num = newPageNum+1;

                        char* root = new char [PAGE_SIZE];
                        Node::createNode(root, ROOT, NULL_NODE);
                        keyEntry entry1;
                        entry1.left = pageNum;
                        entry1.right = newPageNum;
                        int keyLen = 0;
                        memcpy(&keyLen, middleKey, sizeof(int));
                        keyLen+=sizeof(int);
                        entry1.key = new char [keyLen];
                        memset(entry1.key, 0, keyLen);
                        memcpy(entry1.key, middleKey, keyLen);
                        Node::appendKey(root, entry1, attr);

                        ixFileHandle.appendPage(root);
                        ixFileHandle.setRoot(root_num);
                        info[NODE_TYPE] = NODE;
                        info[PARENT] = root_num;
                        Tool::writeInfo(data, info);

                        newInfo[PARENT] = root_num;
                        Tool::writeInfo(newPage, newInfo);
                        ixFileHandle.writePage(newPageNum, newPage);
                        delete [] entry1.key;
                        delete [] root;
                    }
                    delete [] child->key;
                    child->left = pageNum;
                    child->key = middleKey;
                    child->right = newPageNum;

                    delete [] newPage;
                    delete [] info;
                    delete [] newInfo;
                }
            }
        }
        else {
            if(Leaf::haveSpace(data, attr, entry.key)){
                Leaf::insertEntry(data, attr, entry, rid);
                child->left = -1;
            }
            else {
                char* newPage = new char [PAGE_SIZE];
                int* info = new int [TREE_NODE_SIZE];

                Leaf::getInfo(info, data);
                Leaf::createLeaf(newPage, info[PARENT], pageNum, info[NEXT]);
                Leaf::split(data, newPage);
                Leaf::getInfo(info, data);
                int newPageNum = ixFileHandle.getNumberOfPages();

                auto slot = Tool::getSlot(newPage, 0);
                char* newKey = new char [slot.len];
                memset(newKey, 0, slot.len);
                memcpy(newKey, newPage+slot.offset, slot.len);

                if(Tool::compare(entry.key, newKey, attr)<0){
                    Leaf::insertEntry(data, attr, entry, rid);
                } else {
                    Leaf::insertEntry(newPage, attr, entry, rid);
                }

                int* newInfo = new int [TREE_NODE_SIZE];
                getInfo(newInfo, newPage);
                getInfo(info, data);

                if(info[NODE_TYPE] == ROOT){
                    char* root = new char [PAGE_SIZE];
                    Node::createNode(root, ROOT, NULL_NODE);
                    keyEntry entry1;
                    entry1.left = pageNum;
                    entry1.right = newPageNum+1;
                    auto slot = Tool::getSlot(newPage, 0);
                    entry1.key = new char [slot.len];
                    memset(entry1.key, 0, slot.len);
                    memcpy(entry1.key, newKey, slot.len);
                    Node::appendKey(root, entry1, attr);

                    ixFileHandle.appendPage(root);
                    ixFileHandle.setRoot(newPageNum);
                    // Change Parent pointer
                    info[NODE_TYPE] = LEAF;
                    info[PARENT] = newPageNum;
                    Tool::writeInfo(data, info);

                    newInfo[PARENT] = newPageNum;
                    Tool::writeInfo(newPage, newInfo);

                    newPageNum++;
                    delete [] entry1.key;
                    delete [] root;
                }
                getInfo(info, data);

                child->left = pageNum;
                child->key = newKey;
                child->right = newPageNum;

                info[NEXT] = newPageNum;
                Tool::writeInfo(data, info);
                ixFileHandle.appendPage(newPage);

                delete [] newPage;
                delete [] newInfo;
                delete [] info;
            }
        }
        ixFileHandle.writePage(pageNum, data);
        delete [] data;
    }

    RC Node::deleteEntry(IXFileHandle &ixFileHandle, int paPageNum, int pageNum, const Attribute &attr, keyEntry &entry,
                           RID& rid, keyEntry *child) {
        char* data = new char [PAGE_SIZE];
        ixFileHandle.readPage(pageNum, data);
        if(Node::isNode(data) && ixFileHandle.getNumberOfPages() !=2){
            auto num = Node::searchPage(data, attr, entry.key);
            return deleteEntry(ixFileHandle, pageNum, num, attr, entry, rid,  child);
        }
        else {
            auto rc = Leaf::deleteEntry(data, attr, entry, rid);
            if(rc!=0){
                std::cout << " " << pageNum << std::endl;
                delete [] data;
                return rc;
            }
            ixFileHandle.writePage(pageNum, data);
        }
        delete [] data;
        return 0;
    }

    bool Node::isNode(char *pageData) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, pageData);
        auto type = info[NODE_TYPE];
        delete [] info;
        return type != LEAF;
    }

    void Node::print(IXFileHandle &ixFileHandle, const Attribute &attribute, int root, int depth, std::ostream &out) {
        char* data = new char [PAGE_SIZE];
        ixFileHandle.readPage(root, data);
        if(!isNode(data) || ixFileHandle.getNumberOfPages()==2){
            Leaf::print(data, attribute, out);
            delete [] data;
            return;
        }
        out.clear();
        out<<"{\"keys\": [";

        std::queue<int> children;
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        int child = 0;
        int count = info[SLOT_NUM];
        char* key = new char [PAGE_SIZE];
        int intKey = 0, strLength = 0;
        float floatKey = 0;
        char* stringKey = new char [PAGE_SIZE];
        //keys

        for (int i = 0; i < count; i++){
            auto slot = Tool::getSlot(data, i);
            memset(key, 0, PAGE_SIZE);
            memcpy(key, data + slot.offset, slot.len);
            memcpy(&child, data + slot.offset - sizeof(int), sizeof(int));
            children.push(child);

            out<< "\"";
            switch(attribute.type){
                case TypeVarChar:
                    memcpy(&strLength, key, sizeof(int));
                    memcpy(stringKey, key + sizeof(int), strLength);
                    stringKey[strLength] = '\0';
                    out<<( reinterpret_cast< char const* >(stringKey));
                    break;
                case TypeInt:
                    memcpy(&intKey, key, sizeof(int));
                    out<<intKey;
                    break;
                case TypeReal:
                    memcpy(&floatKey, key, sizeof(int));
                    out<<floatKey;
                    break;
            }
            out<<"\"";
            if (i != count - 1) {out<<",";}
        }
        memcpy(&child, data + info[DATA_OFFSET]-sizeof(int), sizeof(int));
        if(child!=-1)children.push(child);

        delete [] data;
        delete [] info;
        delete [] key;
        delete [] stringKey;
        out<<"],"<<std::endl;

        //children
        out << std::string(depth * 2, ' ');
        out<<"\"children\": ["<<std::endl;
        while(!children.empty()){
            out << std::string(depth * 2, ' ');
            out<<"    ";
            print(ixFileHandle, attribute, children.front(), depth + 4, out);
            children.pop();
            if (!children.empty()){ out<<",";}
            out<<std::endl;
        }
        out << std::string(depth * 2, ' ');
        out<<"]}" << std::endl;
    }

    void Node::createNode(char *data, int type, int parent) {
        memset(data, 0, PAGE_SIZE);
        int* info = new int [TREE_NODE_SIZE];
        memset(info, 0, sizeof(int)*TREE_NODE_SIZE);
        info[INFO_OFFSET] = sizeof(int)*TREE_NODE_SIZE;
        info[PARENT] = parent;
        info[NODE_TYPE] = type;
        Tool::writeInfo(data, info);
        delete [] info;
    }

    bool Node::haveSpace(char *data, const char *key, Attribute& attr) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        auto empty = PAGE_SIZE - info[DATA_OFFSET] - info[INFO_OFFSET];
        int space = 0;
        switch (attr.type) {
            case TypeInt:
            case TypeReal:
                space+=sizeof(int);
                break;
            case TypeVarChar:
                int len = 0;
                memcpy(&len, key, sizeof(int));
                space+=len+sizeof(int);
                break;
        }
        space += Slot_Size + sizeof(int);   // + the size of an index
        delete [] info;
        return empty>=space;
    }

    void Node::appendKey(char* data, keyEntry &entry, Attribute &attr) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);

        auto pos = data + info[DATA_OFFSET];
        int len = 4;
        if(attr.type==TypeVarChar){
            len = 0;
            memcpy(&len, entry.key, sizeof(int));
            len+=sizeof(int);
        }
        if(info[DATA_OFFSET]==0){
            memcpy(pos, &entry.left, sizeof(int));
            pos += sizeof(int);
            info[DATA_OFFSET]+=sizeof(int);
        } else {
            memcpy(pos-sizeof(int), &entry.left, sizeof(int));
        }
        memcpy(pos, entry.key, len);
        memcpy(pos+len, &entry.right, sizeof(int));
        Tool::appendSlot(data, info, len);

        info[DATA_OFFSET] += len+sizeof(int);
        Tool::writeInfo(data, info);
        delete [] info;
    }

    void Node::split(char *data, char *newData, char *middle) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        auto count = info[SLOT_NUM];
        int d = count/2;
        auto slot = Tool::getSlot(data, d);

        int len = slot.offset+slot.len;
        auto dataLen = info[DATA_OFFSET]-len;
        auto infoLen = Slot_Size*(count-d-1);

        auto data_pivot = data+len;
        auto info_pivot = data+PAGE_SIZE-info[INFO_OFFSET];

        memcpy(middle, data+slot.offset, slot.len);

        // Transfer data
        memcpy(newData, data_pivot, dataLen);
        memcpy(newData+PAGE_SIZE-sizeof(int)*TREE_NODE_SIZE-infoLen, info_pivot, infoLen);
        // Update info
        memset(data+slot.offset, 0, info[DATA_OFFSET] - slot.offset);
        memset(info_pivot, 0, infoLen+Slot_Size);
        Tool::updateInfo(info, d, slot.offset, sizeof(int)*TREE_NODE_SIZE + Slot_Size*d);
        Tool::writeInfo(data, info);

        int* newInfo = new int [TREE_NODE_SIZE];
        getInfo(newInfo, newData);
        Tool::updateInfo(newInfo, count-d-1, dataLen, infoLen+sizeof(int)*TREE_NODE_SIZE);
        Tool::writeInfo(newData, newInfo);
        Tool::updateSlot(newData, newInfo, -len, 0);

        delete [] info;
        delete [] newInfo;
    }

    int Node::searchPage(char *page, Attribute attribute, char *key) {
        int pos = 0, len = 0, i = 0;
        Tool::search(page, attribute, key, pos, i, len);
        float diff = Tool::compare(key, page+pos, attribute);
        if(diff==0){
            pos+=len+sizeof(int);
        }
        int num = 0;
        memcpy(&num, page+pos-sizeof(int), sizeof(int));
        return num;
    }

    void Node::insertKey(char *data, keyEntry entry, Attribute &attr) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        int pos = 0, len = 0, i = 0;
        Tool::search(data, attr, entry.key, pos, i, len);
        if(i==-1)i=info[SLOT_NUM];
        int attrLen = 4;
        if(attr.type==TypeVarChar){
            memcpy(&attrLen, entry.key, sizeof(int));
            attrLen+=sizeof(int);
        }
        int dis = attrLen+sizeof(int);
        if(entry.right==0){
            std::cout<<std::endl;
        }

        memmove(data+pos+dis, data+pos, info[DATA_OFFSET]-pos);
        memcpy(data+pos-sizeof(int), &entry.left, sizeof(int));
        memcpy(data+pos, entry.key, attrLen);
        memcpy(data+pos+attrLen, &entry.right, sizeof(int));

        int num = info[SLOT_NUM]-i;
        auto info_pos = data+PAGE_SIZE-info[INFO_OFFSET];
        memmove(info_pos-Slot_Size, info_pos, Slot_Size*num);
        Tool::writeSlot(data, pos, attrLen, 1, i);

        Tool::updateInfo(info, info[SLOT_NUM]+1, info[DATA_OFFSET]+dis, info[INFO_OFFSET]+Slot_Size);
        Tool::writeInfo(data, info);
        Tool::updateSlot(data, info, dis, i + 1);

        delete [] info;
    }

    void Leaf::getInfo(int *info, char *leafData) {
        memset(info, 0, sizeof(int)*TREE_NODE_SIZE);
        auto base = leafData+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<TREE_NODE_SIZE;i++){
            memcpy(&info[i], base-offset, sizeof(int));
            offset += sizeof(int);
        }
    }

    void Leaf::createLeaf(char *page, int parent, int pre, int next) {
        memset(page, 0, PAGE_SIZE);
        int* info = new int [TREE_NODE_SIZE];
        memset(info, 0, sizeof(int)*TREE_NODE_SIZE);
        info[INFO_OFFSET] = sizeof(int)*TREE_NODE_SIZE;
        info[PARENT] = parent;
        info[PRE] = pre;
        info[NODE_TYPE] = LEAF;
        info[NEXT] = next;
        Tool::writeInfo(page, info);
        delete [] info;
    }

    void Leaf::split(char *data, char *newData) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        auto count = info[SLOT_NUM];
        int d = count/2;
        auto slot = Tool::getSlot(data, d-1);

        int len = slot.offset+slot.len+sizeof(RID)*slot.rid_num;
        auto dataLen = info[DATA_OFFSET]- len;
        auto infoLen = Slot_Size*(count-d);

        auto data_pivot = data+len;
        auto info_pivot = data+PAGE_SIZE-info[INFO_OFFSET];
        // Transfer data
        memcpy(newData, data_pivot, dataLen);
        memcpy(newData+PAGE_SIZE-sizeof(int)*TREE_NODE_SIZE-infoLen, info_pivot, infoLen);
        // Update info
        Tool::updateInfo(info, d, len, sizeof(int)*TREE_NODE_SIZE + Slot_Size*d);
        Tool::writeInfo(data, info);
        memset(data_pivot, 0, dataLen);
        memset(info_pivot, 0, infoLen);

        int* newInfo = new int [TREE_NODE_SIZE];
        getInfo(newInfo, newData);
        Tool::updateInfo(newInfo, count-d, dataLen, infoLen+sizeof(int)*TREE_NODE_SIZE);

        Tool::writeInfo(newData, newInfo);
        Tool::updateSlot(newData, newInfo, -len, 0);
        delete [] info;
        delete [] newInfo;
    }

    void Leaf::print(char *data, const Attribute &attr, std::ostream &out) {
        out<<"{\"keys\": [";
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        int count = info[SLOT_NUM];
        int strLength = 0;
        char* key = new char [PAGE_SIZE];
        char* stringKey = new char [PAGE_SIZE];

        int intKey = 0;
        float floatKey = 0;


        for (int i = 0; i < count; i++){
            auto slot = Tool::getSlot(data, i);
            if(slot.rid_num==0)continue;
            if(i!=0)out<<",";
            memset(key, 0, PAGE_SIZE);
            memcpy(key, data + slot.offset, slot.len);


            out<< "\"";
            switch(attr.type){
                case TypeVarChar:
                    memcpy(&strLength, key, sizeof(int));
                    memcpy(stringKey, key + sizeof(int), strLength);
                    stringKey[strLength] = '\0';
                    out<<( reinterpret_cast< char const* >(stringKey));
                    break;
                case TypeInt:
                    memcpy(&intKey, key, sizeof(int));
                    out<<intKey;
                    break;
                case TypeReal:
                    memcpy(&floatKey, key, sizeof(int));
                    out<<floatKey;
                    break;
            }
            RID rid;
            out<<":[";
            for (int j = 0; j < slot.rid_num; ++j) {
                memcpy(&rid, data+slot.offset+slot.len+Slot_Size*j, sizeof(RID));
                out<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
                if(j!=slot.rid_num-1)out<<",";
            }
            out<<"]\"";
        }
        delete [] key;
        delete [] stringKey;
        delete [] info;
        out<<"]}";
    }

    bool Leaf::haveSpace(char *data, const Attribute &attr, const char *key) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, data);
        auto empty = PAGE_SIZE - info[DATA_OFFSET] - info[INFO_OFFSET];
        int space = 0;
        switch (attr.type) {
            case TypeInt:
            case TypeReal:
                space+=sizeof(int);
                break;
            case TypeVarChar:
                int len = 0;
                memcpy(&len, key, sizeof(int));
                space+=len+sizeof(int);
                break;
        }
        space += Slot_Size + sizeof(RID);   // + the size of an index
        delete [] info;
        return empty>=space;
    }

    void Leaf::insertEntry(char *leafData, const Attribute &attr, keyEntry &entry, RID &rid) {
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, leafData);

        int offset = 0;
        int len = sizeof (int);
        int left = 0;
        int slot_len = 0;
        if(attr.type==TypeVarChar){
            memcpy(&len, entry.key, sizeof(int));
            len+=sizeof(int);
        }
        int keyLen;
        Tool::search(leafData, const_cast<Attribute &>(attr), entry.key, offset, left, keyLen);
        slot_len = len;
        // Insert an entry in the middle
        if(offset!=info[DATA_OFFSET]) {
            char* oldKey = new char [keyLen];
            memset(oldKey,0, keyLen);
            memcpy(oldKey, leafData+offset, keyLen);
            float diff = Tool::compare(entry.key, oldKey, const_cast<Attribute &>(attr));
            if(diff==0)Leaf::appendToKey(leafData, info, attr, left, rid);
            else {
                Tool::moveBack(leafData, offset, len + sizeof(RID), info[DATA_OFFSET] - offset);
                int num = info[SLOT_NUM]-left;
                auto info_pos = leafData+PAGE_SIZE-info[INFO_OFFSET];
                memmove(info_pos-Slot_Size, info_pos, Slot_Size*num);
                // Write data and slot
                auto data_pos = leafData+offset;
                memcpy(data_pos, entry.key, len);
                memcpy(data_pos+len, &rid, sizeof(RID));
                Tool::writeSlot(leafData, offset, len, 1, left);
                // Update info and slot
                Tool::updateInfo(info, info[SLOT_NUM]+1, info[DATA_OFFSET]+len+sizeof(RID), info[INFO_OFFSET]+Slot_Size);
                Tool::writeInfo(leafData, info);
                Tool::updateSlot(leafData, info, len + sizeof(RID), left + 1);
            }
            delete [] oldKey;
            delete [] info;
            return;
        }

        auto pos = leafData + offset;
        memcpy(pos, entry.key, len);
        memcpy(pos+len, &rid, sizeof(RID));
        Tool::appendSlot(leafData, info, slot_len);

        info[DATA_OFFSET] += len+sizeof(RID);
        Tool::writeInfo(leafData, info);
        delete [] info;
    }

    bool Leaf::equal(RID &rid, char *pos, int len) {
        RID newRID;
        memcpy(&newRID, pos+len, sizeof(RID));
        if(newRID.pageNum!=rid.pageNum || newRID.slotNum!=rid.slotNum)return false;
        return true;
    }

    RC Leaf::deleteEntry(char *leafData, const Attribute &attr, keyEntry &entry, RID &rid) {
        int pos = 0, len = 0, i = 0;
        Tool::search(leafData, const_cast<Attribute &>(attr), entry.key, pos, i, len);
        float diff = Tool::compare(entry.key, leafData+pos, const_cast<Attribute &>(attr));
        if(diff!=0){
            std::cout<< "Key not found in this page ";
            return -1;
        }
        auto slot = Tool::getSlot(leafData, i);
        auto off = pos+len-sizeof(RID);
        RID r;
        bool found = false;
        for(int j = 0;j<slot.rid_num&&!found;j++){
            off+=sizeof (RID);
            found = equal(rid, leafData+off, 0);
        }
        if(!found){
            std::cout<< "RID not found" << std::endl;
            return -1;
        }
        int* info = new int [TREE_NODE_SIZE];
        getInfo(info, leafData);

        Tool::shiftEntry(leafData, i, off, sizeof(RID), info);
        Tool::writeSlot(leafData, slot.offset, slot.len, slot.rid_num-1, i);
        delete [] info;
        return 0;
    }

    void Leaf::appendToKey(char *data, int* info, const Attribute &attr, int i, RID &rid) {
        auto slot = Tool::getSlot(data, i);
        auto pos = slot.offset+slot.len+sizeof(RID)*slot.rid_num;
        Tool::moveBack(data, pos, sizeof(RID), info[DATA_OFFSET]-pos);
        memcpy(data+pos, &rid, sizeof(RID));
        info[DATA_OFFSET] += sizeof(RID);
        Tool::writeInfo(data, info);
        Tool::writeSlot(data, slot.offset, slot.len, slot.rid_num+1, i);
        Tool::updateSlot(data, info, sizeof(RID), i+1);
    }

    void Tool::moveBack(char* data, int offset, int distance, int length){
        memmove(data+offset+distance, data+offset, length);
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
                if(fabs(floatVal1-floatVal2)<FLOAT_DIFF)return 0;
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

                memcpy(val1, key1+sizeof(int), len1);
                memcpy(val2, key2+sizeof(int), len2);

                auto res = strcmp(val1, val2);
                delete [] val1;
                delete [] val2;
                return res;
            }
        }
        return 0;
    }

    void Tool::appendSlot(char *data, int *info, short len) {
        Slot slot = {static_cast<short>(info[DATA_OFFSET]), len, 1};
        memcpy(data+PAGE_SIZE-info[INFO_OFFSET]-Slot_Size, &slot, Slot_Size);
        info[INFO_OFFSET] += Slot_Size;
        info[SLOT_NUM]++;
    }

    void Tool::updateInfo(int *info, int slot_num, int data_offset, int info_offset) {
        info[SLOT_NUM] = slot_num;
        info[DATA_OFFSET] = data_offset;
        info[INFO_OFFSET] = info_offset;
    }

    // binary search to find the left border of the key
    void Tool::search(char *data, Attribute &attr, char *key, int& pos, int& left, int& len) {
        int* info = new int [TREE_NODE_SIZE];
        Node::getInfo(info, data);

        int l = 0, r = info[SLOT_NUM]-1, mid;
        char* middle = new char [PAGE_SIZE];
        while(l<=r){
            mid = l+(r-l)/2;
            auto slot = getSlot(data, mid);
            getKey(data, slot.offset, slot.len, middle);
            auto diff = compare(middle, key, attr);
            if(diff<0)l = mid+1;
            else r = mid-1;
        }
        if(l==info[SLOT_NUM]){
            pos = info[DATA_OFFSET];
            left = -1;
            delete [] info;
            delete [] middle;
            return;
        }
        auto slot = getSlot(data, l);
        pos = slot.offset;
        left = l;
        len = slot.len;
        delete [] middle;
        delete [] info;
    }

    Slot Tool::getSlot(char *page, int i){
        auto base = page+PAGE_SIZE-sizeof(int)*TREE_NODE_SIZE;
        auto addr = base - Slot_Size*(i+1);

        Slot slot;
        memcpy(&slot, addr, Slot_Size);

        return slot;
    }

    void Tool::getKey(char *data, unsigned int pos, unsigned int len, char *key) {
        memset(key, 0, PAGE_SIZE);
        memcpy(key, data+pos, len);
    }

    void Tool::writeSlot(char *data, short offset, short len, short rid_num, int i) {
        Slot slot = {offset, len, rid_num};
        auto addr = data+PAGE_SIZE-sizeof(int)*TREE_NODE_SIZE-Slot_Size*(i+1);
        memset(addr, 0, Slot_Size);
        memcpy(addr, &slot, Slot_Size);
    }

    void Tool::updateSlot(char *data, int *info, int dis, int i) {
        for(;i<info[SLOT_NUM];i++){
            auto slot = getSlot(data, i);
            slot.offset += dis;
            writeSlot(data, slot.offset, slot.len, slot.rid_num, i);
        }
    }
    // To compact the page. Used for deletion
    void Tool::shiftEntry(char *data, int i, int pos, int len, int *info) {
        memset(data+pos, 0, len);
        memmove(data+pos, data+pos+len, info[DATA_OFFSET]-pos-len);

        updateSlot(data, info, -len,  i+1);
        info[DATA_OFFSET] -= len;
        memset(data+info[DATA_OFFSET], 0, len);
        writeInfo(data, info);
    }

    void Tool::writeInfo(char* data, int* info){
        auto base = data+PAGE_SIZE;
        int offset = sizeof(int);
        for(int i=0;i<TREE_NODE_SIZE;i++){
            memcpy(base-offset, &info[i], sizeof(int));
            offset += sizeof(int);
        }
    }

    keyEntry::keyEntry(int left, char *key, int right) {
        this->left = left;
        this->key = key;
        this->right = right;
    }
} // namespace PeterDB