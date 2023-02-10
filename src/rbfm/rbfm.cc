#include <cmath>
#include <iostream>
#include <cstring>
#include <climits>
#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle& fileHandle) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager& pfm = PagedFileManager::instance();
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::appendNewPage(FileHandle &fileHandle) {
        char* page = new char [PAGE_SIZE];
        memset(page,0,PAGE_SIZE);
        const unsigned info[3] = {sizeof (unsigned)*PAGE_INFO_NUM, 0, 0};
        memcpy((void *) (page + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);

        fileHandle.appendPage(page);

        delete[] page;
        return 0;
    }

    /*
     * - The first part of data will contain the flag information
     * - When the flag is set to 1, it means the corresponding field contains null
     * - The field with null value will not use any space in the data.
     *
     *  Tombstone: use a pointer and compact the space
     */
    // Extrem case: the old record is smalller than the size of a tombstone
    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if(fileHandle.getNumberOfPages() == 0) {
            appendNewPage(fileHandle);
        }

        Record record(recordDescriptor, data, rid);
        /*
         * find the next available page and perform the follwing operation
         *  - insert the record into a page
         *  - update the slot information at the back of this page
         */
        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        unsigned lastPageNum = fileHandle.getNumberOfPages()-1;
        unsigned pageNum = getNextAvailablePageNum(record.size + SLOT_SIZE + sizeof(RID), fileHandle, lastPageNum);
        fileHandle.readPage(pageNum, (void *) pageData);
        unsigned slotID = getSlotNum(pageData);
        rid.pageNum = pageNum;
        rid.slotNum = slotID;
        writeRecord(record, fileHandle, pageNum, rid, pageData);

        delete [] pageData;
        return 0;
    }

    unsigned RecordBasedFileManager::getSlotNum(char* pageData){
        auto* info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(pageData, info);
        auto slotID = getDeletedSlot(pageData);
        if(slotID<0){
            slotID = info[SLOT_NUM];
            info[SLOT_NUM]++;
            info[INFO_OFFSET] += SLOT_SIZE;
        }
        memcpy((void*)(pageData + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);
        delete [] info;
        return slotID;
    }

    void RecordBasedFileManager::writeRecord(const Record& record, FileHandle &handle, unsigned num, RID &rid, char* data) {
        auto* info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(data, info);
        // Write the record to page
        memcpy(data+info[DATA_OFFSET], record.data, record.size);
        memcpy(data+info[DATA_OFFSET]+ record.size, &rid, sizeof(RID));
        // Write a new slot information
        std::pair<unsigned ,unsigned> newSlot;
        newSlot = {info[DATA_OFFSET], record.size+sizeof(RID)};

        writeSlotInfo(rid.slotNum, data, newSlot);
        // Update information
        info[DATA_OFFSET] += record.size+sizeof(RID);
        // Write back
        memcpy((void*)(data + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);
        // Write to disk
        handle.writePage(num, data);

        delete [] info;
    }

    void RecordBasedFileManager::getInfo(char* data, unsigned *info){
        memcpy(info, data + PAGE_SIZE - sizeof(unsigned)*PAGE_INFO_NUM, sizeof(unsigned)*PAGE_INFO_NUM);
    }

    int RecordBasedFileManager::getDeletedSlot(char* data){
        auto* info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(data, info);
        auto slotNum = (info[INFO_OFFSET]-sizeof(short)*PAGE_INFO_NUM)/SLOT_SIZE;
        for(unsigned i=0;i<slotNum;i++){
            auto slot = getSlotInfo(i, data);
            if(slot.first==5000) {
                delete [] info;
                return i;
            }
        }
        delete [] info;
        return -1;
    }

    // Should also return the flag information and stick to the format as before
    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        if(fileHandle.readPage(rid.pageNum, pageData)==0){
            auto slot = getSlotInfo(rid.slotNum, pageData);
            if(slot.first==5000){
                delete [] pageData;
                return -1;
            }
            auto data_offset = pageData+slot.first;
            if(isTomb(data_offset)){
                auto rc= readRecord(fileHandle, recordDescriptor, getPointRID(data_offset), data);
                delete [] pageData;
                return rc;
            }
            fetchRecord(slot.first, slot.second, data, pageData);
            delete [] pageData;
            return 0;
        }
        delete [] pageData;
        return -1;
    }

    /*
     * Get the slot from disk
     * Slot:
     *  first: offset from the beginning
     *  second: The record size
     * Deleted:
     *  first = 5000
     *  second = 0
    */
    std::pair<unsigned, unsigned> RecordBasedFileManager::getSlotInfo(unsigned slotNum, const char* data) {
        void* slotPtr = (void *) (data + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM - (slotNum + 1) * SLOT_SIZE);
        return reinterpret_cast<std::pair<unsigned ,unsigned>*>(slotPtr)[0];
    }

    void RecordBasedFileManager::writeSlotInfo(unsigned slotNum, const char* data, std::pair<unsigned, unsigned> slot){
        auto info_offset = sizeof(unsigned)*PAGE_INFO_NUM + slotNum*SLOT_SIZE;
        memcpy((void *) (data + PAGE_SIZE - info_offset - SLOT_SIZE), &slot, SLOT_SIZE);
    }

    void RecordBasedFileManager::fetchRecord(int offset, int recordSize, void *data, void *page) {
        char* recordPtr = (char*)page + offset;
        short int fieldNum = *(short int*)recordPtr;
        char* flagPtr = recordPtr + FIELD_NUM_SIZE;
        short int flag_size = std::ceil( static_cast<double>(fieldNum) /CHAR_BIT);
        char* dataPtr = flagPtr + flag_size + INDEX_SIZE*fieldNum;

        memcpy(data, flagPtr, flag_size);
        memcpy((char*)data+flag_size, dataPtr, recordSize-sizeof(short)-flag_size-INDEX_SIZE*fieldNum-sizeof(RID));
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        RID fakeRID = {0, 0};
        Record record(recordDescriptor, data, fakeRID);
        short int offset = sizeof(short int) + INDEX_SIZE * record.fieldNum;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            out << recordDescriptor[i].name << ": ";
            if (record.isNull(i)) {
                out << "NULL ";
            } else {
                short int diff = record.index[i] - offset;
                const char* pos = (char*)data;
                pos += diff;
                switch (recordDescriptor[i].type) {
                    case TypeInt: {
                        int val;
                        memcpy(&val, pos, 4);
                        out << val;
                        break;
                    }
                    case TypeReal: {
                        float val;
                        memcpy(&val, pos, sizeof(float));
                        out << val;
                        break;
                    }
                    case TypeVarChar: {
                        int varSize;
                        memcpy(&varSize, pos, 4);
                        char* str = new char[varSize+1];
                        memset(str, 0, varSize+1);
                        memcpy(str, pos + 4, varSize);
                        str[varSize] = '\0';
                        out << str;
                        delete[] str;
                        break;
                    }
                }
            }
            if(i != recordDescriptor.size() -1) {
                out << ", ";
            } else {
                out << std::endl;
            }
        }
        return 0;
    }

    /*
     *  PART 2
     */
    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageData);
        auto slot = getSlotInfo(rid.slotNum, pageData);
        auto* info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(pageData, info);
        auto data_offset = pageData+slot.first;

        if(isTomb(data_offset)){
            // Tomb Handler:
            //  Recursively delete the pointer chain
            deleteRecord(fileHandle, recordDescriptor, getPointRID(data_offset));
        }
        delete [] pageData;
        pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageData);
        // shift record data to reuse empty space
        shiftRecord(pageData, slot.first, 0, slot.second, info[DATA_OFFSET]-slot.first-slot.second);
        info[DATA_OFFSET] -= slot.second;
        slot.first = DELETE_MARK;
        auto slotPos = pageData + PAGE_SIZE - sizeof(unsigned )*PAGE_INFO_NUM-(rid.slotNum+1)*SLOT_SIZE;
        memcpy(slotPos, &slot, SLOT_SIZE);
        updateInfo(fileHandle, pageData, rid.pageNum, info);
        fileHandle.writePage(rid.pageNum, pageData);
        delete [] info;
        delete [] pageData;
        return 0;
    }

    /*
     * 1. Migrate: tombstone -  Pointing to the new location
     * 2. Compact: reuse space
     */
    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        RID cpy = {rid.pageNum, rid.slotNum};
        Record record(recordDescriptor, data, cpy);

        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageData);
        auto slot = getSlotInfo(rid.slotNum, pageData);
        auto* info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(pageData, info);

        auto data_offset = pageData+slot.first;
        if(isTomb(data_offset)){
            auto rc = updateRecord(fileHandle, recordDescriptor, data, getPointRID(data_offset));
            delete [] pageData;
            delete [] info;
            return rc;
        }
        char* oldData = new char [slot.second];
        memset(oldData, 0, slot.second);
        fetchRecord(slot.first, slot.second, oldData, pageData);
        Record oldRecord = Record(recordDescriptor, oldData, cpy);
        delete [] oldData;

        if(record.size<=oldRecord.size){
            memcpy(data_offset, record.data, record.size);
            shiftRecord(pageData, slot.first, record.size+sizeof(RID), slot.second, info[DATA_OFFSET]-slot.first-slot.second);
            writeUpdateInfo(fileHandle, info, slot, record.size, oldRecord.size, rid, pageData);
            delete [] pageData;
            delete [] info;
            return 0;
        }
        // Record is the last record, and there is enough space in the middle
        if(slot.first+slot.second==info[DATA_OFFSET] && getFreeSpace(pageData)>=(record.size-oldRecord.size)){
            memcpy(data_offset, record.data, record.size);
            writeUpdateInfo(fileHandle, info, slot, record.size, oldRecord.size, rid, pageData);
            delete [] pageData;
            delete [] info;
            return 0;
        }
        // New available space for this record, need to migrate this record to a new page
        // insertRecord(fileHandle, recordDescriptor, data, cpy);
        unsigned lastPageNum = fileHandle.getNumberOfPages()-1;
        cpy.pageNum = getNextAvailablePageNum(record.size + SLOT_SIZE + sizeof(RID), fileHandle, lastPageNum);
        memset(pageData, 0, PAGE_SIZE);
        fileHandle.readPage(cpy.pageNum, pageData);
        cpy.slotNum = getSlotNum(pageData);
        writeRecord(record, fileHandle, cpy.pageNum, cpy, pageData);
        fileHandle.readPage(rid.pageNum, pageData);
        getInfo(pageData, info);
        insertTomb(data_offset, cpy.pageNum, cpy.slotNum);
        shiftRecord(pageData, slot.first, TOMB_SIZE, slot.second, info[DATA_OFFSET]-slot.first-slot.second);
        writeUpdateInfo(fileHandle, info, slot, TOMB_SIZE, oldRecord.size+sizeof(RID), rid, pageData);

        delete [] pageData;
        delete [] info;
        return 0;
    }

    void RecordBasedFileManager::writeUpdateInfo(FileHandle &fileHandle, unsigned* info, std::pair<unsigned, unsigned> slot,
                                                 int size, int oldSize, const RID &rid, char* pageData){
        slot.second -= oldSize-size;
        writeSlotInfo(rid.slotNum, pageData, slot);
        info[DATA_OFFSET] += size-oldSize;
        updateInfo(fileHandle, pageData, rid.pageNum, info);
        fileHandle.writePage(rid.pageNum, pageData);
    }

    void RecordBasedFileManager::shiftRecord(char* data, unsigned offset, long size, unsigned shiftOffset, unsigned len){
        auto data_offset = data+offset;
        memmove(data_offset+size, data_offset+shiftOffset, len);
        auto info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(data, info);
        for(unsigned i=0;i<info[SLOT_NUM];i++){
            auto slot = getSlotInfo(i, data);
            if(slot.first==5000)continue;
            if(slot.first>offset){
                slot.first -= shiftOffset-size;
                writeSlotInfo(i, data, slot);
            }
        }
        delete [] info;
    }

    void RecordBasedFileManager::insertTomb(char* data, unsigned pageNum, unsigned slotNum){
        short deleteFlag = DELETE_MARK;
        memcpy(data, &deleteFlag , FIELD_NUM_SIZE);
        memcpy(data+FIELD_NUM_SIZE, &pageNum, sizeof (unsigned));
        memcpy(data+FIELD_NUM_SIZE+sizeof(unsigned), &slotNum, sizeof(unsigned short));
    }

    // Return the pointed record by the tombstone
    RID RecordBasedFileManager::getPointRID(char* data_offset){
        unsigned pageNum;
        unsigned short slotNum;
        data_offset+=sizeof(unsigned short);
        memcpy(&pageNum, data_offset, sizeof(unsigned));
        data_offset+=sizeof(unsigned);
        memcpy(&slotNum, data_offset, sizeof(unsigned short));

        return RID{pageNum, slotNum};
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        auto id = getAttrID(recordDescriptor, attributeName);
        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, pageData);
        auto slot = getSlotInfo(rid.slotNum, pageData);
        char* recordData = new char [slot.second];
        memset(recordData, 0, slot.second);
        memcpy(recordData, pageData+slot.first, slot.second);
        if(isNull(recordData+FIELD_NUM_SIZE, id)){
            char flag = 0x80;
            memcpy(data, &flag, 1);
            delete [] pageData;
            delete [] recordData;
            return 0;
        }
        int offset = getAttrPos(recordDescriptor, recordData, id);
        auto attrPos = recordData+offset;
        switch (recordDescriptor.at(id).type) {
            case TypeInt:
                memcpy((char*)data+1, attrPos, sizeof(int));
                break;
            case TypeReal:
                memcpy((char*)data+1, attrPos, sizeof(float));
                break;
            case TypeVarChar:
                int size = 0;
                memcpy(&size, attrPos, sizeof(int));
                memcpy((char*)data+1, &size, sizeof(int));
                memcpy((char*)data+1+sizeof(int), attrPos+sizeof(int), size);
                break;
        }
        delete [] recordData;
        delete [] pageData;
        return 0;
    }

    short RecordBasedFileManager::getAttrID(const std::vector<Attribute> &recordDescriptor, const std::string &attributeName){
        unsigned short id;
        for(id=0;id<recordDescriptor.size();id++){
            if(!recordDescriptor.at(id).name.compare(attributeName)){
                return id;
            }
        }
        return -1;
    }

    int RecordBasedFileManager::getAttrPos(const std::vector<Attribute> &recordDescriptor, char* recordData, short id){
        short int flag_size = std::ceil( static_cast<double>(recordDescriptor.size()) /CHAR_BIT);
        auto indexPos = recordData+FIELD_NUM_SIZE+flag_size;
        auto attrIndexPos = indexPos+INDEX_SIZE*id;
        int offset;
        memcpy(&offset, attrIndexPos, INDEX_SIZE);
        return offset;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.init(fileHandle, recordDescriptor,conditionAttribute, compOp, value, attributeNames);
        return 0;
    }

    unsigned RecordBasedFileManager::getNextAvailablePageNum(unsigned insertSize, FileHandle &handle, unsigned int startingNum) {
        char* pageData = new char [PAGE_SIZE];
        for (int i = 0; i < handle.getNumberOfPages(); ++i) {
            memset(pageData, 0, PAGE_SIZE);
            handle.readPage((startingNum + i) % handle.getNumberOfPages(), pageData);
            if(insertSize<getFreeSpace(pageData)){
                delete[] pageData;
                return (startingNum+i)%handle.getNumberOfPages();
            }
        }
        appendNewPage(handle);
        delete [] pageData;
        return handle.getNumberOfPages()-1;
    }

    unsigned RecordBasedFileManager::getFreeSpace(char* data) {
        auto* info = new unsigned [PAGE_INFO_NUM];
        memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
        getInfo(data, info);
        unsigned data_offset = info[DATA_OFFSET];
        unsigned info_offset = info[INFO_OFFSET];
        delete [] info;
        return PAGE_SIZE-data_offset-info_offset;
    }

    void RecordBasedFileManager::updateInfo(FileHandle& fileHandle, char* data, unsigned pageNum, unsigned* info){
        memcpy((void*)(data + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);
        fileHandle.writePage(pageNum, data);
    }

    bool RecordBasedFileManager::isTomb(char* data){
        short id;
        memcpy(&id, data, sizeof(short));
        return id==DELETE_MARK;
    }

    Record::Record(const std::vector<Attribute> &recordDescriptor, const void *val, RID &rid) {
        fieldNum = recordDescriptor.size();
        flag = (char*)val;
        buildRecord(recordDescriptor, val);
    }

    Record::~Record() {
        delete [] data;
        delete [] index;
    }

    bool Record::isNull(int fieldNum) {
        int bytes = fieldNum / CHAR_BIT;
        int bits = fieldNum % CHAR_BIT;

        return flag[bytes] >> (7 - bits) & 1;
    }

    bool RecordBasedFileManager::isNull(char* flag, int fieldNum){
        if(fieldNum<0)return true;
        int bytes = fieldNum / CHAR_BIT;
        int bits = fieldNum % CHAR_BIT;

        return flag[bytes] >> (7 - bits) & 1;
    }

    void Record::buildRecord(const std::vector<Attribute> &descriptor, const void* value) {
        short int flag_size = std::ceil( static_cast<double>(descriptor.size()) /CHAR_BIT);
        char* ptr = (char*)value+flag_size;
        short int offset = FIELD_NUM_SIZE+flag_size + INDEX_SIZE * fieldNum;
        short int totalSize = 0;
        index = new short [fieldNum];
        memset(index, 0, sizeof(short)*fieldNum);
        for (int i = 0; i < fieldNum; ++i) {
            index[i] = offset + totalSize;
            if ( descriptor[i].type == TypeInt && !isNull(i) ) {
                totalSize+=sizeof(int);
                ptr+=sizeof(int);
            }
            else if (descriptor[i].type == TypeReal && !isNull(i)) {
                totalSize+=sizeof(float);
                ptr += sizeof(float);
            }
            else if (descriptor[i].type == TypeVarChar && !isNull(i)) {
                int varCharSize;
                memcpy(&varCharSize, ptr, sizeof(int));
                ptr +=  sizeof(int) + varCharSize;
                totalSize += sizeof(int) + varCharSize;
            }
        }

        size = sizeof(short int)+flag_size + INDEX_SIZE*fieldNum + totalSize;
        data = new char [size];
        memset(data, 0, size);
        short int recordOffset = 0;
        // # of fields
        memcpy(data+recordOffset, &fieldNum, FIELD_NUM_SIZE);
        recordOffset += FIELD_NUM_SIZE;

        // Indicator
        memcpy(data + recordOffset, value, flag_size);
        recordOffset += flag_size;

        // indexing
        memcpy(data + recordOffset, index, INDEX_SIZE * fieldNum);
        recordOffset += INDEX_SIZE * fieldNum;

        // content
        memcpy(data + recordOffset, (char*)value+flag_size, totalSize);
    }

    tombStone::tombStone(unsigned int pageNum, unsigned short slotNum) {
        this->pageNum = pageNum;
        this->slotNum = slotNum;
    }

     void RBFM_ScanIterator::setAttrNull(void *src, ushort attrNum, bool isNull) {
        unsigned bytes = 0;
        unsigned pos = 0;
        getByteOffset(attrNum, bytes, pos);
        setBit(*((char *) src + bytes), isNull, pos);
    }

    void RBFM_ScanIterator::setBit(char &src, bool value, unsigned offset) {
        if (value) {
            src |= 1u << offset;
        } else {
            src &= ~(1u << offset);
        }
    }

    void RBFM_ScanIterator::getByteOffset(unsigned pos, unsigned &bytes, unsigned &offset) {
        bytes = pos / 8;

        offset = 7 - pos % 8;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        if(currentPageNum>=fileHandle->getNumberOfPages()){
            return RBFM_EOF;
        }
        bool found = false;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        char* pageData = new char [PAGE_SIZE];
        char* attrValue = new char [attrLength+1];
        while(!found){
            // Initialize data
            memset(pageData, 0, PAGE_SIZE);
            fileHandle->readPage(currentPageNum, pageData);
            unsigned* info = new unsigned [PAGE_INFO_NUM];
            memset(info, 0, sizeof(unsigned)*PAGE_INFO_NUM);
            rbfm.getInfo(pageData, info);
            // Judge if the current record match
            auto slot = rbfm.getSlotInfo(currentSlotNum, pageData);
            if(slot.first==5000 || slot.second<=10){
                if(moveToNext(fileHandle->getNumberOfPages(), info[SLOT_NUM])==-1){
                    delete [] pageData;
                    delete [] attrValue;
                    delete [] info;
                    return RBFM_EOF;
                }
                delete [] info;
                continue;
            }
            char* recordData = pageData + slot.first;
            if(rbfm.isTomb(recordData)){
                if(moveToNext(fileHandle->getNumberOfPages(), info[SLOT_NUM])==-1){
                    delete [] pageData;
                    delete [] attrValue;
                    delete [] info;
                    return RBFM_EOF;
                }
                delete [] info;
                continue;
            }
            memset(attrValue, 0, attrLength+1);
            rid.pageNum = currentPageNum;
            rid.slotNum = currentSlotNum;
            rbfm.readAttribute(*fileHandle, descriptor, rid, conditionAttribute, attrValue);
            if(isMatch(recordData, attrValue+1)){
                char* res = (char*)data;
                found = true;
                char* recordBody = new char[slot.second];
                memset(recordBody, 0, slot.second);
                rbfm.fetchRecord(slot.first, slot.second, recordBody, pageData);
                Record record(descriptor, recordBody, rid);
                // Include a null indicator to the returned data
                short int flag_size = std::ceil( static_cast<double>(attributeNames.size()) /CHAR_BIT);
                memset(res, 0, flag_size);
                res+=flag_size;
                // write to the void* data
                for (int i = 0; i < attributeNames.size(); ++i) {
                    auto id = rbfm.getAttrID(descriptor, attributeNames[i]);
                    if(record.isNull(id)){
                        setAttrNull(data, i, true);
                        continue;
                    }
                    int offset = rbfm.getAttrPos(descriptor, recordData, id);
                    auto attrPos = recordData+offset;
                    int attrSize = 4;
                    if(descriptor[id].type==TypeVarChar){
                        memcpy(&attrSize, attrPos, sizeof(int));
                        memcpy(res, &attrSize, sizeof(int));
                        attrPos += sizeof(int);
                        res += sizeof(int);
                    }
                    memcpy(res, attrPos, attrSize);
                    res += attrSize;
                }
                rid = readRID(recordData, slot.second);
                moveToNext(fileHandle->getNumberOfPages(), info[SLOT_NUM]);
                delete [] recordBody;
                delete [] info;
                break;
            }
            // Move to the nexxt record
            // If moved to the last one, break the loop return -1
            if(moveToNext(fileHandle->getNumberOfPages(), info[SLOT_NUM])==-1) {
                delete [] info;
                break;
            }
            delete [] info;
        }
        delete [] attrValue;
        delete [] pageData;
        if(found){
            return 0;
        }
        return -1;
    }

    RID RBFM_ScanIterator::readRID(char* recordData, int offset){
        RID rid;
        memcpy(&rid, recordData+offset-sizeof(RID), sizeof(RID));

        return rid;
    }


    RC RBFM_ScanIterator::moveToNext(unsigned pageNum, unsigned slotNum){
        currentSlotNum++;
        if(currentSlotNum>=slotNum){
            currentSlotNum = 0;
            currentPageNum++;
            if(currentPageNum>=pageNum){
                return RBFM_EOF;
            }
        }
        return 0;
    }

    RC RBFM_ScanIterator::close() {
        fileHandle->closeFile();
        delete [] conditionVal;
        return 0;
    }

    void RBFM_ScanIterator::init(FileHandle &filehandle, const std::vector<Attribute> &descriptor, const std::string &condition,
                            const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames) {
        this->fileHandle = &filehandle;
        this->descriptor = descriptor;
        this->commOp = compOp;
        this->attributeNames = attributeNames;
        this->conditionAttribute = condition;

        this->currentPageNum = 0;
        this->currentSlotNum = 0;

        for (int i = 0; i < descriptor.size(); ++i) {
            if(!descriptor.at(i).name.compare(condition)){
                this->attributeIndex = i;
                this->attrType = descriptor.at(i).type;
                this->attrLength = descriptor.at(i).length;
                break;
            }
        }
        if (value){
            switch (this->attrType) {
                case TypeInt:
                case TypeReal:
                    this->conditionVal = new char [sizeof(float)];
                    memset(this->conditionVal, 0, sizeof(float));
                    memcpy(this->conditionVal, value, sizeof(float));
                    break;
                case TypeVarChar:
                    unsigned size;
                    memcpy(&size, value, sizeof(unsigned));
                    this->conditionVal = new char [size+sizeof(unsigned)+1];
                    memset(this->conditionVal, 0, size+sizeof(unsigned )+1);
                    memcpy(this->conditionVal, value, size+sizeof(unsigned ));
                    break;
                default:
                    std::cout<< "error: No type match"<< std::endl;
            }
        } else {
            this->conditionVal = nullptr;
        }
    }

    bool RBFM_ScanIterator::isMatch(char* record, char* attrValue) {
        if(commOp == NO_OP)return true;
        if(!conditionVal)return false;

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(rbfm.isTomb(record)){
            return false;
        }

        switch (attrType) {
            case TypeInt:
            {
                int condition = *(int*)conditionVal;
                int val = *(int*)attrValue;
                switch (commOp) {
                    case EQ_OP: return val == condition;
                    case LT_OP: return val < condition;
                    case LE_OP: return val <= condition;
                    case GT_OP: return val > condition;
                    case GE_OP: return val >= condition;
                    case NE_OP: return val != condition;
                    default: return false;
                }
            }
            case TypeReal:
            {
                float condition = *(float *)conditionVal;
                float val = *(float *)attrValue;
                switch (commOp) {
                    case EQ_OP: return (val-condition)<FLOAT_DIFF;
                    case LT_OP: return val < condition;
                    case LE_OP: return val <= condition;
                    case GT_OP: return val > condition;
                    case GE_OP: return val >= condition;
                    case NE_OP: return val != condition;
                    default: return false;
                }
            }
            case TypeVarChar:
            {
                bool res = false;

                switch (commOp) {
                    case EQ_OP: {
                        res = strcmp(attrValue+4, conditionVal+4)==0;
                        break;
                    }
                    case LT_OP: {
                        res = strcmp(attrValue+4, conditionVal+4)<0;
                        break;
                    }
                    case LE_OP: {
                        res = strcmp(attrValue+4, conditionVal+4)<=0;
                        break;
                    }
                    case GT_OP: {
                        res = strcmp(attrValue+4, conditionVal+4)>0;
                        break;
                    }
                    case GE_OP: {
                        res = strcmp(attrValue+4, conditionVal+4)>=0;
                        break;
                    }
                    case NE_OP: {
                        res = strcmp(attrValue+4, conditionVal+4)!=0;
                        break;
                    }
                }
                return res;
            }
            default:
                std::cout<<"Error with the attribute type"<< std::endl;
                return false;
        }
    }
}// namespace PeterDB

