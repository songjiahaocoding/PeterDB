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
     * - rid: Uniquely identify a record in a file. page_num + slot_num 6 bytes.
     *
     *  Reuse slot: insertSlot() to determine if there is deleted slot.
     *
     *  Tombstone: use a pointer and compact the space
     */
    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if(fileHandle.getNumberOfPages() == 0) {
            // Insert a new page
            appendNewPage(fileHandle);
        }

        Record record(recordDescriptor, data, rid);
        /*
         * find the next available page and perform the follwing operation
         *  - insert the record into a page
         *  - update the slot information at the back of this page
         */
        char* pageData = new char [PAGE_SIZE];
        memset((void*)pageData, 0, PAGE_SIZE);

        unsigned lastPageNum = fileHandle.getNumberOfPages()-1;
        unsigned pageNum = getNextAvailablePageNum(record.size + SLOT_SIZE, fileHandle, lastPageNum);
        fileHandle.readPage(pageNum, (void *) pageData);
        writeRecord(record, fileHandle, pageNum, rid, pageData);

        delete [] pageData;

        return 0;
    }

    void RecordBasedFileManager::writeRecord(const Record& record, FileHandle &handle, unsigned num, RID &rid, char* data) {
        auto* info = new unsigned [PAGE_INFO_NUM];
        getInfo(data, info);
        auto slotID = getDeletedSlot(data);
        if(slotID<0){
            slotID = info[SLOT_NUM];
            info[SLOT_NUM]++;
            info[INFO_OFFSET] += SLOT_SIZE;
        }
        rid = {num, slotID};
        // Write the record to page
        memcpy(data+info[DATA_OFFSET], record.data, record.size);
        // Write a new slot information
        std::pair<short int ,short int> newSlot;
        newSlot = {info[DATA_OFFSET], record.size};
        auto info_offset = sizeof(unsigned short)*PAGE_INFO_NUM + rid.slotNum*SLOT_SIZE;
        memcpy(data + PAGE_SIZE - info_offset - SLOT_SIZE, &newSlot, SLOT_SIZE);
        // Update information
        info[DATA_OFFSET] += record.size;
        // Write back
        memcpy((void*)(data + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);
        // Write to disk
        handle.writePage(num, data);

        delete [] info;
    }

    void RecordBasedFileManager::getInfo(const char* data, unsigned *info){
        memcpy(info, data + PAGE_SIZE - sizeof(unsigned)*PAGE_INFO_NUM, sizeof(unsigned)*PAGE_INFO_NUM);
    }

    unsigned short RecordBasedFileManager::getDeletedSlot(const char* data){
        auto* info = new unsigned [PAGE_INFO_NUM];
        getInfo(data, info);
        auto slotNum = (info[INFO_OFFSET]-sizeof(short)*PAGE_INFO_NUM)/SLOT_SIZE;
        for(unsigned short i=0;i<slotNum;i++){
            auto slot = getSlotInfo(i, data);
            if(slot.first == DELETE_MARK) return i;
        }
        return -1;
    }

    // Should also return the flag information and stick to the format as before
    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        if(fileHandle.readPage(rid.pageNum, pageData)==0){
            auto slot = getSlotInfo(rid.slotNum, pageData);
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
    std::pair<short int, short int> RecordBasedFileManager::getSlotInfo(short unsigned slotNum, const char* data) {
        void* slotPtr = (void *) (data + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM - (slotNum + 1) * SLOT_SIZE);
        return reinterpret_cast<std::pair<short int,short int>*>(slotPtr)[0];
    }

    void RecordBasedFileManager::fetchRecord(int offset, int recordSize, void *data, void *page) {
        char* recordPtr = (char*)page + offset;
        short int fieldNum = *(short int*)recordPtr;
        char* flagPtr = recordPtr + sizeof(short int);
        short int flag_size = std::ceil( static_cast<double>(fieldNum) /CHAR_BIT);
        char* dataPtr = flagPtr + flag_size + INDEX_SIZE*fieldNum;

        memcpy(data, flagPtr, flag_size);
        memcpy((char*)data+flag_size, dataPtr, recordSize-flag_size-INDEX_SIZE*fieldNum-sizeof(short));
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
     *  TODO：
     *  1. Compact the page, reuse the space after deletion
     *  2. Reuse the deleted slot space
     */
    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        char* pageData = new char [PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, pageData);


        return -1;
    }

    /*
     * 1. Migrate: tombstone -  Pointing to the new location
     * 2. Compact: reuse space
     */
    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    unsigned RecordBasedFileManager::getNextAvailablePageNum(short int insertSize, FileHandle &handle, unsigned int startingNum) {
        for (int i = 0; i < handle.getNumberOfPages(); ++i) {
            char* pageData = new char [PAGE_SIZE];
            memset(pageData, 0, PAGE_SIZE);
            handle.readPage((startingNum + i) % handle.getNumberOfPages(), pageData);
            if(insertSize<getFreeSpace(pageData)){
                delete[] pageData;
                return (startingNum+i)%handle.getNumberOfPages();
            }
            delete[] pageData;
        }
        appendNewPage(handle);

        return handle.getNumberOfPages()-1;
    }

    unsigned RecordBasedFileManager::getFreeSpace(char* data) {
        auto* info = new unsigned [PAGE_INFO_NUM];
        getInfo(data, info);
        unsigned data_offset = info[DATA_OFFSET];
        unsigned info_offset = info[INFO_OFFSET];
        delete [] info;
        return PAGE_SIZE-data_offset-info_offset;
    }

    void RecordBasedFileManager::updateInfo(FileHandle& fileHandle, char* data, unsigned pageNum, std::map<unsigned, unsigned> valMap){
        auto* info = new unsigned [PAGE_INFO_NUM];
        getInfo(data, info);
        for(const auto &entry: valMap){
            info[entry.first] = entry.second;
        }
        memcpy((void*)(data + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);
        fileHandle.writePage(pageNum, data);
    }

    // Used to compact the page
    // 1. Update the page information
    // 2. Compact the page
    void RecordBasedFileManager::compact(FileHandle &fileHandle, int pageNum, short idx, short offset) {

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

    void Record::buildRecord(const std::vector<Attribute> &descriptor, const void* value) {
        short int flag_size = std::ceil( static_cast<double>(descriptor.size()) /CHAR_BIT);
        char* ptr = (char*)value+flag_size;
        short int offset = sizeof(short int)+flag_size + INDEX_SIZE * fieldNum;
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
                memcpy(&varCharSize, ptr, sizeof(uint32_t));
                ptr +=  (sizeof(int) + varCharSize);
                totalSize += (sizeof(int) + varCharSize);
            }
        }

        size = sizeof(short int)+flag_size + INDEX_SIZE*fieldNum + totalSize;
        data = new char [size];
        memset(data, 0, sizeof(char)*size);
        short int recordOffset = 0;
        // # of fields
        memcpy(data+recordOffset, &fieldNum, INDEX_SIZE);
        recordOffset += INDEX_SIZE;

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
}// namespace PeterDB

