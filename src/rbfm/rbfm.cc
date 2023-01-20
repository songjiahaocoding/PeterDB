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
     */
    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if(fileHandle.getNumberOfPages() == 0) {
            // Insert a new page
            appendNewPage(fileHandle);
        }

        const Record record(recordDescriptor, data, rid);
        /*
         * find the next available page and perform the follwing operation
         *  - insert the record into a page
         *  - update the slot information at the back of this page
         */
        char* pageData = new char [PAGE_SIZE];
        memset((void*)pageData, 0, PAGE_SIZE);

        uint32_t lastPageNum = fileHandle.getNumberOfPages()-1;
        unsigned pageNum = getNextAvailablePageNum(record.size + SLOT_SIZE, fileHandle, lastPageNum);
        fileHandle.readPage(pageNum, (void *) pageData);
        Page page(pageData);
        page.writeRecord(record, fileHandle, pageNum, rid);

//        char* val = new char [PAGE_SIZE];
//        memset(val, 0, PAGE_SIZE);
//        readRecord(fileHandle, recordDescriptor, rid, val);
//        printRecord(recordDescriptor, val, std::cout);
        delete [] pageData;

        return 0;
    }

    // Should also return the flag information and stick to the format as before
    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* pageData = new char [PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        if(fileHandle.readPage(rid.pageNum, pageData)==0){
            Page page(pageData);
            auto slot = page.getSlotInfo(rid.slotNum);
            page.readRecord(fileHandle, slot.first, slot.second, data);
            delete [] pageData;
            return 0;
        }
        delete [] pageData;
        return -1;
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
     */
    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

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

    unsigned int RecordBasedFileManager::getNextAvailablePageNum(short int insertSize, FileHandle &handle, unsigned int startingNum) {
        for (int i = 0; i < handle.getNumberOfPages(); ++i) {
            char* pageData = new char [PAGE_SIZE];
            memset(pageData, 0, PAGE_SIZE);
            handle.readPage((startingNum + i) % handle.getNumberOfPages(), pageData);
            Page page(pageData);
            if(insertSize<page.getFreeSpace()){
                delete[] pageData;
                return (startingNum+i)%handle.getNumberOfPages();
            }
            delete[] pageData;
        }
        appendNewPage(handle);

        return handle.getNumberOfPages()-1;
    }

    Record::Record(const std::vector<Attribute> &recordDescriptor, const void *val, RID &rid) {
        fieldNum = recordDescriptor.size();
        flag = (char*)val;
        buildRecord(recordDescriptor, val);
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

    const char* Record::getRecord() const {
        return data;
    }

    Page::Page(const void *data) {
        memcpy(&info, (char*)data + PAGE_SIZE - sizeof(unsigned)*PAGE_INFO_NUM, sizeof(unsigned)*PAGE_INFO_NUM);

        page = new char [PAGE_SIZE];
        memcpy(page, data, PAGE_SIZE);
    }

    // Read the indicated record
    void Page::readRecord(FileHandle &fileHandle, int offset, int recordSize, void *data) {
        char* recordPtr = page + offset;
        short int fieldNum = *(short int*)recordPtr;
        char* flagPtr = recordPtr + sizeof(short int);
        short int flag_size = std::ceil( static_cast<double>(fieldNum) /CHAR_BIT);
        char* dataPtr = flagPtr + flag_size + INDEX_SIZE*fieldNum;

        memcpy(data, flagPtr, flag_size);
        memcpy((char*)data+flag_size, dataPtr, recordSize-flag_size-INDEX_SIZE*fieldNum-sizeof(short));
    }

    // Write record to the given place
    void Page::writeRecord(const Record &record, FileHandle &fileHandle, unsigned availablePage, RID &rid) {
        // Assign the new RID

        // rid didn't change?
        rid = {availablePage, static_cast<unsigned short>(info[SLOT_NUM])};
        // Write the record to page
        memcpy(page+info[DATA_OFFSET], record.getRecord(), record.size);
        // Write a new slot information
        std::pair<short int ,short int> newSlot;
        newSlot = {info[DATA_OFFSET], record.size};
        memcpy(page + PAGE_SIZE - info[INFO_OFFSET] - SLOT_SIZE, &newSlot, SLOT_SIZE);
        // Update information
        info[DATA_OFFSET] += record.size;
        info[INFO_OFFSET] += SLOT_SIZE;
        info[SLOT_NUM]++;
        // Write back
        memcpy((void*)(page + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM), info, sizeof(unsigned) * PAGE_INFO_NUM);
        // Write to disk
        fileHandle.writePage(availablePage, page);
    }

    /*
     * Get the slot from disk
     * Slot:
     *  first: offset from the beginning
     *  second: The record size
    */
    std::pair<short int, short int> Page::getSlotInfo(short unsigned slotNum) {
        void* slotPtr = page + PAGE_SIZE - sizeof(unsigned) * PAGE_INFO_NUM - (slotNum + 1) * SLOT_SIZE;
        return reinterpret_cast<std::pair<uint16_t,uint16_t>*>(slotPtr)[0];
    }

    unsigned Page::getFreeSpace() {
        return PAGE_SIZE-info[DATA_OFFSET]-info[INFO_OFFSET];
    }

    Page::~Page() = default;
}// namespace PeterDB

