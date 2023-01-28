#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <map>
#include "pfm.h"

// More than 4096 the PAGE_SIZE
#define DELETE_MARK 5000;


namespace PeterDB {

    #define TOMB_SIZE sizeof(unsigned) + sizeof(short)+INDEX_SIZE


    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;
    #define FIELD_NUM_SIZE sizeof(short)
    #define INDEX_SIZE 2
    class Record {
    public:
        Record(const std::vector<Attribute> &recordDescriptor,
               const void *data, RID &rid);
        ~Record();
        bool isNull(int);
        const char* getRecord() const;

        RID rid;
        short int  fieldNum;
        short int  size;
        short int* index;
        void buildRecord(const std::vector<Attribute> &descriptor, const void* data);
        char* flag;
        char* data;
    };


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
    public:
        RBFM_ScanIterator() = default;;

        ~RBFM_ScanIterator() = default;;

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };

        RC close() { return -1; };
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        RC appendNewPage(FileHandle &fileHandle);

        void writeRecord(const Record &record, FileHandle &fileHandle, unsigned availablePage, RID &rid, char* data);
        void fetchRecord(int offset, int recordSize, void* data, void* page);
        unsigned int getFreeSpace(char* data);

        void compact(FileHandle& fileHandle, int pageNum, short idx, short offset);

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment
        unsigned getNextAvailablePageNum(short int insertSize, PeterDB::FileHandle &handle, unsigned num);

        std::pair<short, short> getSlotInfo(unsigned short slotNum, const char *data);

        void updateInfo(FileHandle& fileHandle, char *data, unsigned pageNum, unsigned* info);

        bool isTomb(char *data);

        short getDeletedSlot(char *data);

        void getInfo(char *data, unsigned int *info);

        RID getPointRID(char *data_offset);

        void insertTomb(char *data, unsigned int offset, unsigned int pageNum, unsigned short slotNum);

        void insertTomb(char *data, unsigned int pageNum, unsigned short slotNum);

        void writeSlotInfo(unsigned short slotNum, const char *data, std::pair<short, short> slot);

        void shiftRecord(char *data, unsigned int offset, unsigned size, unsigned int shiftOffset, unsigned int len);

        void
        writeUpdateInfo(FileHandle &fileHandle, unsigned int *info, std::pair<short, short> slot, unsigned int size,
                        unsigned int oldSize, const RID &rid, char *pageData);
    };

    #define SLOT_SIZE sizeof(std::pair<uint16_t, uint16_t>)
    enum pageInfo {
        INFO_OFFSET = 0,
        DATA_OFFSET,
        SLOT_NUM,
        PAGE_INFO_NUM
    };

    class tombStone{
    public:
        unsigned pageNum;
        unsigned short slotNum;
        tombStone(unsigned pageNum, unsigned short slotNum);
        tombStone() = default;
        ~tombStone() = default;
    };
} // namespace PeterDB

#endif // _rbfm_h_
