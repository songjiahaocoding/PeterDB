#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    #define NULL_NODE -1

    class IX_ScanIterator;

    class IXFileHandle;


    #define Slot_Size sizeof(Slot)
    struct Slot {
        short offset;
        short len;
        short rid_num;
    };

    struct keyEntry{
        keyEntry(int left, char *key, int right);
        keyEntry() = default;

        int left = -1;
        char* key;
        int right = -1;
    };

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        char* low;
        char* high;
        char* page;
        bool lowInclusive;
        bool highInclusive;
        int pageNum;
        int slotNum;
        int curCount;
        int ridNum;
        int curRIDNum;
        IXFileHandle* fileHandle;

        Attribute attr;

        RC init(IXFileHandle &handle, const Attribute &attr, const void *low, const void *high, bool lowInclusive,
                bool highInclusive);

        void moveToLeft();

        void moveToNext();

        bool inRange(char* key);
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        FileHandle fileHandle;

        int getRoot();

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        void setRoot(unsigned int num);

        char* rootPage;
    };

    // the structure of metadata for intermediate node
    enum {
        NODE_TYPE = PAGE_INFO_NUM,
        PARENT,
        PRE,
        NEXT,
        TREE_NODE_SIZE
    };

    class Tool{
    public:
        static float compare(char* key1, char* key2, Attribute& attr);

        static void writeSlot(char *data, short offset, short len, short rid_num, int i);

        static void appendSlot(char *data, int *info, short len);

        static void updateInfo(int *info, int slot_num, int data_offset, int info_offset);

        static Slot getSlot(char *page, int i);

        static void getKey(char *data, unsigned int pos, unsigned int len, char *key);

        static void search(char *data, Attribute &attr, char *key, int& pos, int& left, int& len);

        static void moveBack(char *data, int offset, int distance, int length);

        static void updateSlot(char *data, int *info, int dis, int i);

        static void shiftEntry(char *data, int i, int pos, int len, int *info);

        static void writeInfo(char *data, int *info);

        static bool isNull(int i, char* data) {
            int bytePosition = i / 8;
            int bitPosition = i % 8;
            char b = data[bytePosition];
            return ((b >> (7 - bitPosition)) & 0x1);
        }

        static void setNull(int i, char* data){
            int byteNum = (double)i / 8;
            int bitNum = i % 8;
            char mask = 0x01 << (7 - bitNum);
            data[byteNum] |= mask;
        }

        static void buildNullBytes(std::vector<Attribute> attrs, char *tuple, int offset, char *nullIndicator);

        static void mergeTwoTuple(std::vector<Attribute> attr1, char *tuple1, int size1, std::vector<Attribute> attr2, char *tuple2,
                      int size2, void *data);
    };

    enum {
        ROOT,
        NODE,
        LEAF,
    };
    // intermediate node
    class Node {
    public:
        static bool isNode(char* pageData);

        static void insertEntry(IXFileHandle& ixFileHandle, int pageNum, Attribute& attr, keyEntry& entry, RID& rid, keyEntry* child);

        static RC deleteEntry(IXFileHandle& ixFileHandle, int paPageNum, int pageNum, const Attribute &attr, keyEntry& entry, RID& rid, keyEntry* child);

        static void getInfo(int* info, char* data);

        static bool haveSpace(char *data, const char *key, Attribute& attr);

        static void appendKey(char* data, keyEntry& entry, Attribute& attr);

        static void createNode(char *data, int type, int parent);

        static void split(char *data, char *page, char* middle);

        static void removeKey(IXFileHandle &ixFileHandle, int pageNum, keyEntry entry, const Attribute &attr);

        static void print(IXFileHandle &ixFileHandle, const Attribute &attribute, int root, int i, std::ostream &out);

        static int searchPage(char *page, Attribute attribute, char *low);

        static void insertKey(char *data, keyEntry entry, Attribute &attr);
    };
    // Leaf node
    class Leaf {
    public:
        static void insertEntry(char* leafData, const Attribute& attr, keyEntry& entry, RID& rid);

        static RC deleteEntry(char *leafData, const Attribute &attr, keyEntry &entry, RID &rid);

        static void getInfo(int* info, char* leafData);

        static bool haveSpace(char *data, const Attribute &attr, const char *key);

        static void createLeaf(char *page, int parent, int pre, int next);

        static void split(char *data, char *newData);

        static void print(char *data, const Attribute &attr, std::ostream &out);

        static bool equal(RID &rid, char *pos, int len);

        static void appendToKey(char *data, int* info, const Attribute &attr, int i, RID &rid);
    };
}// namespace PeterDB
#endif // _ix_h_
