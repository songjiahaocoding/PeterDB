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

    struct keyEntry{
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
        IXFileHandle* fileHandle;

        Attribute attr;

        RC init(IXFileHandle &handle, const Attribute &attr, const void *low, const void *high, bool lowInclusive,
                bool highInclusive);

        void moveToLeft();

        void increaseRID();

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
        NODE_SIZE
    };
    // The additional structure for leaf node
    enum {
        PRE = NODE_SIZE,
        NEXT,
        LEAF_SIZE
    };

    class Tool{
    public:
        static float compare(char* key1, char* key2, Attribute& attr);

        static void writeSlot(char *data, int offset, int len, int i, int size);

        static void appendSlot(char *data, int *info, int len);

        static void updateInfo(int *info, int slot_num, int data_offset, int info_offset);

        static std::pair<unsigned, unsigned> getSlot(char *page, int i, int size);

        static void getKey(char *data, unsigned int pos, unsigned int len, char *key);

        static void search(char *data, Attribute &attr, char *key, int *pos, int* left, int* len, int size);

        static void moveBack(char *data, int offset, int distance, int length);

        static void updateSlot(char *data, int *info, int dis, int size, int i);
    };

    // mainly for child entry of insert
    struct childEntry {
        char* key;
        int pageNum;
    };
    enum {
        ROOT,
        NODE,
        LEAF,
        LEAF_ROOT
    };
    // intermediate node
    class Node {
    public:
        static bool isNode(char* pageData);

        static void insertEntry(IXFileHandle& ixFileHandle, int pageNum, Attribute& attr, keyEntry& entry, RID& rid, childEntry* child);

        static void deleteEntry(IXFileHandle& ixFileHandle, int paPageNum, int pageNum, const Attribute &attr, keyEntry& entry, childEntry* child);

        static void getInfo(int* info, char* data);

        static bool haveSpace(char *data, const char *key, Attribute& attr);

        static void appendKey(char* data, keyEntry& entry, Attribute& attr);

        static void createNode(char *data, int type, int parent);

        static void split(char *data, char *page, char* middle);

        static void writeInfo(char *data, int *info);

        static void removeKey(IXFileHandle &ixFileHandle, int pageNum, keyEntry entry, const Attribute &attr);

        static void print(IXFileHandle &ixFileHandle, const Attribute &attribute, int root, int i, std::ostream &out);

        static int searchPage(char *page, Attribute attribute, char *low);
    };
    // Leaf node
    class Leaf {
    public:
        static void insertEntry(char* leafData, const Attribute& attr, keyEntry& entry, RID& rid);

        static void deleteEntry(char* leafData, const Attribute& attr, keyEntry& entry);

        static void getInfo(int* info, char* leafData);

        static bool haveSpace(char *data, const Attribute &attr, const char *key);

        static void createLeaf(char *page, int parent, int pre, int next);

        static void split(char *data, char *newData);

        static void writeInfo(char *data, int *info);

        static void print(char *data, const Attribute &attr, std::ostream &out);


    };
}// namespace PeterDB
#endif // _ix_h_
