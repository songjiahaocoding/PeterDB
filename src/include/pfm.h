#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <fstream>

namespace PeterDB {

    typedef unsigned PageNum;
    // Return code
    // 0 - normal
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    enum infoVal {
        READ_NUM,
        WRITE_NUM,
        APPEND_NUM,
        ACTIVE_PAGE_NUM,
        INFO_NUM
    };

    class infoPage {
    public:
        unsigned info[INFO_NUM];

        infoPage();
        ~infoPage();

        void readInfoPage(std::FILE*);
        void flushInfoPage(std::FILE*);
    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor
        FileHandle &operator=(const FileHandle &);

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
        RC openFile(const std::string &fileName);
        bool handlingFile();

        RC closeFile();

    private:
        FILE *file;
        PeterDB::infoPage *infoPage;
    };

    class Page {
    public:
        Page();
        ~Page();


    };
} // namespace PeterDB

#endif // _pfm_h_