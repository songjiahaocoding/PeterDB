#include <iostream>
#include "src/include/pfm.h"

namespace PeterDB {
    // Meyers' Singleton
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    // default: Generated constructor which will do nothing
    PagedFileManager::PagedFileManager() = default;

    // destructor: will be invoked when the object is deleted
    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    // operator overloading: Prevent assignment
    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        std::ifstream file(fileName);
        if(!file){
            std::ofstream f(fileName);
            if(f){
                f.close();
                return 0;
            } else {
                std::cout << "Error when creating the file" << std::endl;
                return -1;
            }
        }
        file.close();
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        std::ifstream file(fileName);
        if(!file){
            std::cout << "The file "<< fileName << " does not exist"<< std::endl;
            return -1;
        } else {
            if(remove(fileName.c_str())!=0){
                std::cout << "The deletion failed: " << fileName << std::endl;
            }
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // Error: it is already a handle for some open file
        if(fileHandle.handlingFile())return -1;
        std::ifstream file(fileName);
        if(!file) {
            std::cout << "The file "<< fileName << " does not exist"<< std::endl;
            return -1;
        }
        return fileHandle.openFile(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        // Error: The handler is not handling any file
        if(!fileHandle.handlingFile())return -1;
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    FileHandle &FileHandle::operator=(const FileHandle &) {
        return *this;
    }

    // void pointer(void *data) which can be assigned to the point of any type
    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum==0)return -1;
        if(pageNum < infoPage->info[ACTIVE_PAGE_NUM]) {
            // Offset the information page
            fseek(file, (pageNum+1)*PAGE_SIZE, SEEK_SET);
            fread(data, PAGE_SIZE, 1, file);
            infoPage->info[READ_NUM]++;
            return 0;
        }
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(pageNum==0)return -1;
        if(pageNum < infoPage->info[ACTIVE_PAGE_NUM]){
            fseek(file, (pageNum+1)*PAGE_SIZE, SEEK_SET);
            fwrite(data, PAGE_SIZE, 1, file);
            infoPage->info[WRITE_NUM]++;
            return 0;
        }
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

    RC FileHandle::openFile(const std::string& fileName) {
        file = fopen(fileName.c_str(), "r+");
        std::cout << "The status of the file " << handlingFile() << std::endl;
        if(!handlingFile()){
            std::cout << "Error cannot open the file " << fileName << std::endl;
            return -1;
        }
        return 0;
    }

    RC FileHandle::closeFile(){
        fclose(file);
        return 0;
    }

    RC FileHandle::handlingFile() {
        if(file){
            return 0;
        }
        return -1;
    }


} // namespace PeterDB