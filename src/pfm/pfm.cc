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
        FILE* file = fopen(fileName.c_str(), "r");
        if(!file){
            file = fopen(fileName.c_str(), "wb");
            if(file){
                //std::cout << "Create " << fileName << std::endl;
                infoPage infoPage;
                infoPage.flushInfoPage(file);
                fclose(file);
                return 0;
            } else {
                std::cout << "Error when creating the file" << std::endl;
                fclose(file);
                return -1;
            }
        }
        fclose(file);
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
        file.close();
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
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
        file = nullptr;
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
        fseek(file, 0, SEEK_END);
        fwrite(data, PAGE_SIZE, 1, file);
        infoPage->info[ACTIVE_PAGE_NUM]++;
        infoPage->info[APPEND_NUM]++;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return infoPage->info[ACTIVE_PAGE_NUM];
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        infoPage->readInfoPage(file);
        readPageCount = infoPage->info[READ_NUM];
        writePageCount = infoPage->info[WRITE_NUM];
        appendPageCount = infoPage->info[APPEND_NUM];
        return 0;
    }

    RC FileHandle::openFile(const std::string& fileName) {
        if(handlingFile())return -1;
        file = fopen(fileName.c_str(), "r+");
        if(!handlingFile()){
            std::cout << "Error cannot open the file " << fileName << std::endl;
            return -1;
        } else {
            infoPage = new class infoPage();
            infoPage -> readInfoPage(file);
        }
        return 0;
    }

    RC FileHandle::closeFile(){
        infoPage->flushInfoPage(file);
        fclose(file);
        return 0;
    }

    bool FileHandle::handlingFile() {
        if(file != nullptr){
            return true;
        }
        return false;
    }

    infoPage::infoPage() {
        info[READ_NUM]  = 0;
        info[WRITE_NUM] = 0;
        info[APPEND_NUM] = 0;
        info[ACTIVE_PAGE_NUM] = 0;
    }

    void infoPage::readInfoPage(FILE *file) {
        fseek(file, 0, SEEK_SET);
        char* data = new char [PAGE_SIZE];
        fread(data, PAGE_SIZE, 1, file);
        auto* value = (unsigned*)data;
        info[READ_NUM] = value[READ_NUM];
        info[WRITE_NUM] = value[WRITE_NUM];
        info[APPEND_NUM] = value[APPEND_NUM];
        info[ACTIVE_PAGE_NUM] = value[ACTIVE_PAGE_NUM];
        delete value;
    }

    void infoPage::flushInfoPage(FILE *file) {
        fseek(file, 0, SEEK_SET);
        fwrite(reinterpret_cast<const void *>(info), PAGE_SIZE, 1, file);
    }

    infoPage::~infoPage() = default;
} // namespace PeterDB

