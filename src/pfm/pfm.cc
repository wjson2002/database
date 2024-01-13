#include "src/include/pfm.h"
#include <iostream>
#include <sys/stat.h>
#include <map>
#include <vector>
#include <algorithm>

namespace PeterDB {
    std::map<std::string, FILE*> filesMap;
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();

        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        struct stat stFileInfo{};

        bool exist = stat(fileName.c_str(), &stFileInfo) == 0;
        if(exist)
        {
            std::cerr << "File already created" << std::endl;
            return -1;
        }

        FILE* file;
        file = fopen(fileName.c_str(), "w");
        if(file == nullptr)
        {
            std::cerr << "File failed to create" << std::endl;
            return -1;
        }
        fclose(file);

        return 0;

    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        int result = remove(fileName.c_str());
        if (result != 0)
        {
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {

        if(fileHandle.file != nullptr){
            std::cerr << "FileHandle already assigned to open file" << std::endl;

            return -1;
        }
        FILE* file = fopen(fileName.c_str(), "wb");
        if(file == nullptr)
        {
            std::cerr << "File failed to open" << std::endl;
            printf("File failed to openf");
            return -1;
        }
        else
        {
            printf("File successf");
            fileHandle.file = file;
            return 0;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        FILE* file = fileHandle.file;
        fclose(file);
        fileHandle.file = nullptr;

        return 0;
    }

    FileHandle::FileHandle() {
        FILE* file = nullptr;

        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        readPageCounter += 1;
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
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

} // namespace PeterDB