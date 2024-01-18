#include "src/include/pfm.h"
#include <iostream>
#include <sys/stat.h>
#include <map>
#include <unistd.h>

namespace PeterDB {

    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();

        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        FILE* file;
        file = fopen(fileName.c_str(), "wrb");
        if(file == nullptr)
        {
            std::cerr << "File failed to create" << std::endl;
            return -1;
        }
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
        if(fileHandle.f == nullptr){
            FILE* file = fopen(fileName.c_str(), "wrb+");

            if(file == nullptr)
            {
                std::cerr << "File failed to open" << std::endl;
                return -1;
            }
            else
            {
                std::cerr << "File fopened " << std::endl;
                fileHandle.initFile(file);
                return 0;
            }
        }
        else
        {
            std::cerr << "FileHandle already assigned to open file?" << std::endl;
            return -1;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        FILE* file = fileHandle.f;
        int result = fclose(file);
        printf("file closed");
        return 0;
    }

    FileHandle::FileHandle() {
        FILE* f = nullptr;

        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {

        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {

        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(f,0,SEEK_END);

        size_t write = fwrite(data, PAGE_SIZE, 1, f);

        if(write <= 0){
            perror("Write Error: appendPage");
            return -1;
        }

        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        fseek(f,0,SEEK_END);
        long fileSize = ftell(f);
        int numOfPages = (int)(fileSize / PAGE_SIZE);
        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {


        return -1;
    }

    void FileHandle::initFile(FILE *file) {
        f = file;
    }


} // namespace PeterDB