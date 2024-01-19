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
        //MAKE SURE TO CLOSE FILE AFTER OPENING
        printf("Start Creating File");
        struct stat fileInfo{};

        if(stat(fileName.c_str(), &fileInfo) == 0){
            perror("File already created");
            return -1;
        }
        FILE* file;
        file = fopen(fileName.c_str(), "wr+");
        initFile(file);
        if(file == nullptr)
        {
            std::cerr << "File failed to create" << std::endl;
            return -1;
        }
        else{
            printf("file %s created...\n now closing\n", fileName.c_str());
            fclose(file);
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
        printf("Start OpenFile\n");
        FILE* file = fopen(fileName.c_str(), "r+");
        fileHandle.initFile(file);
        unsigned test = -1;
        fseek(file,0,SEEK_SET);
        fread(&test ,sizeof(unsigned), 1, file);
        printf("OPEN Read flushed values: page{%d}}\n", test);
        if(file == nullptr)
        {
            perror("File failed to open");
            return -1;
        }
        else {
            printf("File Fopened %s \n", fileName.c_str());
            return 0;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        FILE* file = fileHandle.myFile;
        fileHandle.flushFile();

        int result = fclose(file);
        printf("file closed\n");
        return 0;
    }

    void PagedFileManager::initFile(FILE* file) {
        printf("initing file\n");
        fseek(file,0,SEEK_SET);
        int defaultPageData[4] = {0, 0,0,0};
        size_t write = fwrite(defaultPageData, PAGE_SIZE, 1, file);
        if(write<=0){
            perror("init hidden page write failed");
        }
        else{
            printf("init hidden wrote %zu bytes\n", write);
        }
    }

    FileHandle::FileHandle() {
        FILE* myFile = nullptr;
        unsigned numOfPages = 0;
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if(pageNum > getNumberOfPages())
        {
            perror("Page doesn't exist");
            return -1;
        }

        readPageCounter++;
        unsigned offset = (pageNum + 1) * PAGE_SIZE;
        printf("Offset: {%d}\n", offset);
        fseek(myFile,offset,SEEK_SET);
        size_t read = fread(data, 1, PAGE_SIZE, myFile);

        if(read <= 0){
            perror("Read Error: readPage");
            return -1;
        }
        else{
            printf("Read Bytes: %zu\n", read);
            return 0;
        }

    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(pageNum > getNumberOfPages())
        {
            perror("Page doesn't exist");
            return -1;
        }

        writePageCounter++;
        unsigned offset = (pageNum + 1) * PAGE_SIZE;
        printf("Offset: {%d}\n", offset);
        fseek(myFile,offset,SEEK_SET);
        size_t write = fwrite(data, 1, PAGE_SIZE, myFile);

        if(write <= 0){
            perror("Read Error: readPage");
            return -1;
        }
        else{
            printf("Write Bytes: %zu\n", write);
            return 0;
        }
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        //write to end
        printf("Start appending Page\n");
        numOfPages++;
        appendPageCounter++;
        fseek(myFile,0,SEEK_END);
        size_t write = fwrite(data, 1, PAGE_SIZE, myFile);;

        if(write <= 0){
            perror("Write Error: appendPage");
            return -1;
        }
        else{
            printf("Append bytes written: %zu", write);
            return 0;
        }
    }

    unsigned FileHandle::getNumberOfPages() {
        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        //if num of pages is 0, insert hidden page and write default values
        //else read first 3 integers
        printf("start collectCounterValues\n");
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    void FileHandle::initFile(FILE *file) {
        myFile = file;
        loadFile();
    }

    void FileHandle::loadFile(){
        printf("Loading File...\n");

        fseek(myFile,0,SEEK_SET);
        fread(&numOfPages, sizeof(unsigned), 1, myFile);
        printf("Number of Pages Loaded: {%d} ", numOfPages);

        fread(&readPageCounter, sizeof(unsigned), 1, myFile);
        printf("Read Count Loaded: %d ", readPageCounter);

        fread(&writePageCounter, sizeof(unsigned), 1, myFile);
        printf("Write Count Loaded: %d ", writePageCounter);

        printf("Loading File Finished...\n");
    }
    void FileHandle::flushFile() {
        printf("Flushing values: page{%d}, read{%d}, write{%d}\n", numOfPages,readPageCounter, writePageCounter);
        fseek(myFile,0,SEEK_SET);

        fwrite(&numOfPages,sizeof(unsigned), 1, myFile);
        fwrite(&readPageCounter,sizeof(unsigned), 1, myFile);
        fwrite(&writePageCounter,sizeof(unsigned), 1, myFile);
        fwrite(&appendPageCounter,sizeof(unsigned), 1, myFile);

        size_t bufferSize = PAGE_SIZE - (sizeof(unsigned) * 4);
        char *buffer = (char *)malloc(bufferSize);
        fwrite(buffer, 1, bufferSize, myFile);
        free(buffer);

        fflush(myFile);

        printf("Flushing values FINISHED\n");

        printf("testing flush values\n");
        unsigned test = -1;
        fseek(myFile,0,SEEK_SET);
        fread(&test ,sizeof(unsigned), 1, myFile);
        printf("Read flushed values: page{%d}, read{%d}, write{%d}\n", test,readPageCounter, writePageCounter);
    }

    void FileHandle::initPage() {
        if(myFile != nullptr){
            fseek(myFile,0,SEEK_END);
            printf("calling init: file size: %ld\n", ftell(myFile));
            printf("reseting values to 0\n");

            fseek(myFile,0,SEEK_SET);
            int defaultPageData[4] = {0, 0,0,0};
            size_t write = fwrite(defaultPageData, PAGE_SIZE, 1, myFile);

            fseek(myFile,0,SEEK_END);
            printf("after: file size: %ld\n", ftell(myFile));
            if(write <= 0)
            {
                perror("initPage write fail");
            }

        }
        else{
            perror("Unable to init Page");
        }
    }

    void FileHandle::getFileSize(FILE* file){
        if(file != nullptr) {
            fseek(file, 0, SEEK_END);
            printf("file size: %ld\n", ftell(file));
            fseek(file, 0, SEEK_SET);
        }
    }




} // namespace PeterDB