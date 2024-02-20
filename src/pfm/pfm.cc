#include "src/include/pfm.h"
#include <iostream>
#include <sys/stat.h>


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

        struct stat fileInfo{};
        if(stat(fileName.c_str(), &fileInfo) == 0){
            return -1;
        }
        FILE* file;
        file = fopen(fileName.c_str(), "wrb+");

        pfmInitFile(file);
        if(file == nullptr)
        {
            std::cerr << "File failed to create" << std::endl;
            return -1;
        }
        else{
            printf("file created... now closing\n");
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

        FILE* file = fopen(fileName.c_str(), "rb+");
        if(file == nullptr)
        {
            perror("File failed to open");
            return -1;
        }
        else {
            fileHandle.FileName = fileName;
            fileHandle.myFile = file;
            fileHandle.initFile(file);
            unsigned test = -1;
            fseek(file,0,SEEK_SET);
            fread(&test ,sizeof(unsigned), 1, file);
            printf("OPEN File with: pages: {%d}}\n", test);
            return 0;
        }


    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        FILE* file = fileHandle.myFile;
        if(file != nullptr){
            fileHandle.flushFile();
            fclose(file);
            return 0;
        }
        else{
            return -1;
        }
    }

    void PagedFileManager::pfmInitFile(FILE* file) {
        fseek(file,0,SEEK_SET);
        int defaultPageData[4] = {0, 0,0,0};
        size_t write = fwrite(defaultPageData, PAGE_SIZE, 1, file);
        if(write<=0){
            perror("init hidden page write failed");
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
        fseek(myFile,offset,SEEK_SET);
        size_t read = fread(data, 1, PAGE_SIZE, myFile);

        if(read <= 0){
            perror("Read Error: readPage");
            return -1;
        }
        else{
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

        fseek(myFile,offset,SEEK_SET);
        size_t write = fwrite(data, 1, PAGE_SIZE, myFile);

        if(write <= 0){
            perror("Read Error: readPage");
            return -1;
        }
        else{
            return 0;
        }

    }

    RC FileHandle::appendPage(const void *data) {
        //write to end
        numOfPages++;
        appendPageCounter++;
        fseek(myFile,0,SEEK_END);
        size_t write = fwrite(data, PAGE_SIZE, 1, myFile);


        if(write <= 0){
            perror("Write Error: appendPage");
            return -1;
        }
        else{
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
       // printf("Loading File...");

        fseek(myFile,0,SEEK_SET);
        fread(&numOfPages, sizeof(unsigned), 1, myFile);
       // printf("Page Count:{%d} ", numOfPages);

        fread(&readPageCounter, sizeof(unsigned), 1, myFile);
       // printf("Read Count:{%d} ", readPageCounter);

        fread(&writePageCounter, sizeof(unsigned), 1, myFile);
       // printf("Write Count: {%d} ", writePageCounter);

        fread(&appendPageCounter, sizeof(unsigned), 1, myFile);
        //printf("....Loading File Finished...\n");
    }
    void FileHandle::flushFile() {
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

        //printf("flushed values: page{%d}, read{%d}, write{%d}, append{%d}\n", numOfPages,readPageCounter, writePageCounter,appendPageCounter);
    }

    void FileHandle::initPage() {
        if(myFile != nullptr){
            fseek(myFile,0,SEEK_END);
            //printf("calling init: file size: %ld\n", ftell(myFile));


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