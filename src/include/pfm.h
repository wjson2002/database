#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>


namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance
        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file
        void pfmInitFile(FILE* file);

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        FILE* myFile = nullptr;
        unsigned numOfPages = 0;
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        std::string FileName;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values



        void initFile(FILE* file);
        void loadFile();
        void flushFile();
        void initPage();
        void getFileSize(FILE* file);

    };

} // namespace PeterDB

#endif // _pfm_h_