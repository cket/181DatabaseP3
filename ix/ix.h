#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

#define NONODE (-1)
#define NEWLESSTHANNODE (-2)
#define NEWGREATERTHANNODE (-3)

void* getValue(void * node, int offset, const Attribute &attribute);
int compareVals(const void * val1, void * val2, const Attribute &attribute);

typedef struct NodeHeader
{
    uint16_t numEntries;
    int freeSpaceOffset;
    bool isLeaf;
    // We'll need the addresses of the previous and next node
    int nextNode;
    int previousNode;
} NodeHeader;

typedef struct NonLeafEntry
{
    int offset; //offset to value. can't store directly b/c don't know data type
    int lessThanNode; //page num to child node containing entries<value at offset
    int greaterThanNode; //page num to child node containing entries>value at offset
} NonLeafEntry;

typedef struct LeafEntry
{
    int offSet; //offset to key
    RID rid; //rid for rest of record
} LeafEntry;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        PagedFileManager* _pf_manager;
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;	//PROBLEM - when things explode, look here.
	void printValue(void* data, const Attribute &attribute) const;
	void printRecur(IXFileHandle ixfileHandle, int pageNum, const Attribute& attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        void setNodeHeader(NodeHeader header, void * page);
        void setNonLeafEntry(void * page, unsigned entryNumber, NonLeafEntry nEntry);
        void setLeafEntry(void * page, unsigned entryNumber, LeafEntry lEntry);
        int searchTree(IXFileHandle &ixfileHandle, const void* value, const Attribute &attribute, int nodeNum, unsigned &parentNodeNumber);
        void moveEntries(void * page, int i, NodeHeader header);
        void moveNonLeafEntries(void * page, int i, NodeHeader header);
        int freeSpaceStart(void *page);
        int getKeySize(const void * key, const Attribute &attribute);
        RC createLeaf(IXFileHandle &ixfileHandle, const RID &rid, const void *key, unsigned &pageNumber, const Attribute &attribute);
        void deleteLeafEntry(void * page, int i, NodeHeader header);
        int getMostLeftLeafNumber(IXFileHandle &ixfileHandle);
        int getMostRightLeafNumber(IXFileHandle &ixfileHandle);
        RC deleteEntryOnPage(void * node, const RID &rid);
};

//We want to use these functions in scan iterator and they don't require any specific members of IndexManager, so I moved them outside
        NodeHeader getNodeHeader(const void *node);
        LeafEntry getLeafEntry(void * page, unsigned entryNumber);
	NonLeafEntry getNonLeafEntry(void * page, unsigned entryNumber);

class IX_ScanIterator {
    public:
        IXFileHandle *ixfileHandle;
        int currentNode; // page of the current node
        int endNode; // page of the end node
        int startFlag;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        bool done;
        Attribute attribute;
        void *lowKey;
        void *highKey;
        int currentEntryNumber;
		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle: public FileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
