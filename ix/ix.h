#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

void* getValue(void * node, int offset, const Attribute &attribute);
int compareVals(const void * val1, void * val2, const Attribute &attribute);

typedef struct NodeHeader
{
    uint16_t numEntries;
    int freeSpaceOffset;
    bool isLeaf;
    // We'll need the addresses of the previous and next node
    void *nextNode;
    void *previousNode;
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
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        NodeHeader getNodeHeader(void *page);
        void setNodeHeader(NodeHeader header, void * page);
        LeafEntry getLeafEntry(void * page, unsigned entryNumber);
        NonLeafEntry getNonLeafEntry(void * page, unsigned entryNumber);
        void setLeafEntry(void * page, unsigned entryNumber, LeafEntry lEntry);
        void * searchTree(IXFileHandle &ixfileHandle, const void* value, const Attribute &attribute, int nodeNum=0);
        void moveEntries(void * page, int i, NodeHeader header);
        int freeSpaceStart(void *page);
        int getKeySize(void * page, void * key, const Attribute &attribute);
};


class IX_ScanIterator {
    public:
        void *currentNode;
        void *endNode;
        int startFlag;
        bool lowKeyInclusive;
        bool highKeyInclusive;
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

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
