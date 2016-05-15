#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "ix.h"
#include <stdlib.h>


IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	int err;

	//should create two pages - root and leaf
	err = _pf_manager->createFile(fileName);
	if (err != 0)
	{
		return 1; //bad pfm file creation;
	}
	
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	int err;
	err = _pf_manager->destroyFile(fileName);
	if (err != 0)
	{
		return 1; //bad pfm file destruction
	}

	return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	int err;
	err = _pf_manager->openFile(fileName, ixfileHandle);
	if (err != 0)
	{
		return 1; //bad pfm file opening
	}

	return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	int err;
	err = _pf_manager->closeFile(ixfileHandle);
	if (err != 0)
	{
		return 1; //bad pfm file closing
	}
	
	return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //DOES NOT HANDLE FULL NODES
    // still needs to handle insertion of key into page & decrement freeSpace offset
    LeafEntry to_insert = LeafEntry();
    to_insert.rid = rid;

    void * node = searchTree(ixfileHandle, key, attribute, 0); //start search from root
    //we now have proper node. Search through sorted record & insert at proper location
    //get number of entries
    NodeHeader header = getNodeHeader(node);
    int keySize = getKeySize(node, key, attribute);
    int new_offset = header.freeSpaceOffset - keySize;
    //iterate over entriesTree
    for(int i = 0; i<header.numEntries; i++){
        LeafEntry entry = getLeafEntry(node, i);
        void *val2 = getValue(node, entry.offSet, attribute);
        //  compare value to entry
        if(compareVals(key, val2, attribute) < 0){
            //make space for new entry
            moveEntries(node, i, header);
            // insert at old entries offset
            setLeafEntry(node, i, to_insert);
            // update node header
            header.numEntries++;
            setNodeHeader(header, node);
            return SUCCESS;
        }
    }
}
int IndexManager::getKeySize(void *page, void * key, const Attribute &attribute)
{
    return -1;
}
int IndexManager::freeSpaceStart(void *page)
{
    NodeHeader header = getNodeHeader(node);
    int length = header.numEntries;
    int entry_size;
    if(header.isLeaf){
        entry_size = sizeof(LeafEntry);
    }else{
        entry_size = sizeof(NonLeafEntry);
    }
    return sizeof(NodeHeader) + entry_size*length;
}
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}
void IndexManager::moveEntries(void * page, int i, NodeHeader header)
{
    //make space for new entry!
    int current_offset = sizeof(NodeHeader) + sizeof(LeafEntry)*i;
    int desired_offset = current_offset + sizeof(LeafEntry);
    //how many entries after the current position we have to move
    int number_to_move = header.numEntries-i;
    int size = number_to_move * sizeof(LeafEntry);
    memmove((char*)page+desired_offset, (char*)page+current_offset, size);

    //save entries new positions!
    for(int j = i; j < header.numEntries; j++){
        LeafEntry entry = _index_manager->getLeafEntry(page, i);
        entry.offSet += sizeof(LeafEntry);
        setLeafEntry(page, j, entry);
    }
}

void* IndexManager::searchTree(IXFileHandle &ixfileHandle, const void* value, const Attribute &attribute, int nodeNum)
{
    // https://en.wikipedia.org/wiki/B%2B_tree#Search
    void * node = malloc(PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);
    //always start at root!
    ixfileHandle.readPage(nodeNum, node); 

    NodeHeader header = getNodeHeader(node);
    if(header.isLeaf){
        return node;
    }
    //not leaf so we need to find what node to find next 
    for(int i = 0; i < header.numEntries; i++){
        //will find all nodes iff value < max value on page
        NonLeafEntry entry = getNonLeafEntry(node, i);
        void * min_val = getValue(node, entry.offset, attribute);
        if(compareVals(value, min_val, attribute) < 0){
            return searchTree(ixfileHandle, value, attribute, entry.lessThanNode);
        }
    }
    //if we get here we know there is only one place to search
    NonLeafEntry entry = getNonLeafEntry(node, header.numEntries-1);
    return searchTree(ixfileHandle, value, attribute, entry.greaterThanNode);
}

void* getValue(void * node, int offset, const Attribute &attribute)
{
    void * value;
    int size = 0;
    if(attribute.type != TypeVarChar){
        size = sizeof(int);
    }
    //need to handle varchars still
    value = malloc(size);
    memcpy(value, (char*)node+offset, size);
}

int compareVals(const void * val1, void * val2, const Attribute &attribute)
{
    if(attribute.type == TypeInt){
        if(*(int*)val1 < *(int*)val2){
            return -1;
        }else if(*(int*)val1 > *(int*)val2){
            return 1;
        }else{
            return 0;
        }
    }
    if(attribute.type == TypeReal){
        if(*(float*)val1 < *(float*)val2){
            return -1;
        }else if(*(float*)val1 > *(float*)val2){
            return 1;
        }else{
            return 0;
        }
    }
    //if we get here we know its varchar type.
}

NodeHeader IndexManager::getNodeHeader(void * node)
{
    // Getting the slot directory header.
    NodeHeader header;
    memcpy (&header, node, sizeof(NodeHeader));
    return header;
}

void IndexManager::setNodeHeader(NodeHeader header, void * node)
{
    memcpy (node, &header, sizeof(NodeHeader));
}

LeafEntry IndexManager::getLeafEntry(void * page, unsigned entryNumber)
{
    // Getting the slot directory entry data.
    LeafEntry lEntry;
    memcpy  (
            &lEntry,
            ((char*) page + sizeof(NodeHeader) + entryNumber * sizeof(LeafEntry)),
            sizeof(LeafEntry)
            );

    return lEntry;
}

void IndexManager::setLeafEntry(void * page, unsigned entryNumber, LeafEntry lEntry)
{
    memcpy  (
            ((char*) page + sizeof(NodeHeader) + entryNumber * sizeof(LeafEntry)),
            &lEntry,
            sizeof(LeafEntry)
            );
}

NonLeafEntry IndexManager::getNonLeafEntry(void * page, unsigned entryNumber)
{
    // Getting the slot directory entry data.
    NonLeafEntry nEntry;
    memcpy  (
            &nEntry,
            ((char*) page + sizeof(NodeHeader) + entryNumber * sizeof(NonLeafEntry)),
            sizeof(NonLeafEntry)
            );

    return nEntry;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}

