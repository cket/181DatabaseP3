#include "../rbf/pfm.h"
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
	
	err = _pf_manager->createFile(fileName);
	if (err != 0)
	{
		return 1; //bad file creation;
	}
	
	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
    _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    _pf_manager->openFile(fileName, ixfileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    _pf_manager->closeFile(ixfileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    
    num_entries = header.numEntries;
    //ifiterate over entries
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

void* IndexManager::search(void* value, const Attribute &attribute, int nodeNum=0)
{
    // https://en.wikipedia.org/wiki/B%2B_tree#Search
    void * node = malloc(PAGE_SIZE);
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
        void * min_val = getValue(entry.offset);
        if(compareVals(value, min_val, attribute) < 0){
            return search(value, attribute, entry.lessThanNode)
        }
    }
    //if we get here we know there is only one place to search
    NonLeafEntry entry = getNonLeafEntry(node, header.numEntries-1);
    return search(value, attribute, entry.greaterThanNode);
}

void* getValue(void * node, int offset, const Attribute &attribute)
{
    void * value;
    int size = 0;
    if(attribute.type != VarCharType){
        size = sizeof(int);
    }
    //need to handle varchars still
    value = malloc(size);
    memcpy(value, node+offset, size);
}

int compareVals(void * val1, void * val2, const Attribute &attribute)
{
    if(attribute.type == IntType){
        if(*(int*)val1 < *(int*)val2){
            return -1;
        }else if(*(int*)val1 > *(int*)val2){
            return 1;
        }else{
            return 0;
        }
    }
    if(attribute.type == FloatType){
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

