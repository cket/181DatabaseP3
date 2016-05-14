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
    _pf_manager->createFile(fileName);
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
    void * page = malloc(PAGE_SIZE);
    //always start at root!
    ixfileHandle.readPage(0,page); 

    NodeHeader header = getNodeHeader(page);
    //
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

NodeHeader IndexManager::getNodeHeader(void * page)
{
    // Getting the slot directory header.
    NodeHeader header;
    memcpy (&header, page, sizeof(NodeHeader));
    return header;
}

void IndexManager::setNodeHeader(NodeHeader header, void * page)
{
    memcpy (page, &header, sizeof(NodeHeader));
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

