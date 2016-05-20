#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "ix.h"
#include <stdlib.h>
#include <string.h>

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
int IndexManager::getKeySize(void *page, const void * key, const Attribute &attribute)
{
    return -1;
}
int IndexManager::freeSpaceStart(void *page)
{
    NodeHeader header = getNodeHeader(node);	//PROBLEM - where does node come from
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

/*
 * Move entries in the case where something needs to be inserted before it
 * or an entry was deleted before.
*/
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

/*
 * Search the index tree
 * Given an attribute and value, return the leaf node (note: not entry!) corresponding to the keyed value
*/
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

/*
 * Get a value from a node given the node and the offset in bytes
*/
void* getValue(void * node, int offset, const Attribute &attribute)
{
    void * value;
    int size = 0;
    if(attribute.type != TypeVarChar){
        size = sizeof(int);
    }
    else
    {
	size = (int*)((char*)(node)+offset);
    }
    //need to handle varchars still
    value = malloc(size);
    if (attribute.type != TypeVarChar)
    {
    	memcpy(value, (char*)node+offset, size);
    }
    else
    {
	memcpy(value, (char*)node+offset+sizeof(int), size);
    }
    return value;
}

/*
 * Compare two values given an attribute type
*/
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
    else
    {
        if(*(int*)val1 < *(int*)val2){
		return -2;	//val1 shorter
	}else if(*(int*)val1 > *(int*)val2){
		return 2;	//val1 longer
	else
	{
		int size = *(int*)val1;
		int comparison = strncmp((char*)val1, (char*)val2, size);
		if (comparison < 0)
		{
			return -1;	//val1 is "lower" in the alphabet
		}
		else if(comparison > 0)
		{
			return 1; 	//val1 is "higher" in the alphabet
		} 
		else if(comparison == 0)
		{
			return 0;	//val1 and val2 are the same.
		}
	}
    }
    return 3; //fell through entire function
}

/*
 * Get the header for a node (Note: not entry!)
*/
NodeHeader IndexManager::getNodeHeader(const void * node)
{
    // Getting the slot directory header.
    NodeHeader header;
    memcpy (&header, node, sizeof(NodeHeader));
    return header;
}

/*
 * Set the node header for a given node
*/
void IndexManager::setNodeHeader(NodeHeader header, void * node)
{
    memcpy (node, &header, sizeof(NodeHeader));
}

/*
 * Given a page (node?) and the entry number on that page, get a leaf entry value
 * Don't quite understand this one.
 */
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

/*
 * Scan the BTree.
 * Search for the first lowKey node. Then search for the last
 * high key node. Attach these to the ScanIterator, which will handle
 * actually returning the data. We're going to set a flag for the iterator
 * to find the first matching corresponding entry.
*/

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    IX_ScanIterator iterator;
    void *startNode = searchTree(ixfileHandle, lowKey, attribute, 0);
    void *endNode = searchTree(ixfileHandle, highKey, attribute, 0);
    iterator->currentNode = startNode;
    iterator->endNode = endNode;
    iterator->startFlag = 1;
    iterator->attribute = attribute;
    iterator->lowKeyInclusive = lowKeyInclusive;
    iterator->highKeyInclusive = highKeyInclusive;
    iterator->lowKey = lowKey;
    iterator->highKey = highKey;
    ix_ScanIterator = iterator;
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
    currentNode = NULL;
    endNode = NULL;
    startFlag = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
    currentNode = NULL;
    endNode = NULL;
}

/*
 * Get the next entry
 * Given a start node and an end node, point to the next entry matching the attribute
 * Because we are accessing memory addresses, when the currentNode address matches the
 * end node address, we've reached the final node.
 */
RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if(curentNode == NULL)
        return IX_EOF;
    NodeHeader header = getNodeHeader(currentNode);
    // Is this the last node?
    int lastNode = currentNode == endNode;

    /*
     * If this is the starting node, we're going to need to find the
     * first appropriate node. This logic tree gets a big convoluted
     * because we repeat a bit based on inclusives or exclusives.
     * Probably could factor some of this code to be more efficient, but oh well
     */
    if(startFlag){
        int i;
        for(i = 0; i < header.numEntries; i++){
            LeafEntry leaf = getLeafEntry(currentNode, i);
            void *entryValue = getValue(currentNode, leaf.offSet, attribute);
            // Double check me!
            // Remember to account for inclusives and exclusives!
            if(lowKeyInclusive){
                if(compareVals(lowKey, entryValue, attribute) >= 0){
                    if(highKeyInclusive){
                        if(compareVals(highKey, entryValue, attribute) <= 0){
                            // Great, got our first value
                            rid = leaf.rid
                            key = entryValue;
                        }
                    } else{
                        if(compareVals(highKey, entryValue, attribute) < 0){
                            // Great, got our first value
                            rid = leaf.rid;
                            key = entryValue;
                        }
                    }
                    break;
                }
            } else{
                if(compareVals(lowKey, entryValue, attribute) > 0){
                    if(highKeyInclusive){
                        if(compareVals(highKey, entryValue, attribute) <= 0){
                            // Great, got our first value
                            rid = leaf.rid
                            key = entryValue;
                        }
                    } else{
                        if(compareVals(highKey, entryValue, attribute) < 0){
                            // Great, got our first value
                            rid = leaf.rid;
                            key = entryValue;
                        }
                    }
                    break;
                }
            }
            
        }
        currentEntryNumber = i;
        // If we're past the number of entries in the node, go to the next node
        if(++currentEntryNumber == header.numEntries){
            // If we're on the last node and our return values are null, we return EOF
            if(lastNode && rid == NULL)
                return IX_EOF;
            currentEntryNumber = 0;
            currentNode = currentNode->nextNode;
        }
        startFlag = 0;
        return 0;
    }
    // Great, we already have our currentEntryNumber set with our current node.
    LeafEntry leaf = getLeafEntry(currentNode, currentEntryNumber);
    void *entryValue = getValue(currentNode, leaf.offSet, attribute);
    if(highKeyInclusive){
        if(compareVals(highKey, entryValue, attribute) <= 0){
            rid = leaf.rid
            key = entryValue;
        } else{
            return IX_EOF;
        }
    } else{
        if(compareVals(highKey, entryValue, attribute) < 0){
            rid = leaf.rid
            key = entryValue;
        } else{
            return IX_EOF;
        }
    }
    // iterate the currentEntryNumber
    // Could be a bug here with reaching the last value in a node
    if(++currentEntryNumber == header.numEntries){
        // If we're on the last node and our return values are null, we return EOF
        if(lastNode && rid == NULL)
            return IX_EOF;
        currentEntryNumber = 0;
        currentNode = currentNode->nextNode;
    }
    return 0;
}

RC IX_ScanIterator::close()
{
    currentNode = NULL;
    endNode = NULL;
    startFlag = 0;
    return 0;
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
    readPage = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

