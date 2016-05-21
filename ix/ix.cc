#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "ix.h"
#include <stdlib.h>
#include <string.h>
#include <iostream>

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
		return 1; //bad pfm file creation;
	}
	
	return 0;
}

/*
 * Create a leaf node with an initial value in it
 */
RC IndexManager::createLeaf(IXFileHandle &ixfileHandle, const RID &rid, const void *key, unsigned &pageNumber, const Attribute &attribute)
{
    NodeHeader leafHeader;
    int keySize = getKeySize(key, attribute);
    leafHeader.freeSpaceOffset = PAGE_SIZE - keySize;
    leafHeader.numEntries = 1;
    leafHeader.isLeaf = true;
    leafHeader.nextNode = NONODE;
    leafHeader.previousNode = NONODE;

    void * leafPage = malloc(PAGE_SIZE);
    memset(leafPage, 0, PAGE_SIZE);
    memcpy((char*)leafPage+leafHeader.freeSpaceOffset, key, keySize);
    setNodeHeader(leafHeader, leafPage);
    LeafEntry entry;
    entry.rid = rid;
    entry.offSet = leafHeader.freeSpaceOffset;
    setLeafEntry(leafPage, 0, entry);
    ixfileHandle.appendPage(leafPage);
    pageNumber = ixfileHandle.getNumberOfPages()-1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    cout << "HELP" << endl;
	int err;
	err = _pf_manager->destroyFile(fileName);
    cout << "NOT WORKING" << endl;
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
    cout << "HELLO WORLD" << endl;
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
    cout << endl <<"number of pages : " << ixfileHandle.getNumberOfPages()<<endl;
    // DOES NOT HANDLE FULL NODES
    // still needs to handle insertion of key into page & decrement freeSpace offset
    LeafEntry to_insert;
    to_insert.rid = rid;

    //start search from root
    unsigned* parentNum = (unsigned*)malloc(sizeof(int));
    int nodeNum = searchTree(ixfileHandle, key, attribute, 0, *parentNum);
    cout << nodeNum <<endl;
    void * node = malloc(PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);

    //tree was empty, search returned null
    /*
     * So the tree currently has no leaves, and we can presume that the tree is empty.
     * Lets add our first leaf.
     */
    if(nodeNum == NONODE){
        cout << "First insertion - building tree" <<endl;
        void * rootPage = malloc(PAGE_SIZE);
        memset(rootPage, 0, PAGE_SIZE);
        NodeHeader rootHeader;
        rootHeader.isLeaf = false;
        rootHeader.freeSpaceOffset = PAGE_SIZE;
        rootHeader.nextNode = NONODE;
        rootHeader.previousNode = NONODE;
        rootHeader.numEntries = 0;
        setNodeHeader(rootHeader, rootPage);
        ixfileHandle.appendPage(rootPage);

        unsigned newPageNumber;
        createLeaf(ixfileHandle, rid, key, newPageNumber, attribute);
        void * root = malloc(PAGE_SIZE);
        memset(root, 0, PAGE_SIZE);
        // The first page is the root node
        ixfileHandle.readPage(0, root);
        NonLeafEntry entry;

        int keySize = getKeySize(key, attribute);
        entry.offset = PAGE_SIZE - keySize;
        memcpy((char*)root+entry.offset, key, keySize);
        entry.greaterThanNode = newPageNumber;
        entry.lessThanNode = NONODE;
        NodeHeader header = getNodeHeader(root);
        header.numEntries += 1;
        header.freeSpaceOffset -= keySize;
        setNodeHeader(header, root);
        setNonLeafEntry(root, 0, entry);
        ixfileHandle.writePage(0, root);
        return 0;
    }
    if(nodeNum == NEWLESSTHANNODE){
        unsigned newPageNumber;
        createLeaf(ixfileHandle, rid, key, newPageNumber, attribute);
        //now point the parent at the new leaf
        void * parentNode = malloc(PAGE_SIZE);
        memset(parentNode, 0, PAGE_SIZE);        
        ixfileHandle.readPage((int)*parentNum, parentNode);
        NonLeafEntry nle = getNonLeafEntry(parentNode, 0);//we know its the leftmost/smallest entry
        nle.lessThanNode = (int)newPageNumber;
        setNonLeafEntry(parentNode, 0, nle);
        //now point sibling nodes at each other
        //first get right siblings nodenum
        int rightNodeNum = nle.greaterThanNode;

        //now load the nodes
        void * leftNode = malloc(PAGE_SIZE);
        memset(leftNode, 0, PAGE_SIZE);        
        ixfileHandle.readPage((int)newPageNumber, leftNode);
        void * rightNode = malloc(PAGE_SIZE);
        memset(rightNode, 0, PAGE_SIZE);        
        ixfileHandle.readPage(rightNodeNum, rightNode);
        //get the headers
        NodeHeader lh = getNodeHeader(leftNode);
        NodeHeader rh = getNodeHeader(rightNode);
        //set the pointers :)
        lh.nextNode = rightNodeNum;
        rh.previousNode = (int)newPageNumber;
        //write them back to page
        setNodeHeader(lh, leftNode);
        setNodeHeader(rh, rightNode);
        //write to disk
        ixfileHandle.writePage((int)*parentNum, parentNode);
        ixfileHandle.writePage(rightNodeNum, rightNode);
        ixfileHandle.writePage((int)newPageNumber, leftNode);
        return SUCCESS;
    }
    cout << "Existing Tree" <<endl;
    ixfileHandle.readPage(nodeNum, node);

    //we now have proper node. Search through sorted record & insert at proper location
    //get number of entries
    NodeHeader header = getNodeHeader(node);
    int keySize = getKeySize(key, attribute);
    int new_offset = header.freeSpaceOffset - keySize;
    if(freeSpaceStart(node)+sizeof(LeafEntry) > new_offset){
        cout << "WE have to split"<<endl;
        //No space to insert. We have to split :(
        void * parentNode = malloc(PAGE_SIZE);
        memset(parentNode, 0, PAGE_SIZE);
        ixfileHandle.readPage(*parentNum, parentNode);

	void * newNode = malloc(PAGE_SIZE);
        memset(newNode, 0, PAGE_SIZE);
        //ixfileHandle.readPage(*newNum, newNode);
        
	void * rightNode = malloc(PAGE_SIZE);
        memset(newNode, 0, PAGE_SIZE);
        //ixfileHandle.readPage(*rightNum, rightNode);

	NodeHeader parentHeader = getNodeHeader(parentNode);
	NodeHeader newHeader = getNodeHeader(newNode);
	NodeHeader rightHeader = getNodeHeader(rightNode);
	
	LeafEntry bubbleEntry = getLeafEntry(node, header.numEntries/2);
	NonLeafEntry bubEntry;
	for(int i = header.numEntries/2; i < header.numEntries; i++)
	{
		LeafEntry shiftEntry = getLeafEntry(node, i);
		insertEntry(ixfileHandle, attribute, getValue(node, shiftEntry.offSet, attribute), shiftEntry.rid);
	}	
	//delete back half of node entry
    }
    to_insert.offSet = new_offset;

    //iterate over entriesTree
    cout << "number entries " << header.numEntries << endl;
    cout << "iterating. On entry number: ";
    for(int i = 0; i<header.numEntries; i++){
        cout << i;
        LeafEntry entry = getLeafEntry(node, i);
        void *val2 = getValue(node, entry.offSet, attribute);
        //  compare value to entry
        if(compareVals(key, val2, attribute) < 0){
            memcpy((char*)node+new_offset, key, keySize);
            //make space for new entry
            moveEntries(node, i, header);
            // insert at old entries offset
            setLeafEntry(node, i, to_insert);
            // update node header
            header.numEntries++;
            setNodeHeader(header, node);
            ixfileHandle.writePage(nodeNum, node);
            return SUCCESS;
        }
    }
    memcpy((char*)node+new_offset, key, keySize);
    // insert at old entries offset
    setLeafEntry(node, header.numEntries, to_insert);
    // update node header
    header.numEntries++;
    setNodeHeader(header, node);
    ixfileHandle.writePage(nodeNum, node);
    return SUCCESS;
}

int IndexManager::getKeySize(const void * key, const Attribute &attribute)
{
    if(attribute.type == TypeVarChar){
        int * strLen = (int*)malloc(sizeof(int));
        memcpy(strLen, key, sizeof(int));
        return *strLen;
    }
    return sizeof(int);
}
int IndexManager::freeSpaceStart(void *page)
{
    NodeHeader header = getNodeHeader(page);
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

    //start search from root
    void * node = malloc(PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);
    unsigned* parentNum = (unsigned*)malloc(sizeof(int));
    int nodeNum = searchTree(ixfileHandle, key, attribute, 0, *parentNum);

    ixfileHandle.readPage(nodeNum, node);

    NodeHeader header = getNodeHeader(node);
    //iterate over entriesTree
    for(int i = 0; i<header.numEntries; i++){
        LeafEntry entry = getLeafEntry(node, i);
        //  compare value to entry
        if(entry.rid.pageNum != rid.pageNum || entry.rid.slotNum != rid.slotNum){
            continue;
        }else{
            deleteLeafEntry(node, i, header);
        }
    }
    ixfileHandle.writePage(nodeNum, node);
    return 0;
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
        LeafEntry entry = getLeafEntry(page, i);	//removed _index_manager after moving getLeafEntry out of the class
        entry.offSet -= sizeof(LeafEntry);
        setLeafEntry(page, j, entry);
    }
}

void IndexManager::deleteLeafEntry(void * page, int i, NodeHeader header)
{
    //make space for new entry!
    int current_offset = sizeof(NodeHeader) + sizeof(LeafEntry)*i;
    int desired_offset = current_offset - sizeof(LeafEntry);
    //how many entries after the current position we have to move
    int number_to_move = header.numEntries-i;
    int size = number_to_move * sizeof(LeafEntry);
    memmove((char*)page+desired_offset, (char*)page+current_offset, size);

    //save entries new positions!
    header.numEntries --;
    for(int j = i; j < header.numEntries; j++){
        LeafEntry entry = getLeafEntry(page, i);    //removed _index_manager after moving getLeafEntry out of the class
        entry.offSet += sizeof(LeafEntry);
        setLeafEntry(page, j, entry);
    }
}

/*
 * Search the index tree
 * Given an attribute and value, return the leaf node (note: not entry!) corresponding to the keyed value
*/
int IndexManager::searchTree(IXFileHandle &ixfileHandle, const void* value, const Attribute &attribute, int nodeNum, unsigned &parentNodeNumber)
{
    // https://en.wikipedia.org/wiki/B%2B_tree#Search
    if(ixfileHandle.getNumberOfPages() == 0){
        return -1;
    }
    void * node = malloc(PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);
    //always start at root!
    ixfileHandle.readPage(nodeNum, node);
    cout << "searchTree 1.0" << endl;
    NodeHeader header = getNodeHeader(node);
    if(header.isLeaf){
        return nodeNum;
    }
    cout << "searchTree 2" << endl;
    parentNodeNumber = nodeNum;
    //not leaf so we need to find what node to find next 
    for(int i = 0; i < header.numEntries; i++){
        //will find all nodes iff value < max value on page
        cout << "searchTree 2.01" << endl;
        NonLeafEntry entry = getNonLeafEntry(node, i);
        void * min_val = getValue(node, entry.offset, attribute);
        if(compareVals(value, min_val, attribute) < 0){
            if(entry.lessThanNode != NONODE){
                return searchTree(ixfileHandle, value, attribute, entry.lessThanNode, parentNodeNumber);
            }else{
                return NEWLESSTHANNODE;
            }
        }
    }
    //if we get here we know there is only one place to search
    NonLeafEntry entry = getNonLeafEntry(node, header.numEntries-1);
    cout << "searchTree 3" << endl;
    return searchTree(ixfileHandle, value, attribute, entry.greaterThanNode, parentNodeNumber);
}

int IndexManager::getMostLeftLeafNumber(IXFileHandle &ixfileHandle){
    if(ixfileHandle.getNumberOfPages() == 0){
        return -1;
    }
    void * node = malloc(PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);
    //always start at root!
    ixfileHandle.readPage(0, node);
    NodeHeader header = getNodeHeader(node);
    if(header.isLeaf){
        return -1;
    }
    NonLeafEntry entry = getNonLeafEntry(node, 0);
    int nodeNum = entry.lessThanNode;
    if(nodeNum == NONODE)
        nodeNum = entry.greaterThanNode;
    void * tempNode = malloc(PAGE_SIZE);
    while(true){
        memset(tempNode, 0, PAGE_SIZE);
        ixfileHandle.readPage(nodeNum, tempNode);
        header = getNodeHeader(tempNode);
        if(header.isLeaf)
            break;
        else{
            entry = getNonLeafEntry(tempNode, 0);
            nodeNum = entry.lessThanNode;
            if(nodeNum == NONODE)
                nodeNum = entry.greaterThanNode;
        }
    }
    free(tempNode);
    return nodeNum;
}

int IndexManager::getMostRightLeafNumber(IXFileHandle &ixfileHandle){
    if(ixfileHandle.getNumberOfPages() == 0){
        return -1;
    }
    void * node = malloc(PAGE_SIZE);
    memset(node, 0, PAGE_SIZE);
    //always start at root!
    ixfileHandle.readPage(0, node);
    NodeHeader header = getNodeHeader(node);
    if(header.isLeaf){
        return -1;
    }
    NonLeafEntry entry = getNonLeafEntry(node, header.numEntries-1);
    int nodeNum = entry.greaterThanNode;
    if(nodeNum == NONODE)
        nodeNum = entry.lessThanNode;
    void * tempNode = malloc(PAGE_SIZE);
    while(true){
        memset(tempNode, 0, PAGE_SIZE);
        ixfileHandle.readPage(nodeNum, tempNode);
        header = getNodeHeader(tempNode);
        if(header.isLeaf)
            break;
        else{
            entry = getNonLeafEntry(tempNode, header.numEntries-1);
            nodeNum = entry.greaterThanNode;
            if(nodeNum == NONODE)
                nodeNum = entry.greaterThanNode;
        }
    }
    free(tempNode);
    return nodeNum;
}

/*
 * Get a value from a node given the node and the offset in bytes
*/
	void* getValue(void * node, int offset, const Attribute &attribute)
	{
        cout << "getValue 1" << endl;
	    void * value;
	    int size = 0;
	    if(attribute.type != TypeVarChar){
		size = sizeof(int);
	    }
	    else
	    {
		size = *(int*)((char*)(node)+offset);
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
        cout << "getValue 2" << endl;
	    return value;
	}

	/*
	 * Compare two values given an attribute type
	*/
	int compareVals(const void * val1, void * val2, const Attribute &attribute)
	{
        if(val1 == NULL && val2 == NULL){
            return 0;
        }
        if(val1 == NULL && val2 != NULL){
            return -1;
        }
        if(val1 != NULL && val2 == NULL){
            return 1;
        }
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
		}else
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
	    //return 3; //fell through entire function
	}

/*
 * Get the header for a node (Note: not entry!)
*/
NodeHeader getNodeHeader(const void * node)
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
LeafEntry getLeafEntry(void * page, unsigned entryNumber)
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

NonLeafEntry getNonLeafEntry(void * page, unsigned entryNumber)
{
    // Getting the slot directory entry data.
    cout << "Get non leaf entry 1" << endl;
    NonLeafEntry nEntry;
    memcpy  (
            &nEntry,
            ((char*) page + sizeof(NodeHeader) + entryNumber * sizeof(NonLeafEntry)),
            sizeof(NonLeafEntry)
            );
    cout << "Get non leaf entry 2" << endl;
    return nEntry;
}

void IndexManager::setNonLeafEntry(void * page, unsigned entryNumber, NonLeafEntry nEntry)
{
    memcpy  (
            ((char*) page + sizeof(NodeHeader) + entryNumber * sizeof(NonLeafEntry)),
            &nEntry,
            sizeof(NonLeafEntry)
            );
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
        bool		lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    unsigned parentNum;
    // Check if a file is attached to filehandle
    if(ixfileHandle.getNumberOfPages() == 0 || ixfileHandle.getNumberOfPages() == -1){
        return -1;
    }
    cout << "scan: 1" << endl;
    int nodeNum;
    if(lowKey == NULL){
        cout << "scan: 1.01" << endl;
        nodeNum = getMostLeftLeafNumber(ixfileHandle);
    } else{
        nodeNum = searchTree(ixfileHandle, lowKey, attribute, 0, parentNum);
    }
    ix_ScanIterator.currentNode = nodeNum;
    
    cout << "scan: 2" << endl;
    
    if(highKey == NULL){
        cout << "scan: 2.01" << endl;
        nodeNum = getMostRightLeafNumber(ixfileHandle);
    } else{
        nodeNum = searchTree(ixfileHandle, highKey, attribute, 0, parentNum);
    }
    cout << "scan: 2.1" << endl;
    ix_ScanIterator.endNode = nodeNum;
    ix_ScanIterator.ixfileHandle = &ixfileHandle;
    ix_ScanIterator.startFlag = 1;
    ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    cout << "scan: 3" << endl;
    int size;
	if (attribute.type != TypeVarChar)
	{
		size = sizeof(int);
	}
	else
	{
		size = sizeof(int) + *(int*)lowKey;
	}
    if(lowKey == NULL)
        ix_ScanIterator.lowKey = NULL;
    else{
        ix_ScanIterator.lowKey = malloc(size);
    	memcpy(ix_ScanIterator.lowKey, lowKey, size);
    }
    cout << "scan: 4" << endl;
	if (attribute.type != TypeVarChar)
	{
		size = sizeof(int);
	}
	else
	{
		size = sizeof(int) + *(int*)highKey;
	}
    if(lowKey == NULL)
        ix_ScanIterator.lowKey = NULL;
    else{
        ix_ScanIterator.highKey = malloc(size);
        memcpy(ix_ScanIterator.highKey, highKey, size);
    }
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {

		
	static int depth = 0;	
	void* page = malloc(PAGE_SIZE);
	if(depth == 0)
	{
		ixfileHandle.readPage(0, page);
	}
    	NodeHeader header = getNodeHeader(page);
    	if(header.isLeaf)
	{
    		cout<<"{\"keys\": [";
    		for(int i = 0; i<header.numEntries; i++)
		{
			cout<<"\"";
			//print entry name/entry rid
        		LeafEntry entry = getLeafEntry(page, i);
			printValue((char*)page+entry.offSet, attribute);
			cout<<": [";
			//
			cout<<"("<<entry.rid.pageNum<<","<<entry.rid.slotNum<<")";
			LeafEntry nextEntry = getLeafEntry(page, i + 1);
			while(compareVals(getValue(page, entry.offSet, attribute), getValue(page,nextEntry.offSet, attribute), attribute) == 0)
			{
				cout<<"("<<nextEntry.rid.pageNum<<","<<nextEntry.rid.slotNum<<")";
				i++;
				nextEntry = getLeafEntry(page, i + 1);
			}
			cout<<"]";
			//
			cout<<"\"";
			if (header.numEntries - 1 != i)
			{
				cout<<",";
			}		
		}
		cout<<"]}";
    	}
	else if(!header.isLeaf)
	{
    		cout<<"{\"keys\": [";
		for (int i = 0; i < header.numEntries; i++ )
		{
			cout<<"\"";
			//print entry name/entry rid
        		NonLeafEntry entry = getNonLeafEntry(page, i);
			printValue((void*)((char*)page+entry.offset), attribute);		
			cout<<"\"";
			if (header.numEntries - 1 != i)
			{
				cout<<",";
			}		
		}
		cout<<"] , \n \"children\": [ \n";
		depth++;
		for (int i = 0; i < header.numEntries; i++ )
		{
			cout<<"\"";
			//print entry name/entry rid
        		NonLeafEntry entry = getNonLeafEntry(page, i);
			if(entry.lessThanNode != -1)
				printRecur(ixfileHandle, entry.lessThanNode, attribute);
		}
		NonLeafEntry entry = getNonLeafEntry(page, header.numEntries-1);
		if(entry.greaterThanNode != -1)
			printRecur(ixfileHandle, entry.greaterThanNode, attribute);
		depth--;
		cout<<"]} \n";	
	}
/*typedef struct NonLeafEntry
{
    int offset; //offset to value. can't store directly b/c don't know data type
    int lessThanNode; //page num to child node containing entries<value at offset
    int greaterThanNode; //page num to child node containing entries>value at offset
} NonLeafEntry;
*/
}

void IndexManager::printRecur(IXFileHandle ixfileHandle, int pageNum, const Attribute& attribute) const
{
	void* page = malloc(PAGE_SIZE);
	ixfileHandle.readPage(pageNum, page);
    	NodeHeader header = getNodeHeader(page);
    	if(header.isLeaf)
	{
    		cout<<"{\"keys\": [";
    		for(int i = 0; i<header.numEntries; i++)
		{
			cout<<"\"";
			//print entry name/entry rid
        		LeafEntry entry = getLeafEntry(page, i);
			printValue((void*)((char*)page+entry.offSet), attribute);
			cout<<": [";
			//
			cout<<"("<<entry.rid.pageNum<<","<<entry.rid.slotNum<<")";
			if(header.numEntries -1 > i)
			{
				LeafEntry nextEntry = getLeafEntry(page, i + 1);
				while(compareVals(getValue(page, entry.offSet, attribute), getValue(page,nextEntry.offSet, attribute), attribute) == 0)
				{
					cout<<"("<<nextEntry.rid.pageNum<<","<<nextEntry.rid.slotNum<<")";
					i++;
					nextEntry = getLeafEntry(page, i + 1);
				}
			}
			cout<<"]";
			//
			cout<<"\"";
			if (header.numEntries - 1 != i)
			{
				cout<<",";
			}		
		}
		cout<<"]} \n";
    	}
	else if(!header.isLeaf)
	{
    		cout<<"{\"keys\": [";
		for (int i = 0; i < header.numEntries; i++ )
		{
			cout<<"\"";
			//print entry name/entry rid
        		NonLeafEntry entry = getNonLeafEntry(page, i);
			printValue((void*)((char*)page+entry.offset), attribute);		
			cout<<"\"";
			if (header.numEntries - 1 != i)
			{
				cout<<",";
			}		
		}
		cout<<"] , \n \"children\": [ \n ";
		for (int i = 0; i < header.numEntries; i++ )
		{
			cout<<"\"";
			//print entry name/entry rid
        		NonLeafEntry entry = getNonLeafEntry(page, i);
			IXFileHandle recurHandle;
			if(entry.lessThanNode != -1)
				printRecur(ixfileHandle, entry.lessThanNode, attribute);
		}
		NonLeafEntry entry = getNonLeafEntry(page, header.numEntries-1);
		if(entry.greaterThanNode != -1)
			printRecur(ixfileHandle, entry.greaterThanNode, attribute);
		cout<<"]}";	
	}
}

void IndexManager::printValue(void* data, const Attribute &attribute) const
{
	
	switch (attribute.type)
	{
		case TypeInt :
		{
			int valueI = *(int*)data;
			cout<<valueI;
			break;
		}
		case TypeReal :
		{
			float valueF = *(float*)data;
			cout<<valueF;
			break; 
		}
		case TypeVarChar:
		{
			int size = *(int*)data;
			string valueVC;
			valueVC.assign((char*)data+sizeof(int), size);		
			cout<<valueVC;
			break;
		}
	}
}

IX_ScanIterator::IX_ScanIterator()
{
    currentNode = NULL;
    endNode = NULL;
    startFlag = 0;
    done = false;
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
    if(done)
        return IX_EOF;
    cout << "currentNode: " << currentNode << endl;
    void *startNode = malloc(PAGE_SIZE);
    memset(startNode, 0, PAGE_SIZE);
    ixfileHandle->readPage(currentNode, startNode);

    void *eNode = malloc(PAGE_SIZE);
    memset(eNode, 0, PAGE_SIZE);
    ixfileHandle->readPage(endNode, eNode);

    NodeHeader header = getNodeHeader(startNode);
    // Is this the last node?
    int lastNode = (currentNode == endNode);	//for clarity, should be bool	
    cout << "lastNode: " << lastNode << endl;
    /*
     * If this is the starting node, we're going to need to find the
     * first appropriate node. This logic tree gets a big convoluted
     * because we repeat a bit based on inclusives or exclusives.
     * Probably could factor some of this code to be more efficient, but oh well
     * TODO: Get next leaf using integer, not pointer
     */
    if(startFlag){
        int i;
        for(i = 0; i < header.numEntries; i++){
            LeafEntry leaf = getLeafEntry(startNode, i);	
            void *entryValue = getValue(startNode, leaf.offSet, attribute);
            // Double check me!
            // Remember to account for inclusives and exclusives!
            if(lowKeyInclusive){
                if(compareVals(lowKey, entryValue, attribute) >= 0){
                    if(highKeyInclusive){
                        if(compareVals(highKey, entryValue, attribute) <= 0){
                            // Great, got our first value
                            rid = leaf.rid;
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
                            rid = leaf.rid;
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
        if(++currentEntryNumber >= header.numEntries - 1){
            // If we're on the last node and our return values are null, we return EOF
            if(lastNode){
                done = true;
                cout << "WE TRUE DAWG" << endl;
            }
            currentEntryNumber = 0;
            currentNode = header.nextNode;
        }
        startFlag = 0;
        cout << rid.pageNum << endl;
        cout << "getnextryentry 1" << endl;
        return 0;
    }
    // Great, we already have our currentEntryNumber set with our current node.
    LeafEntry leaf = getLeafEntry(startNode, currentEntryNumber);
    void *entryValue = getValue(startNode, leaf.offSet, attribute);
    if(highKeyInclusive){
        if(compareVals(highKey, entryValue, attribute) <= 0){
            rid = leaf.rid;
            key = entryValue;
        } else{
            return IX_EOF;
        }
    } else{
        if(compareVals(highKey, entryValue, attribute) < 0){
            rid = leaf.rid;
            key = entryValue;
        } else{
            return IX_EOF;
        }
    }
    // iterate the currentEntryNumber
    // Could be a bug here with reaching the last value in a node
    if(++currentEntryNumber >= header.numEntries - 1){
        // If we're on the last node and our return values are null, we return EOF
        if(lastNode){
            done = true;
            cout << "WE TRUE DAWG2" << endl;
        }
        currentEntryNumber = 0;
        currentNode = header.nextNode;
    }
    cout << "Getnextentry 2" << endl;
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

RC IXFileHandle::readPage(PageNum pageNum, void *data){
    FileHandle::readPage(pageNum, data);
    ixReadPageCounter++;
    return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
    FileHandle::writePage(pageNum, data);
    ixWritePageCounter++;
    return 0;
}

RC IXFileHandle::appendPage(const void *data){
    FileHandle::appendPage(data);
    ixAppendPageCounter++;
    return 0;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;

    return 0;
}

