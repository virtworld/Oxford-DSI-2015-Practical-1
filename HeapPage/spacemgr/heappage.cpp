//Database System Implementation Practical 1
// Candidate Number: 589087
#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "../include/heappage.h"
#include "../include/heapfile.h"
#include "../include/bufmgr.h"
#include "../include/db.h"

using namespace std;

//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------

void HeapPage::Init(PageID pageNo)
{
  this->pid = pageNo;
  this->nextPage = INVALID_PAGE;
  this->prevPage = INVALID_PAGE;
  
  this->freeSpace = HEAPPAGE_DATA_SIZE;
  this->numOfSlots = 0;
  this->fillPtr = HEAPPAGE_DATA_SIZE;
  SLOT_SET_EMPTY( slots[0]);
}

void HeapPage::SetNextPage(PageID pageNo)
{
  this->nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
  this->prevPage = pageNo;
}

PageID HeapPage::GetNextPage()
{
  return nextPage;
}

PageID HeapPage::GetPrevPage()
{
  return prevPage;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist
//------------------------------------------------------------------

Status HeapPage::InsertRecord(char *recPtr, int length, RecordID& rid)
{
 
  int slotIndex = 0;
  bool emptySlotExist = false;

  // Find available slot
  while( slotIndex < numOfSlots){
    if( SLOT_IS_EMPTY( slots[slotIndex])){
      emptySlotExist = true;
      break;
    }
    ++slotIndex;
  }

  if( emptySlotExist){
    // Not enough space
    if( freeSpace < length) return DONE;
    // fill the slot
    SLOT_FILL( slots[slotIndex], fillPtr - length, length);
  }
  else {
    // Not enough space
    if( freeSpace < length + sizeof(Slot)) return DONE; 
    // fill the slot
    SLOT_FILL( slots[slotIndex], fillPtr - length, length);
    freeSpace -= sizeof(Slot);
    ++numOfSlots;
  }

  // copy record to data area
  memcpy( &(data[fillPtr - length]), recPtr, length);
  freeSpace -= length; // decrease free space
  fillPtr -= length; // move pointer of the free speace
  
  rid.pageNo = pid;
  rid.slotNo = slotIndex;
  
  return OK;
}


//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(const RecordID& rid)
{
  // input slot number greater than the number of slots
  if( rid.slotNo >= numOfSlots) return FAIL;
  // input slot is empty
  if( SLOT_IS_EMPTY( slots[rid.slotNo])) return FAIL;

  // Optional: compact records
  short delSlotOffset = slots[rid.slotNo].offset;
  short delSlotLength = slots[rid.slotNo].length;
  // move affected records forward.
  memmove( &(data[fillPtr + delSlotLength]), &(data[fillPtr]), 
    delSlotOffset - fillPtr);
  // scan to update slots
  int slotIndex = 0;
  while(slotIndex < numOfSlots){
    // update slots that are not empty and affected by deletion
    if( !SLOT_IS_EMPTY(slots[slotIndex]) && 
      (slots[slotIndex].offset < delSlotOffset)){
      slots[slotIndex].offset += delSlotLength;
    }

    slotIndex++;
  }
  // End of optional compact

  // if slot is the last one, remove it
  if( rid.slotNo == numOfSlots - 1){
    numOfSlots--;
    freeSpace += sizeof( Slot);
  }

  // mark slot as invalid
  SLOT_SET_EMPTY(slots[rid.slotNo]);
	return OK;
}


//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
  if(IsEmpty()) return DONE;

  // loop to skip free slots
  int slotIndex = 0;
  while( slotIndex < numOfSlots){
    if( !SLOT_IS_EMPTY(slots[slotIndex])){
      rid.pageNo = pid;
      rid.slotNo = slotIndex;    
      return OK;
    }
    slotIndex++;
  }

  return DONE;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK
//------------------------------------------------------------------

Status HeapPage::NextRecord (RecordID curRid, RecordID& nextRid)
{
  if( curRid.slotNo >= numOfSlots){
    return DONE; // impossible slot number
  } 

  int curRidSlotNo = curRid.slotNo;
  while( curRidSlotNo < numOfSlots){
    curRidSlotNo++; // the next slot number
    if( curRidSlotNo == numOfSlots) return DONE; // reach the end of slots
    if( SLOT_IS_EMPTY( slots[curRidSlotNo])) continue; // skip free slot
    nextRid.pageNo = pid; // find next slot, set pageNo and slotNo
    nextRid.slotNo = curRidSlotNo;
    return OK;
  }
}


//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
  if( rid.slotNo >= numOfSlots) return FAIL; // impossible slot number

  length = slots[rid.slotNo].length; // get length of record
  int offset = slots[rid.slotNo].offset; // get offset
  memcpy( recPtr, &(data[offset]), length); // make a copy
  return OK;
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{
  if( rid.slotNo > numOfSlots) return FAIL; // impossible slot number

  length = slots[rid.slotNo].length; // get length of record
  int offset = slots[rid.slotNo].offset; // get offset 
  recPtr = &data[offset]; // assign the pointer 
  return OK;
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace(void)
{
  // if numOfSlots > number of records, then there's free slot.
  return (numOfSlots > GetNumOfRecords()) ? freeSpace : freeSpace - sizeof(Slot);
}


//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

bool HeapPage::IsEmpty(void)
{
  return GetNumOfRecords() == 0;
}


//------------------------------------------------------------------
// Test: insert 10 records and then delete 5 non-last records. 
// Get freeSpace A, call CompactSlotDir(), then get freeSpace B.
// test succeeds if A - B = 5 * sizeof(Slot), otherwise, test fails.
//------------------------------------------------------------------

void HeapPage::CompactSlotDir()
{
  int numberOfInvalidSlots = 0;
  
  for(int i = 0; i < numOfSlots; i++){
    if( SLOT_IS_EMPTY( slots[i])){
      numberOfInvalidSlots++; // increment counter if a free slot found
    }else{
      // move slot forward
      slots[i - numberOfInvalidSlots].offset = slots[i].offset; 
      slots[i - numberOfInvalidSlots].length = slots[i].length;
    }
  }

  numOfSlots -= numberOfInvalidSlots; // reduce the number of slots
  freeSpace += sizeof(Slot) * numberOfInvalidSlots; // increase free space
}

int HeapPage::GetNumOfRecords()
{
  int slotIndex = 0;
  int validSlotCount = 0;

  while( slotIndex < numOfSlots){
    if( !SLOT_IS_EMPTY( slots[slotIndex])) {
      validSlotCount++; // only count for valid slot
    }
    slotIndex++;
  }

  return validSlotCount;
}
