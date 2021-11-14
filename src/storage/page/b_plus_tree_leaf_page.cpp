//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <common/logger.h>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType(1));
  SetNextPageId(INVALID_PAGE_ID);
  LOG_INFO("LEAFPAGESIZE %lu",LEAF_PAGE_SIZE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    int cmp_res = comparator(array[i].first, key);
    if (cmp_res == 0 || cmp_res == 1) {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  // copy init?
  KeyType key{array[index].first};
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {

  int insert_index;
  for (insert_index = 0; insert_index < GetSize(); ++insert_index) {
    int cmp_res = comparator(key, array[insert_index].first);
    if (cmp_res == -1) {
      break;
    }
  }

  for (int i = GetSize(); i > insert_index; --i) {
    array[i] = array[i - 1];
  }
  array[insert_index] = MappingType{key, value};
  IncreaseSize(1);
  LOG_INFO("insert in %d,after insert size %d",insert_index,GetSize());
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyNFrom(array + GetSize() / 2, GetSize() - GetSize() / 2);
  SetSize(GetSize() / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int copy_location = GetSize();
  for (int i = 0; i < size; ++i) {
    array[copy_location + i] = items[i];
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int key_index = KeyIndex(key, comparator);
  if (comparator(KeyAt(key_index), key) == 0) {
    if (value != nullptr) {
      *value = ValueType{array[key_index].second};
    }
    return true;
  } else {
    return false;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int delete_index = KeyIndex(key, comparator);
  if (delete_index==GetSize()||comparator(KeyAt(delete_index), key) != 0) {
    return GetSize();
  }
  LOG_INFO("delete index %d",delete_index);
  for (int i = delete_index; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  SetSize(GetSize() - 1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 * from right to left
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  recipient->CopyNFrom(array, GetSize());
  SetSize(0);
  if (recipient->GetNextPageId() == GetPageId()) {
    recipient->SetNextPageId(GetNextPageId());
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                  BufferPoolManager *buffer_pool_manager) {
  MappingType first_pair = array[0];
  for (int i = 0; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  SetSize(GetSize() - 1);
  recipient->CopyLastFrom(first_pair);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = MappingType{item};
  SetSize(GetSize() + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                   BufferPoolManager *buffer_pool_manager) {
  MappingType last_pair = array[GetSize() - 1];
  recipient->CopyFirstFrom(last_pair);
  SetSize(GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = GetSize(); i > 0; --i) {
    array[i] = array[i - 1];
  }
  array[0] = MappingType{item};
  SetSize(GetSize() + 1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
