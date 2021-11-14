//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  Page *leaf_page = FindLeafPage(key, Operation::SEARCH);
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType query_res;
  bool exist = node->Lookup(key, &query_res, comparator_);
  UnlockAndUnpinPageOfNode(leaf_page, false, LockMode::READ);

  if (exist) {
    result->push_back(query_res);
  }
  return exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  latch_.lock();

  if (IsEmpty()) {
    StartNewTree(key, value);

    latch_.unlock();
    return true;
  } else {
    latch_.unlock();
    return InsertIntoLeaf(key, value);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (root_page == nullptr) {
    throw "out of memory";
  }

  LockPage(root_page, LockMode::WRITE);
  LOG_INFO("create lock page %d, insert key %d", root_page->GetPageId(), (int)key.ToString());
  LeafPage *node = reinterpret_cast<LeafPage *>(root_page->GetData());
  node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  node->Insert(key, value, comparator_);

  UpdateRootPageId(1);
  UnlockAndUnpinPageOfNode(root_page, true, LockMode::WRITE);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key, Operation::INSERT);
  LOG_INFO("findleaf return page %d pincount %d",leaf_page->GetPageId(),leaf_page->GetPinCount());
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  if (node->Lookup(key, nullptr, comparator_)) {
    UnlockAndUnpinPageOfNode(leaf_page, false, LockMode::WRITE);
    return false;
  }

  LOG_INFO("befor lock page %d pincount %d, insert key %d", leaf_page->GetPageId(),leaf_page->GetPinCount(), (int)key.ToString());
  int after_insert_size = node->Insert(key, value, comparator_);
  LOG_INFO("after lock page %d pincount %d, insert key %d", leaf_page->GetPageId(),leaf_page->GetPinCount(), (int)key.ToString());
  if (after_insert_size >= node->GetMaxSize()) {
    LeafPage *l2_node = Split(node);
    l2_node->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(l2_node->GetPageId());
    KeyType middle_key = l2_node->KeyAt(0);
    LOG_INFO("node page %d pincount %d",leaf_page->GetPageId(),leaf_page->GetPinCount());
    InsertIntoParent(node, middle_key, l2_node);

  } else {
    UnlockAndUnpinPageOfNode(leaf_page, true, LockMode::WRITE);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t l2_page_id;
  Page *l2_page = buffer_pool_manager_->NewPage(&l2_page_id);
  LOG_INFO("AFTER SPLIT PAGE %d", l2_page->GetPageId());
  LockPage(l2_page, LockMode::WRITE);
  if (l2_page == nullptr) {
    throw "out of memory";
  }
  N *l2_node = reinterpret_cast<N *>(l2_page->GetData());
  l2_node->Init(l2_page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(l2_node, buffer_pool_manager_);
  return l2_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  Page *parent_page;
  InternalPage *parent_node;
  // the old_node and new_node have been WLock by caller
  if (old_node->IsRootPage()) {
    page_id_t new_root_page_id;
    parent_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    LOG_INFO("NEW ROOT %d", new_root_page_id);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    new_node->SetParentPageId(new_root_page_id);
    old_node->SetParentPageId(new_root_page_id);
    LockPage(parent_page, LockMode::WRITE);
    parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_node->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
  } else {
    parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (parent_page == nullptr) {
      throw "out of memory";
    }
    LockPage(parent_page, LockMode::WRITE);
    parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  }
  LOG_INFO("SPLIT:old %d new %d parent %d", old_node->GetPageId(), new_node->GetPageId(), parent_node->GetPageId());
  int after_insert_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  UnlockAndUnpinPageOfNode(old_node, true, LockMode::WRITE);
  UnlockAndUnpinPageOfNode(new_node, true, LockMode::WRITE);

  if (after_insert_size >= parent_node->GetMaxSize()) {
    InternalPage *l2_node = Split(parent_node);
    KeyType middle_key = l2_node->KeyAt(0);
    InsertIntoParent(parent_node, middle_key, l2_node);
  } else {
    UnlockAndUnpinPageOfNode(parent_page, true, LockMode::WRITE);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_INFO("delete %d start",(int)key.ToString());
  Page *leaf_page = FindLeafPage(key, Operation::DELETE);
  LOG_INFO("FIND LEAF RETURN");
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  LOG_INFO("LEAF PAGE %d", leaf_page->GetPageId());
  if (!leaf_node->Lookup(key, nullptr, comparator_)) {
    LOG_INFO("cannot find key %d", (int)key.ToString());
    UnlockAndUnpinPageOfNode(leaf_page, false, LockMode::WRITE);
    return;
  }
  int after_delete_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  LOG_INFO("page %d delete key %d, after_delete_size %d", leaf_page->GetPageId(), (int)key.ToString(),
           after_delete_size);
  if (after_delete_size < leaf_node->GetMinSize()) {
    bool should_delete = CoalesceOrRedistribute(leaf_node);
    LOG_INFO("SHOULD DELETE %d",should_delete);
    if (should_delete) {
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
    LOG_INFO("DELETE %d finished",leaf_page->GetPageId());
  } else {
    UnlockAndUnpinPageOfNode(leaf_page, true, LockMode::WRITE);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    LOG_INFO("CR RETURN");
    return AdjustRoot(node);
  }
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (parent_page == nullptr) {
    throw "out of memory";
  }
  InternalPage *parent_page_as_internal = reinterpret_cast<InternalPage *>(parent_page->GetData());
  LockPage(parent_page, LockMode::WRITE);
  int index = parent_page_as_internal->ValueIndex(node->GetPageId());
  int sibling_index;
  if (index == 0) {
    sibling_index = 1;
  } else {
    sibling_index = index - 1;
  }
  page_id_t sibling_page_id = parent_page_as_internal->ValueAt(sibling_index);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  LockPage(sibling_page, LockMode::WRITE);
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  if (sibling_node->GetSize() + node->GetSize() < node->GetMaxSize()) {
    LOG_INFO("COALESCE %d %d to %d %d",node->GetPageId(),node->GetSize(),sibling_node->GetPageId(),sibling_node->GetSize());
    bool parent_should_delete = Coalesce(&sibling_node, &node, &parent_page_as_internal, index);
    LOG_INFO("parent should delete %d",parent_should_delete);
    if (parent_should_delete) {
      buffer_pool_manager_->DeletePage(parent_page_as_internal->GetPageId());
    }

    return true;
  } else {
    LOG_INFO("REDISTRIBUTE %d %d to %d %d",node->GetPageId(),node->GetSize(),sibling_node->GetPageId(),sibling_node->GetSize());
    Redistribute(sibling_node, node, parent_page_as_internal, index);

    return false;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  KeyType middle_key = (*parent)->KeyAt(index);
  (*node)->MoveAllTo(*neighbor_node, middle_key, buffer_pool_manager_);
  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    UnlockAndUnpinPageOfNode(*neighbor_node, true, LockMode::WRITE);
    UnlockAndUnpinPageOfNode(*node, true, LockMode::WRITE);
    return CoalesceOrRedistribute(*parent);
  } else {
    UnlockAndUnpinPageOfNode(*neighbor_node, true, LockMode::WRITE);
    UnlockAndUnpinPageOfNode(*node, true, LockMode::WRITE);
    UnlockAndUnpinPageOfNode(*parent, true, LockMode::WRITE);
    return false;
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_node, int index) {
  KeyType middle_key;
  if (index == 0) {
    middle_key = parent_node->KeyAt(1);
    parent_node->SetKeyAt(1, neighbor_node->KeyAt(1));
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
  } else {
    middle_key = parent_node->KeyAt(index);
    parent_node->SetKeyAt(index, neighbor_node->KeyAt(neighbor_node->GetSize() - 1));
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
  }
  UnlockAndUnpinPageOfNode(node, true, LockMode::WRITE);
  UnlockAndUnpinPageOfNode(parent_node, true, LockMode::WRITE);
  UnlockAndUnpinPageOfNode(neighbor_node, true, LockMode::WRITE);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() < 1) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      UnlockAndUnpinPageOfNode(old_root_node, true, LockMode::WRITE);
      return true;
    } else {
      UnlockAndUnpinPageOfNode(old_root_node, true, LockMode::WRITE);
      return false;
    }

  } else {
    if (old_root_node->GetSize() == 1) {
      InternalPage *old_root_node_as_internal = reinterpret_cast<InternalPage *>(old_root_node);
      page_id_t child_page_id = old_root_node_as_internal->ValueAt(0);
      Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
      if (child_page == nullptr) {
        throw "out of memory";
      }
      LockPage(child_page, LockMode::WRITE);
      BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      UnlockAndUnpinPageOfNode(child_node, true, LockMode::WRITE);
      UnlockAndUnpinPageOfNode(old_root_node, true, LockMode::WRITE);
      return true;
    } else {
      UnlockAndUnpinPageOfNode(old_root_node, true, LockMode::WRITE);
      return false;
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  Page *leftMost_leaf_page = FindLeafPage(KeyType{}, Operation::SEARCH, true);
  page_id_t p_id = leftMost_leaf_page->GetPageId();
  return INDEXITERATOR_TYPE(p_id, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *p = FindLeafPage(KeyType{key}, Operation::SEARCH, false);
  LeafPage *node = reinterpret_cast<LeafPage *>(p->GetData());
  page_id_t p_id = node->GetPageId();
  int key_index = node->KeyIndex(key, comparator_);
  UnlockAndUnpinPageOfNode(node, false, LockMode::READ);
  LOG_INFO("return iterator page %d index %d", p_id, key_index);
  return INDEXITERATOR_TYPE(p_id, buffer_pool_manager_, key_index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    throw "out of memory";
  }
  BPlusTreePage *cur_page_as_tree = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  while (!cur_page_as_tree->IsLeafPage()) {
    InternalPage *cur_page_as_internal = reinterpret_cast<InternalPage *>(cur_page_as_tree);
    cur_page = buffer_pool_manager_->FetchPage(cur_page_as_internal->ValueAt(cur_page_as_internal->GetSize() - 1));
    if (cur_page == nullptr) {
      throw "out of memory";
    }
    buffer_pool_manager_->UnpinPage(cur_page_as_internal->GetPageId(), false);
    cur_page_as_tree = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  }
  page_id_t p_id = cur_page_as_tree->GetPageId();
  int index = cur_page_as_tree->GetSize();
  buffer_pool_manager_->UnpinPage(p_id, false);
  return INDEXITERATOR_TYPE(p_id, buffer_pool_manager_, index);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Operation op, bool leftMost) {
  LOG_INFO("FIND LEAF");
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    LOG_INFO("FindLeafPage fetch page %d fail", root_page_id_);
    throw "out of memory";
  }
  BPlusTreePage *cur_page_as_tree = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  if (cur_page_as_tree->IsLeafPage()) {
    LOG_INFO("curpage is leaf %d", cur_page->GetPageId());
    if (op == Operation::SEARCH) {
      LockPage(cur_page, LockMode::READ);
    } else {
      LockPage(cur_page, LockMode::WRITE);
    }
    return cur_page;
  } else {
    LOG_INFO("curpage is internal %d", cur_page->GetPageId());
    LockPage(cur_page, LockMode::READ);
    InternalPage *cur_page_as_internal = reinterpret_cast<InternalPage *>(cur_page->GetData());

    page_id_t child_page_id =
        leftMost ? cur_page_as_internal->ValueAt(0) : cur_page_as_internal->Lookup(key, comparator_);
    LOG_INFO("child page id %d", child_page_id);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    LOG_INFO("CHILD PAGE ID %d", child_page->GetPageId());
    if (child_page == nullptr) {
      LOG_INFO("FindLeafPage fetch page %d fail", root_page_id_);
      throw "out of memory";
    }

    BPlusTreePage *child_page_as_tree = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    while (!child_page_as_tree->IsLeafPage()) {
      LOG_INFO("childpage is internal %d", child_page_as_tree->GetPageId());
      LockPage(child_page, LockMode::READ);
      UnlockAndUnpinPageOfNode(cur_page, false, LockMode::READ);
      cur_page = child_page;
      cur_page_as_internal = reinterpret_cast<InternalPage *>(cur_page->GetData());
      child_page_id = leftMost ? cur_page_as_internal->ValueAt(0) : cur_page_as_internal->Lookup(key, comparator_);

      child_page = buffer_pool_manager_->FetchPage(child_page_id);
      if (child_page == nullptr) {
        LOG_INFO("FindLeafPage fetch page %d fail", root_page_id_);
        throw "out of memory";
      }
      child_page_as_tree = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    }

    if (op == Operation::SEARCH) {
      LockPage(child_page, LockMode::READ);
    } else {
      LockPage(child_page, LockMode::WRITE);
    }
    LOG_INFO("child page %d", child_page->GetPageId());
    UnlockAndUnpinPageOfNode(cur_page, false, LockMode::READ);
    return child_page;
  }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  LOG_INFO("NOW ROOT %d", root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::UnlockAndUnpinPageOfNode(N *node, bool is_dirty, LockMode mode) {
  page_id_t p_id = node->GetPageId();
  LOG_INFO("Unlock page %d", p_id);
  if (lock_map_.count(p_id) == 0) {
    LOG_ERROR("# Unlock fail page: %d", p_id);
  }
  Page *page = lock_map_[p_id];

  if (mode == LockMode::READ) {
    page->RUnlatch();
  } else {
    page->WUnlatch();
  }
  lock_map_.erase(p_id);
  buffer_pool_manager_->UnpinPage(p_id, is_dirty);
  LOG_INFO("UnLock page %d data %d pointer %p", page->GetPageId(), page->GetPinCount(),page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page *page, LockMode mode) {
  page_id_t p_id = page->GetPageId();
  if (mode == LockMode::READ) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  lock_map_[p_id] = page;
  LOG_INFO("Lock page %d pointer %p pincount %d", lock_map_[p_id]->GetPageId(), page,page->GetPinCount());
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
