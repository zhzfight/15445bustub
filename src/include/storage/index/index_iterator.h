//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once

#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(){
    cur_node_= nullptr;
    index_=0;
    buffer_pool_manager_= nullptr;
  }
  IndexIterator(page_id_t p_id, BufferPoolManager *buffer_pool_manager_, int specific_index = 0);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {

    return cur_node_->GetPageId() == itr.cur_node_->GetPageId() && index_ == itr.index_;
  }

  bool operator!=(const IndexIterator &itr) const {

    return cur_node_->GetPageId() != itr.cur_node_->GetPageId() || index_ != itr.index_;
  }
  INDEXITERATOR_TYPE &operator=(const INDEXITERATOR_TYPE &other){
    cur_node_=other.cur_node_;
    index_=other.index_;
    buffer_pool_manager_=other.buffer_pool_manager_;
    return *this;
  }

 private:
  // add your own private member variables here

  B_PLUS_TREE_LEAF_PAGE_TYPE *cur_node_;
  int index_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
