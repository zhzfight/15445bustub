/**
 * index_iterator.cpp
 */
#include <common/logger.h>
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t p_id,BufferPoolManager *buffer_pool_manager,int specific_index){
  buffer_pool_manager_=buffer_pool_manager;
  Page *p=buffer_pool_manager_->FetchPage(p_id);
  if (p== nullptr){
    throw "out of memory";
  }
  cur_node_=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(p->GetData());
  index_=specific_index;

}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  buffer_pool_manager_->UnpinPage(cur_node_->GetPageId(),false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return cur_node_->GetNextPageId()==INVALID_PAGE_ID&&index_==cur_node_->GetSize();}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  return cur_node_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_++;
  if (cur_node_->GetNextPageId()!=INVALID_PAGE_ID&&index_==cur_node_->GetSize()){
    Page *p=buffer_pool_manager_->FetchPage(cur_node_->GetNextPageId());
    buffer_pool_manager_->UnpinPage(cur_node_->GetPageId(),false);
    index_=0;
    cur_node_=reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(p->GetData());
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
