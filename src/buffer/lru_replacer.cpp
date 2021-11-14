//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capacity_ =num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (cache_.size()==0){
    latch_.unlock();
    return false;
  }
  *frame_id = cache_.back();
  cache_.pop_back();
  m_.erase(*frame_id);
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  if (m_.find(frame_id)== m_.end()){
    latch_.unlock();
    return;
  }
  cache_.erase(m_[frame_id]);
  m_.erase(frame_id);
  latch_.unlock();
  return;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if (m_.find(frame_id)!= m_.end()){
    latch_.unlock();
    return;
  }
  if (cache_.size()== capacity_){
    frame_id_t lr_frame_id = cache_.back();
    m_.erase(lr_frame_id);
    cache_.pop_back();
  }
  cache_.push_front(frame_id);
  m_.insert({frame_id, cache_.begin()});
  latch_.unlock();
  return;

}

size_t LRUReplacer::Size() { return cache_.size(); }

}  // namespace bustub
