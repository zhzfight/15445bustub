//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <common/logger.h>
#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  LOG_INFO("here6");
  latch_.lock();
  if (page_id == INVALID_PAGE_ID) {
    latch_.unlock();
    return nullptr;
  }
  LOG_INFO("here7");
  if (page_table_.find(page_id) != page_table_.end()) {

    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].IncPinCount();
    LOG_INFO("STILL IN PAGETABLE %d page %d pincount %d pointer %p",page_id,pages_[frame_id].page_id_,pages_[frame_id].pin_count_,&pages_[frame_id]);
    replacer_->Pin(frame_id);
    latch_.unlock();
    return &pages_[frame_id];
  }

  frame_id_t replace_frame_id;
  if (free_list_.size() != 0) {
    replace_frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    LOG_INFO("erase page table");
    bool ok = replacer_->Victim(&replace_frame_id);
    if (!ok) {
      latch_.unlock();
      return nullptr;
    }
    page_table_.erase(pages_[replace_frame_id].GetPageId());
    if (pages_[replace_frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[replace_frame_id].GetPageId(), pages_[replace_frame_id].GetData());
    }
    pages_[replace_frame_id].ResetMemory();
    pages_[replace_frame_id].pin_count_ = 0;
    pages_[replace_frame_id].is_dirty_ = false;
    pages_[replace_frame_id].page_id_ = INVALID_PAGE_ID;
  }


  page_table_[page_id] = replace_frame_id;
  pages_[replace_frame_id].page_id_ = page_id;
  pages_[replace_frame_id].IncPinCount();
  disk_manager_->ReadPage(page_id, pages_[replace_frame_id].data_);
  latch_.unlock();
  LOG_INFO("replace PAGETABLE %d page %d pincount %d pointer %p",page_id,pages_[replace_frame_id].page_id_,pages_[replace_frame_id].pin_count_,&pages_[replace_frame_id]);
  return &pages_[replace_frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (pages_[frame_id].GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[frame_id].DecPinCount();
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  LOG_INFO("unpin page %d pincount %d",page_id,pages_[frame_id].pin_count_);
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  frame_id_t frame_id;
  if (free_list_.size() != 0) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    bool ok = replacer_->Victim(&frame_id);
    if (!ok) {
      latch_.unlock();
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData());
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }
  *page_id = disk_manager_->AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].IncPinCount();
  pages_[frame_id].is_dirty_ = false;
  page_table_[*page_id] = frame_id;
  pages_[frame_id].page_id_ = *page_id;
  latch_.unlock();
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
    }
  }
  latch_.unlock();
}

}  // namespace bustub
