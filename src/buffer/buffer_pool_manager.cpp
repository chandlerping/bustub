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

#include <list>
#include <unordered_map>
#include <iostream>

#include "common/config.h"
#include "common/logger.h"
#include "storage/page/page.h"

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
  
  // lock
  latch_.lock();

  // search if the requested page exists
  if (page_table_.find(page_id) != page_table_.end()) {
    // if exist, pin & return
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return &pages_[frame_id];
  }

  // find a replacement page R from free list first, then repalcer
  frame_id_t frame_id_r = -1;
  if (!free_list_.empty()) {
    frame_id_r = *free_list_.begin();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id_r)) {
    latch_.unlock();
    return nullptr;
  }

  // find current page id of that replacement page
  page_id_t page_id_r = pages_[frame_id_r].GetPageId();

  // delete R and insert P to page table
  page_table_.erase(page_id_r);
  page_table_[page_id] = frame_id_r;

  // update P's metadata and read from disk
  // write R to disk if dirty
  if (pages_[frame_id_r].IsDirty()) {
    disk_manager_->WritePage(page_id_r, pages_[frame_id_r].GetData());
  }
  pages_[frame_id_r].page_id_ = page_id;
  pages_[frame_id_r].is_dirty_ = false;
  pages_[frame_id_r].pin_count_++;
  disk_manager_->ReadPage(page_id, pages_[frame_id_r].GetData());
  // pages_[frame_id_r].WUnlatch();

  latch_.unlock();
  return &pages_[frame_id_r];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // lock
  latch_.lock();

  // check if page to unpin is in buffer pool
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  // return false if it has already been unpinned
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }

  // unpin, add to free_list and replacer if pin_count == 0
  if (!pages_[frame_id].is_dirty_) {
    // if this page is already dirty, we don't modify it.
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
    free_list_.push_back(frame_id);
  }

  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!

  // lock
  latch_.lock();

  // return false if page not found in page table
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // flush the page to disk
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());

  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // lock
  latch_.lock();

  // return nullptr if all pages in buffer pool are pinned
  size_t i = 0;
  while (i < pool_size_ && pages_[i].GetPinCount() > 0) {
    i++;
  }
  if (i == pool_size_) {
    latch_.unlock();
    return nullptr;
  }

  // find a victim page P from free list or replacer
  frame_id_t frame_id_p = -1;
  if (!free_list_.empty()) {
    frame_id_p = *free_list_.begin();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id_p)) {
    latch_.unlock();
    return nullptr;
  }

  // remove from page table
  page_table_.erase(pages_[frame_id_p].GetPageId());

  // flush page P if dirty
  if (pages_[frame_id_p].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id_p].GetPageId(), pages_[frame_id_p].GetData());
  }

  // allocate a new page on disk
  *page_id = disk_manager_->AllocatePage();

  // update P's metadata and zero out memory
  pages_[frame_id_p].page_id_ = *page_id;
  pages_[frame_id_p].pin_count_++;
  pages_[frame_id_p].is_dirty_ = false;
  pages_[frame_id_p].ResetMemory();

  // update replacer and replacer
  replacer_->Pin(frame_id_p);
  page_table_[*page_id] = frame_id_p;

  latch_.unlock();
  return &pages_[frame_id_p];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  // lock
  latch_.lock();

  // if page doesn't exist, return true
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  // get frame id
  frame_id_t frame_id = page_table_[page_id];
  // return false if pin count > 0
  if (pages_[frame_id].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }

  // delete from page table
  page_table_.erase(page_id);

  // deallocate from disk
  disk_manager_->DeallocatePage(page_id);

  // update metadata
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  // return to free list and unpin it
  free_list_.push_back(frame_id);
  replacer_->Unpin(frame_id);

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].GetPageId());
  }
}

}  // namespace bustub
