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
#include <iostream>
#include <mutex>

#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  
  // return false if replacer is empty
  if (Size() == 0) {
    return false;
  }

  // pick the first frame in the list as the victim and pin it
  *frame_id = *frame_list_.begin();
  Pin(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock<std::recursive_mutex> lock(latch_);

  // return if frame not found
  if (frame_id2itr_.find(frame_id) == frame_id2itr_.end()) {
    return;
  }

  // delete the frame from frame_list_ and frame_id2itr_
  std::list<frame_id_t>::iterator itr = frame_id2itr_[frame_id];
  frame_id2itr_.erase(frame_id);
  frame_list_.erase(itr);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock<std::recursive_mutex> lock(latch_);

  // error if frame size exceeds upperbound
  if (Size() >= max_num_pages_) {
    LOG_DEBUG("page number exceeds max number of lru replacer");
    return;
  }

  // return if frame already unpinned
  if (frame_id2itr_.find(frame_id) != frame_id2itr_.end()) {
      return;
  }

  // add frame to frame_list and frame_id2itr_
  frame_list_.push_back(frame_id);
  std::list<frame_id_t>::iterator itr = frame_list_.end();
  frame_id2itr_[frame_id] = --itr;
}

size_t LRUReplacer::Size() {
  std::unique_lock<std::recursive_mutex> lock(latch_);
  return frame_list_.size();
}

}  // namespace bustub
