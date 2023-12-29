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
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  explicit IndexIterator(std::string index_name, page_id_t leaf_id, int cur_index, BufferPoolManager *bpm);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return index_name_ == itr.index_name_ && current_leaf_page_id_ == itr.current_leaf_page_id_ && current_index_ == itr.current_index_;
  }

  bool operator!=(const IndexIterator &itr) const {
    return index_name_ != itr.index_name_ || current_leaf_page_id_ != itr.current_leaf_page_id_ || current_index_ != itr.current_index_;
  }

  int GetPageId() {return current_leaf_page_id_;}

  int GetCurIndex() {return current_index_;}

 private:
  // add your own private member variables here
  std::string index_name_;
  page_id_t current_leaf_page_id_;
  int current_index_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
