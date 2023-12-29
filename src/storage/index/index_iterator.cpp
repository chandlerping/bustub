/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::string index_name, page_id_t leaf_id, int cur_index, BufferPoolManager *bpm) {
  index_name_ = std::move(index_name);
  current_leaf_page_id_ = leaf_id;
  current_index_ = cur_index;
  buffer_pool_manager_ = bpm;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  B_PLUS_TREE_LEAF_PAGE_TYPE *current_leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(current_leaf_page_id_)->GetData());
  if (current_leaf->GetNextPageId() == INVALID_PAGE_ID && current_index_ == current_leaf->GetSize()) {
    // last leaf, last index
    buffer_pool_manager_->UnpinPage(current_leaf_page_id_, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(current_leaf_page_id_, false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  // fetch corresponding item
  B_PLUS_TREE_LEAF_PAGE_TYPE *current_leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(current_leaf_page_id_)->GetData());
  const MappingType &ret = current_leaf->GetItem(current_index_);
  buffer_pool_manager_->UnpinPage(current_leaf_page_id_, false);
  return ret;
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
  if (isEnd()) {
    return *this;
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *current_leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(current_leaf_page_id_)->GetData());
  if (current_leaf->GetNextPageId() == INVALID_PAGE_ID) {
    // last leaf
    current_index_++;
  } else if (current_index_ < current_leaf->GetSize() - 1) {
    // not last leaf, not last index
    current_index_++;
  } else {
    // go to next leaf
    current_index_ = 0;
    current_leaf_page_id_ = current_leaf->GetNextPageId();
  }
  buffer_pool_manager_->UnpinPage(current_leaf_page_id_, false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
