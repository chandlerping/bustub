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

#include <memory>
#include <queue>
#include <string>
#include <type_traits>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

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
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
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
  // leaf look up, with latch crabbing to ensure concurrency safety
  // get root page & latch
  Page *page_raw = buffer_pool_manager_->FetchPage(root_page_id_);
  page_raw->RLatch();
  while (!reinterpret_cast<BPlusTreePage *>(page_raw->GetData())->IsLeafPage()) {
    // get child page & lacth
    page_id_t child_page_id = reinterpret_cast<InternalPage *>(page_raw->GetData())->Lookup(key, comparator_);
    Page *child_page_raw = buffer_pool_manager_->FetchPage(child_page_id);
    child_page_raw->RLatch();
    // release parent latch
    page_raw->RUnlatch();
    buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(page_raw->GetData())->GetPageId(), false);
    page_raw = child_page_raw;
  }

  // get leaf page
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page_raw);

  ValueType *val = new ValueType;
  if (!leaf_page->Lookup(key, val, comparator_)) {
    // not found
    page_raw->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  // found, store in result
  page_raw->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  result->push_back(*val);
  return true;
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
  // If empty, start a new tree
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  // insert into leaf
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // try to ask for a new page
  page_id_t *new_page_id = new page_id_t;
  Page *root_page_raw = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page_id == nullptr) {
    LOG_ERROR("out of memory");
    return;
  }

  // update root page id, root page should also be leaf page
  root_page_id_ = *new_page_id;
  UpdateRootPageId(1);

  // set root page metadata
  LeafPage *root_page = reinterpret_cast<LeafPage *>(root_page_raw->GetData());
  root_page->SetPageType(IndexPageType::LEAF_PAGE);
  root_page->SetSize(0);
  root_page->SetMaxSize(leaf_max_size_);
  root_page->SetPageId(root_page_id_);
  root_page->SetParentPageId(INVALID_PAGE_ID);
  root_page->SetNextPageId(INVALID_PAGE_ID);

  // insert into leaf/root
  root_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
  // leaf look up, latch crabbing
  // fetch root page
  std::vector<Page *> latch_pages_raw; // used for recording latches being held
  Page *page_raw = buffer_pool_manager_->FetchPage(root_page_id_);
  page_raw->WLatch();
  latch_pages_raw.push_back(page_raw);

  while (!reinterpret_cast<BPlusTreePage *>(page_raw->GetData())->IsLeafPage()) {
    // land on child page, get the latch
    page_id_t child_page_id = reinterpret_cast<InternalPage *>(page_raw->GetData())->Lookup(key, comparator_);
    Page *child_page_raw = buffer_pool_manager_->FetchPage(child_page_id);
    child_page_raw->WLatch();
    InternalPage *child_page = reinterpret_cast<InternalPage *>(child_page_raw->GetData());

    // If the child page is safe, whose size is less than max size, we can confirm that
    // this insertion will not cause split. So we can release the previous latches on ancestors.
    if (child_page->GetSize() < child_page->GetMaxSize()) {
      for (Page *latched_raw : latch_pages_raw) {
        latched_raw->WUnlatch();
        buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(latched_raw->GetData())->GetPageId(), false);
      }
      latch_pages_raw.clear();
    }
    latch_pages_raw.push_back(child_page_raw);
    page_raw = child_page_raw;
  }

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page_raw);

  ValueType *val = new ValueType;

  if (leaf_page->Lookup(key, val, comparator_)) {
    // key already exists, release all latches
    for (Page *latched_raw : latch_pages_raw) {
      latched_raw->WUnlatch();
      buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(latched_raw->GetData())->GetPageId(), false);
    }
    latch_pages_raw.clear();
    return false;
  }

  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    // if not full, insert, then release all latches
    leaf_page->Insert(key, value, comparator_);
    for (Page *latched_raw : latch_pages_raw) {
      latched_raw->WUnlatch();
      buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(latched_raw->GetData())->GetPageId(), false);
    }
    latch_pages_raw.clear();
    return true;
  }

  // leaf page full, need split & modify parent
  leaf_page->Insert(key, value, comparator_);
  LeafPage *new_leaf_page = Split(leaf_page);
  leaf_page->SetNextPageId(new_leaf_page->GetPageId());

  InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page);
  
  // release latches
  for (Page *latched_raw : latch_pages_raw) {
    latched_raw->WUnlatch();
    buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(latched_raw->GetData())->GetPageId(), false);
  }
  latch_pages_raw.clear();
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
  // allocate a new page
  page_id_t *new_page_id = new page_id_t;
  Page *new_page_raw = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page_id == nullptr) {
    LOG_ERROR("out of memory");
  }

  if (reinterpret_cast<BPlusTreePage *>(node)->IsLeafPage()) {
    // leaf page
    LeafPage *recipient = reinterpret_cast<LeafPage *>(new_page_raw->GetData());
    recipient->SetPageType(IndexPageType::LEAF_PAGE);
    recipient->SetSize(0);
    recipient->SetMaxSize(reinterpret_cast<LeafPage *>(node)->GetMaxSize());
    recipient->SetPageId(*new_page_id);
    recipient->SetParentPageId(reinterpret_cast<LeafPage *>(node)->GetParentPageId());
    recipient->SetNextPageId(reinterpret_cast<LeafPage *>(node)->GetNextPageId());

    reinterpret_cast<LeafPage *>(node)->MoveHalfTo(recipient);
    reinterpret_cast<LeafPage *>(node)->SetNextPageId(recipient->GetPageId());
    buffer_pool_manager_->UnpinPage(new_page_raw->GetPageId(), true);
    return reinterpret_cast<N *>(recipient);
  }

  // internal page
  InternalPage *recipient = reinterpret_cast<InternalPage *>(new_page_raw->GetData());
  recipient->SetPageType(IndexPageType::INTERNAL_PAGE);
  recipient->SetSize(1);
  recipient->SetMaxSize(reinterpret_cast<InternalPage *>(node)->GetMaxSize());
  recipient->SetPageId(*new_page_id);
  recipient->SetParentPageId(reinterpret_cast<InternalPage *>(node)->GetParentPageId());

  reinterpret_cast<InternalPage *>(node)->MoveHalfTo(recipient, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(*new_page_id, true);
  return reinterpret_cast<N *>(recipient);
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
  if (old_node->GetPageId() == root_page_id_) {
    // old_node is root, create a new root page
    page_id_t *new_page_id = new page_id_t;
    Page *new_root_page_raw = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page_id == nullptr) {
      LOG_ERROR("out of memory");
    }

    root_page_id_ = *new_page_id;
    UpdateRootPageId();

    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_root_page_raw->GetData());
    new_root_page->SetPageId(root_page_id_);
    new_root_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    new_root_page->SetMaxSize(internal_max_size_);
    new_root_page->SetSize(1);

    // insert 2 child info to root
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  // old_node is a non-root internal page
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (parent_page->GetSize() > parent_page->GetMaxSize()) {
    // parent needs to be split
    KeyType middle_key = parent_page->KeyAt(parent_page->GetSize() / 2);
    InternalPage *new_parent_page = Split(parent_page);
    InsertIntoParent(parent_page, middle_key, new_parent_page);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
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
  if (IsEmpty()) {
    return;
  }
  
  // leaf look up, latch crabbing
  // get root & latch
  std::vector<Page *> latch_pages_raw; // used for recording all latches being held
  Page *page_raw = buffer_pool_manager_->FetchPage(root_page_id_);
  page_raw->WLatch();
  latch_pages_raw.push_back(page_raw);
  while (!reinterpret_cast<BPlusTreePage *>(page_raw->GetData())->IsLeafPage()) {
    // get child & latch
    page_id_t child_page_id = reinterpret_cast<InternalPage *>(page_raw->GetData())->Lookup(key, comparator_);
    Page *child_page_raw = buffer_pool_manager_->FetchPage(child_page_id);
    child_page_raw->WLatch();
    InternalPage *child_page = reinterpret_cast<InternalPage *>(child_page_raw->GetData());

    // If child is safe, whose size greater than min size, this means this deletion will not cause merge or redistribute.
    // We can release all latches on ancestors.
    if (child_page->GetSize() > child_page->GetMinSize()) {
      for (Page *latched_raw : latch_pages_raw) {
        latched_raw->WUnlatch();
        buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(latched_raw->GetData())->GetPageId(), false);
      }
      latch_pages_raw.clear();
    }
    latch_pages_raw.push_back(child_page_raw);
    page_raw = child_page_raw;
  }

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page_raw);

  // delete record on leaf
  leaf_page->RemoveAndDeleteRecord(key, comparator_);

  // coalesce or redistribute
  CoalesceOrRedistribute(leaf_page);

  // release latches
  for (Page *latched_raw : latch_pages_raw) {
    latched_raw->WUnlatch();
    buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(latched_raw->GetData())->GetPageId(), false);
  }
  latch_pages_raw.clear();
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
  // check if need coalesce or redistribute
  if (reinterpret_cast<BPlusTreePage *>(node)->GetSize() >= reinterpret_cast<BPlusTreePage *>(node)->GetMinSize()) {
    return false;
  }

  // check if node is root page
  if (root_page_id_ == reinterpret_cast<BPlusTreePage *>(node)->GetPageId()) {
    return AdjustRoot(node);
  }

  // get parent pege
  page_id_t parent_id = reinterpret_cast<BPlusTreePage *>(node)->GetParentPageId();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  BPlusTreePage *this_node = reinterpret_cast<BPlusTreePage *>(node);

  // find index of node in parent page
  int arr_id = parent_page->ValueIndex(this_node->GetPageId());

  if (arr_id == 0) {
    // node (0) | neighbor (1)
    page_id_t neighbor_page_id = parent_page->ValueAt(1);
    BPlusTreePage *neighbor_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(neighbor_page_id));
    if (this_node->GetSize() + neighbor_node->GetSize() > this_node->GetMaxSize()) {
      // redistribute
      Redistribute(reinterpret_cast<N *>(neighbor_node), reinterpret_cast<N *>(this_node), 0);
      return false;
    }
    // coalesce
    Coalesce(reinterpret_cast<N **>(&neighbor_node), &node, &parent_page, 0);
    return true;
  }

  // neighbor (id - 1) | node (id)
  page_id_t neighbor_page_id = parent_page->ValueAt(arr_id - 1);
  BPlusTreePage *neighbor_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(neighbor_page_id));
  if (this_node->GetSize() + neighbor_node->GetSize() > this_node->GetMaxSize()) {
    // redistribute
    Redistribute(neighbor_node, this_node, 1);
    return false;
  }
  // coalesce
  Coalesce(reinterpret_cast<N **>(&neighbor_node), &node, &parent_page, 1);
  return true;
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
  // get parent
  page_id_t parent_id = (*reinterpret_cast<BPlusTreePage **>(node))->GetParentPageId();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id));
  page_id_t val_to_delete = 0;
  if (reinterpret_cast<BPlusTreePage *>(*node)->IsLeafPage()) {
    // leaf
    LeafPage *neighbor_leaf = reinterpret_cast<LeafPage *>(*neighbor_node);
    LeafPage *leaf = reinterpret_cast<LeafPage *>(*node);
    if (index == 0) {
      // node <-- neighbor
      neighbor_leaf->MoveAllTo(leaf);
      leaf->SetNextPageId(neighbor_leaf->GetNextPageId());
      // delete neighbor
      buffer_pool_manager_->UnpinPage(neighbor_leaf->GetPageId(), true);
      buffer_pool_manager_->DeletePage(neighbor_leaf->GetPageId());
      val_to_delete = neighbor_leaf->GetPageId();
    } else {
      // neighbor <-- node
      leaf->MoveAllTo(neighbor_leaf);
      neighbor_leaf->SetNextPageId(leaf->GetNextPageId());
      // delete node
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      buffer_pool_manager_->DeletePage(leaf->GetPageId());
      val_to_delete = leaf->GetPageId();
    }
  } else {
    // internal
    InternalPage *neighbor_internal = reinterpret_cast<InternalPage *>(*neighbor_node);
    InternalPage *internal = reinterpret_cast<InternalPage *>(*node);
    if (index == 0) {
      // node <-- neighbor
      KeyType middle_key = parent_page->KeyAt(parent_page->ValueIndex(neighbor_internal->GetPageId()));
      neighbor_internal->MoveAllTo(internal, middle_key, buffer_pool_manager_);
      // delete neighbor
      buffer_pool_manager_->UnpinPage(neighbor_internal->GetPageId(), true);
      buffer_pool_manager_->DeletePage(neighbor_internal->GetPageId());
      val_to_delete = neighbor_internal->GetPageId();
    } else {
      // neighbor <-- node
      KeyType middle_key = parent_page->KeyAt(parent_page->ValueIndex(internal->GetPageId()));
      internal->MoveAllTo(neighbor_internal, middle_key, buffer_pool_manager_);
      // delete node
      buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
      buffer_pool_manager_->DeletePage(internal->GetPageId());
      val_to_delete = internal->GetPageId();
    }
  }
  parent_page->Remove(parent_page->ValueIndex(val_to_delete));
  // recursively process parent
  return CoalesceOrRedistribute(parent_page, transaction);
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // get parent
  page_id_t parent_id = reinterpret_cast<BPlusTreePage *>(node)->GetParentPageId();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id));
  if (reinterpret_cast<BPlusTreePage *>(node)->IsLeafPage()) {
    // node is leaf
    page_id_t second_page_id = 0;
    KeyType new_key = *(new KeyType);
    if (index == 0) {
      // node <-- neighbor
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(node));
      second_page_id = reinterpret_cast<LeafPage *>(neighbor_node)->GetPageId();
      new_key = reinterpret_cast<LeafPage *>(neighbor_node)->KeyAt(0);
    } else {
      // neighbor --> node
      reinterpret_cast<LeafPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(node));
      second_page_id = reinterpret_cast<LeafPage *>(node)->GetPageId();
      new_key = reinterpret_cast<LeafPage *>(node)->KeyAt(0);
    }
    int key_id = parent_page->ValueIndex(second_page_id);
    parent_page->SetKeyAt(key_id, new_key);
  } else {
    // node is internal
    if (index == 0) {
      // node <-- neighbor
      page_id_t second_page_id = reinterpret_cast<InternalPage *>(neighbor_node)->GetPageId();
      int arr_id = parent_page->ValueIndex(second_page_id);
      KeyType middle_key = parent_page->KeyAt(arr_id);
      reinterpret_cast<InternalPage *>(neighbor_node)->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(node), middle_key, buffer_pool_manager_);
      parent_page->SetKeyAt(arr_id, reinterpret_cast<InternalPage *>(neighbor_node)->KeyAt(0));
    } else {
      // neighbor --> node
      page_id_t second_page_id = reinterpret_cast<InternalPage *>(node)->GetPageId();
      int arr_id = parent_page->ValueIndex(second_page_id);
      KeyType middle_key = parent_page->KeyAt(arr_id);
      reinterpret_cast<InternalPage *>(neighbor_node)->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(node), middle_key, buffer_pool_manager_);
      parent_page->SetKeyAt(arr_id, reinterpret_cast<InternalPage *>(node)->KeyAt(0));
    }
  }
  
  buffer_pool_manager_->UnpinPage(parent_id, true);
  buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(neighbor_node)->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(reinterpret_cast<BPlusTreePage *>(node)->GetPageId(), true);
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
  // root size > 1
  if (old_root_node->GetSize() > 1) {
    return false;
  }

  // delete last element in the whole b+ tree
  if (old_root_node->IsLeafPage()) {
    return true;
  }

  // old root is internal page, and has one last child, set the only child as new root
  root_page_id_ = reinterpret_cast<InternalPage *>(old_root_node)->RemoveAndReturnOnlyChild();
  UpdateRootPageId();
  buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  return true;
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
  KeyType *tmp_key = new KeyType;
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(*tmp_key, true)->GetData());
  return INDEXITERATOR_TYPE(index_name_, leaf_page->GetPageId(), 0, buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, true)->GetData());
  return INDEXITERATOR_TYPE(index_name_, leaf_page->GetPageId(), leaf_page->KeyIndex(key, comparator_), buffer_pool_manager_); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  INDEXITERATOR_TYPE itr = begin();
  while (!itr.isEnd()) {
    ++itr;
  }

  return itr;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // get root page
  BPlusTreePage *root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  BPlusTreePage *p = root_page;
  while (!p->IsLeafPage()) {
    // loop until leaf page
    page_id_t child_page_id = -1;
    if (leftMost) {
      child_page_id = static_cast<InternalPage *>(p)->ValueAt(0);
    } else {
      child_page_id = static_cast<InternalPage *>(p)->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
    p = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
  }
  return buffer_pool_manager_->FetchPage(p->GetPageId());
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
