//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "catalog/catalog.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/table_iterator.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) { plan_ = plan; }

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  index_itr_ = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (index_itr_ != reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())->GetEndIterator()) {
    *rid = (*index_itr_).second;
    TableMetadata *table_metadata = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    TableIterator table_itr(table_metadata->table_.get(), *rid, exec_ctx_->GetTransaction());
    if (plan_->GetPredicate()->Evaluate(&(*table_itr), plan_->OutputSchema()).GetAs<bool>()) {
      *tuple = *table_itr;
      break;
    }
    ++index_itr_;
  }
  if (index_itr_ == reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())->GetEndIterator()) {
    return false;
  }
  ++index_itr_;
  return true;
}

}  // namespace bustub
