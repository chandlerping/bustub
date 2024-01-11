//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_itr_ = std::make_unique<TableIterator>(table_metadata_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (*table_itr_ != table_metadata_->table_->End()) {
    if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(&(**table_itr_), plan_->OutputSchema()).GetAs<bool>()) {
      break;
    }
    (*table_itr_)++;
  }

  if (*table_itr_ == table_metadata_->table_->End()) {
    return false;
  }
  *tuple = **table_itr_;
  *rid = tuple->GetRid();
  (*table_itr_)++;
  return true;
}

}  // namespace bustub
