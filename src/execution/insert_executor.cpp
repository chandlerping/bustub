//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (raw_id_ == static_cast<int>(plan_->RawValues().size())) {
      return false;
    }
    Tuple tuple_new(plan_->RawValues()[raw_id_], &table_metadata_->schema_);
    if (!table_metadata_->table_->InsertTuple(tuple_new, rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    for (IndexInfo *table_index : exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_)) {
      table_index->index_->InsertEntry(tuple_new.KeyFromTuple(table_metadata_->schema_, table_index->key_schema_, table_index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());
    }
    raw_id_++;
    return true;
  }
  Tuple *tuple_new = new Tuple();
  if (child_executor_->Next(tuple_new, rid)) {
    return table_metadata_->table_->InsertTuple(*tuple_new, rid, exec_ctx_->GetTransaction());
  }
  for (IndexInfo *table_index : exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_)) {
    table_index->index_->InsertEntry(tuple_new->KeyFromTuple(table_metadata_->schema_, table_index->key_schema_, table_index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());
  }
  return false;
}

}  // namespace bustub
