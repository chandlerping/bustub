//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple *tuple_new = new Tuple();
  if (child_executor_->Next(tuple_new, rid)) {
    if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    Tuple updated_tuple = GenerateUpdatedTuple(*tuple_new);
    if (!table_info_->table_->UpdateTuple(updated_tuple, updated_tuple.GetRid(), exec_ctx_->GetTransaction())) {
      return false;
    }
    for (IndexInfo *table_index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      table_index->index_->DeleteEntry(tuple_new->KeyFromTuple(table_info_->schema_, table_index->key_schema_, table_index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());    
      table_index->index_->InsertEntry(updated_tuple.KeyFromTuple(table_info_->schema_, table_index->key_schema_, table_index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());    
    }
  }
  return false;
}
}  // namespace bustub
