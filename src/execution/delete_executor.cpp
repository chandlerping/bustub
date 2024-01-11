//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple *tuple_new = new Tuple();
  if (child_executor_->Next(tuple_new, rid)) {
    for (IndexInfo *table_index : exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_)) {
      table_index->index_->DeleteEntry(tuple_new->KeyFromTuple(table_metadata_->schema_, table_index->key_schema_, table_index->index_->GetKeyAttrs()), *rid, exec_ctx_->GetTransaction());
    }
    if (table_metadata_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
