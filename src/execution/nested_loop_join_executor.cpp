//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (left_tuple_ == nullptr || right_tuple_ == nullptr || 
    !plan_->Predicate()->EvaluateJoin(left_tuple_, plan_->GetLeftPlan()->OutputSchema(), right_tuple_, plan_->GetRightPlan()->OutputSchema()).GetAs<bool>()) {
    if (left_tuple_ == nullptr) {
      left_tuple_ = new Tuple();
      if (!left_executor_->Next(left_tuple_, rid)) {
        return false;
      }
    }
    right_tuple_ = new Tuple();
    if (!right_executor_->Next(right_tuple_, rid)) {
      // right reaches end
      if (!left_executor_->Next(left_tuple_, rid)) {
        return false;
      }
      right_executor_->Init();
      if (!right_executor_->Next(right_tuple_, rid)) {
        return false;
      }
    }
  }
  std::vector<Value> vals;
  vals.reserve(static_cast<int>(plan_->GetLeftPlan()->OutputSchema()->GetColumnCount()));
  for (int i = 0; i < static_cast<int>(plan_->GetLeftPlan()->OutputSchema()->GetColumnCount()); i++) {
    vals.push_back(left_tuple_->GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
  }
  for (int i = 0; i < static_cast<int>(plan_->GetRightPlan()->OutputSchema()->GetColumnCount()); i++) {
    vals.push_back(right_tuple_->GetValue(plan_->GetRightPlan()->OutputSchema(), i));
  }
  *tuple = *new Tuple(vals, plan_->OutputSchema());
  right_tuple_ = nullptr;
  return true;
}

}  // namespace bustub
