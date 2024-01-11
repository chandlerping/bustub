//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), aht_(new SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
    aht_iterator_(aht_->Begin()) {
  plan_ = plan;
  child_ = std::move(child);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  std::cout << "init" << std::endl;
  Tuple *tuple_new = new Tuple();
  RID *rid = new RID();
  child_->Init();
  while (child_->Next(tuple_new, rid)) {
    aht_->InsertCombine(MakeKey(tuple_new), MakeVal(tuple_new));
  }
  aht_iterator_ = aht_->Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  std::cout << "next" << std::endl;
  while (aht_iterator_ != aht_->End() && (plan_->GetHaving() != nullptr) && !plan_->GetHaving()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
    std::cout << ">---<" << std::endl;
    ++aht_iterator_;
  }
  if (aht_iterator_ == aht_->End()) {
    return false;
  }
  std::cout << "here " << aht_iterator_.Val().aggregates_.size() << std::endl;
  std::vector<Value> vals = {aht_iterator_.Val().aggregates_[0]};
  if (!aht_iterator_.Key().group_bys_.empty()) {
    vals.emplace_back(aht_iterator_.Key().group_bys_[0]);
  }
  for (int i = 1; i < static_cast<int>(aht_iterator_.Val().aggregates_.size()); i++) {
    vals.emplace_back(aht_iterator_.Val().aggregates_[i]);
  }
  *tuple = *new Tuple(vals, plan_->OutputSchema());
  ++aht_iterator_;
  return true;
}

}  // namespace bustub
