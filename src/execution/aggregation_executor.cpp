//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_it_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aht_it_ = aht_.Begin();
  child_->Init();

  auto aggregate_values = aht_.GenerateInitialAggregateValue().aggregates_;
  const auto &aggregates = plan_->GetAggregates();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aggregate_values.clear();
    const auto &groupbys = plan_->GetGroupBys();
    for (const auto &groupby : groupbys) {
      aggregate_values.push_back(groupby->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    std::vector<Value> values;
    values.reserve(aggregates.size());
    for (const auto &aggregate : aggregates) {
      values.push_back(aggregate->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    aht_.InsertCombine(AggregateKey{aggregate_values}, AggregateValue{values});
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (aht_it_ != aht_.End()) {
    auto cur_it = aht_it_;
    ++aht_it_;
    if (plan_->GetHaving() != nullptr) {
      Value value = plan_->GetHaving()->EvaluateAggregate(cur_it.Key().group_bys_, cur_it.Val().aggregates_);
      if (!value.GetAs<bool>()) continue;  // excluding from the `having` clause
    }
    // build the retvalue
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
      values.push_back(column_expr->EvaluateAggregate(cur_it.Key().group_bys_, cur_it.Val().aggregates_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
