//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  /** detail: the outer(left) table uses less pages */
  if (left_executor_->GetExecutorContext()->GetBufferPoolManager()->GetPoolSize() >
      right_executor_->GetExecutorContext()->GetBufferPoolManager()->GetPoolSize())
    left_executor_.swap(right_executor_);

  /** build left hashtable */
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    Value value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema());
    HashedJoinKey key{value};
    left_table_.push_back({std::move(value), std::move(tuple)});
    size_t last_idx = left_table_.size();
    if (left_ht_.find(key) != left_ht_.cend())
      left_ht_[key].push_back(last_idx);
    else
      left_ht_.insert({key, {last_idx}});
  }
  while (right_executor_->Next(&tuple, &rid)) {
    Value value = plan_->RightJoinKeyExpression()->Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
    HashedJoinKey key{value};
    right_table_.push_back({std::move(value), std::move(tuple)});
  }

  /** bind both sides iter */
  left_it_ = left_ht_.begin();
  right_it_ = right_table_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_it_ != left_ht_.cend()) {
    while (right_it_ != right_table_.cend()) {
      const auto &[right_jk, right_tp] = *right_it_;
      HashedJoinKey right_hjk{right_jk};
      auto iter = left_ht_.find(right_hjk);
      if (iter == left_ht_.end()) {  // left_ht miss
        continue;
      } else {  // left_ht hit
        auto left_join_key_count = iter->second.size();
        // left_key hashed from only one join_key
        if (left_join_key_count == 1) {
          LOG_DEBUG("HashJoin: only one join_key");
        }
        // double check for which left join_key
        for (size_t i = 0; i < left_join_key_count; ++i) {
          const auto &[left_jk, left_tp] = left_table_[iter->second[i]];
          if (left_jk.CompareEquals(right_jk) == CmpBool::CmpTrue) {
            // build the retvalue
            std::vector<Value> values;
            values.reserve(plan_->OutputSchema()->GetColumnCount());
            for (const auto &column : plan_->OutputSchema()->GetColumns()) {
              auto column_expr = reinterpret_cast<const ColumnValueExpression*>(column.GetExpr());
              if(column_expr->GetTupleIdx() == 0)
                values.push_back(left_tp.GetValue(plan_->GetLeftPlan()->OutputSchema(), column_expr->GetColIdx()));
              else
                values.push_back(right_tp.GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
            }
            *tuple = Tuple(values, plan_->OutputSchema());
            *rid = tuple->GetRid();
            return true;
          }
        }
      }
    }  // while
    // reset inner loop
    right_it_ = right_table_.begin();
  }  // while
  return false;
}

}  // namespace bustub
