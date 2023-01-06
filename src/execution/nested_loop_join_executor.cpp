//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/** note:
 * Maybe I have a misunderstanding on LoopJoin.
 * I thought it's a double-layer loop, and can yield & resume.
 * Instead of:
 * The left relation is main and the right relation is aux.
 * Just yield on left relation, and right.Init() for each inner loop.
 */

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (auto predicate = plan_->Predicate(); predicate != nullptr)
    predicate_ = predicate;
  else
    predicate_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  /** detail: the outer relation uses less pages
   * So, compare and swap at first.
   * */
  if (left_executor_->GetExecutorContext()->GetBufferPoolManager()->GetPoolSize() >
      right_executor_->GetExecutorContext()->GetBufferPoolManager()->GetPoolSize())
    left_executor_.swap(right_executor_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple, right_tuple;
  RID left_rid, right_rid;

  while (left_executor_->Next(&left_tuple, &left_rid)) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = predicate_->EvaluateJoin(&left_tuple, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                                            plan_->GetRightPlan()->OutputSchema());
      if (value.GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(plan_->OutputSchema()->GetColumnCount());
        for (const auto &column : plan_->OutputSchema()->GetColumns()) {
          auto column_expr = dynamic_cast<const ColumnValueExpression *>(column.GetExpr());
          if (column_expr->GetTupleIdx() == 0)
            values.push_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), column_expr->GetColIdx()));
          else
            values.push_back(right_tuple.GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        *rid = tuple->GetRid();
        return true;
      }  // if
    }
    // deb
    // reset inner loop
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
