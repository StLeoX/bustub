//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cur_{nullptr, RID(), nullptr},
      end_{nullptr, RID(), nullptr},
      schema_idx_array_() {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  if (auto predicate = plan_->GetPredicate(); predicate != nullptr)
    predicate_ = predicate;
  else
    predicate_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
}

void SeqScanExecutor::Init() {
  cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_->End();
  auto schema = plan_->OutputSchema();
  try {
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      auto column_name = schema->GetColumn(i).GetName();
      // named schema, located by name
      schema_idx_array_.push_back(table_info_->schema_.GetColIdx(column_name));  // might `UNREACHABLE`, but throw once
    }
  } catch (const std::logic_error &_) {
    schema_idx_array_.clear();
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      // unnamed schema,  located by column number
      schema_idx_array_.push_back(i);
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cur_ != end_) {
    //    LOG_DEBUG("rid: %ld \n", cur_->GetRid().Get());
    auto cur = cur_++;
    auto value = predicate_->Evaluate(&(*cur), &table_info_->schema_);
    // check the predicate result
    if (value.GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(schema_idx_array_.size());
      for (auto idx : schema_idx_array_) {
        values.push_back(cur->GetValue(&table_info_->schema_, idx));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = cur->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
