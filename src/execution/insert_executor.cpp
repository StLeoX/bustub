//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) child_executor_->Init();
  next_pos_ = 0;
  table_idx_array_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool success = false;
  if (plan_->IsRawInsert()) {
    if (next_pos_ == plan_->RawValues().size()) {
      success = false;
    } else {
      auto &values = plan_->RawValues();
      *tuple = Tuple(values[next_pos_++], &table_info_->schema_);
      success = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  } else {
    if (child_executor_->Next(tuple, rid)) {
      success = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  }
  // update all index_info of the table into which tuples are inserted
  for (auto &index_info : table_idx_array_) {
    const auto &key =
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
  }
  return success;
}

}  // namespace bustub
