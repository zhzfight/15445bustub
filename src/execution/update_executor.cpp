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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  const std::vector<IndexInfo *> &indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while(child_executor_->Next(&tuple,&rid)){
    Tuple new_tuple= GenerateUpdatedTuple(tuple);
    table_info_->table_->UpdateTuple(new_tuple,rid,exec_ctx_->GetTransaction());
    for (auto index_info :indexes) {
      index_info->index_->DeleteEntry(tuple,rid,exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_tuple,rid,exec_ctx_->GetTransaction());
    }
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { return false; }
}  // namespace bustub
