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
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  
  child_executor_->Init();
  TableMetadata *table_metadata = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  const std::vector<IndexInfo *> &indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata->name_);
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple,&rid)){
    table_metadata->table_->MarkDelete(rid,exec_ctx_->GetTransaction());

    for(auto index:indexes){
      Tuple index_tuple=tuple.KeyFromTuple(table_metadata->schema_,index->key_schema_,index->index_->GetKeyAttrs());
      LOG_INFO("delete index_tuple %s",index_tuple.ToString(&index->key_schema_).c_str());
      index->index_->DeleteEntry(index_tuple,rid,exec_ctx_->GetTransaction());
    }
    LOG_INFO("index update finished");
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
