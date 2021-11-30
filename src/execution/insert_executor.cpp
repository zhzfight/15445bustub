//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  TableHeap *table_heap = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  const std::vector<IndexInfo *> indexes =
      exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_);

  if (plan_->IsRawInsert()){

    for (auto raw:plan_->RawValues()) {
      Tuple tuple(raw,&exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_);
      RID rid;
      bool ok=table_heap->InsertTuple(tuple,&rid,exec_ctx_->GetTransaction());
      if (!ok){
        throw "out of table pagesize";
      }
      for(auto index_info:indexes){
        index_info->index_.get()->InsertEntry(tuple,rid,exec_ctx_->GetTransaction());
      }
    }

  }else{

    Tuple tuple;
    RID rid;
    child_executor_->Init();

    while(child_executor_->Next(&tuple, &rid)){
      TryExclusiveLock(rid);
      bool ok=table_heap->InsertTuple(tuple,&rid,exec_ctx_->GetTransaction());
      if (!ok){
        throw "out of table pagesize";
      }

      for(auto index_info:indexes){
        index_info->index_.get()->InsertEntry(tuple,rid,exec_ctx_->GetTransaction());
      }
    }
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
