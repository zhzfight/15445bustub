//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_(plan),table_name_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_){
}

void IndexScanExecutor::Init() {
  if (plan_->GetPredicate()!= nullptr){
    iter=(reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>*>
     (exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get()))
               ->GetBeginIterator(plan_->GetPredicate()->Evaluate(nullptr, nullptr).GetAs<GenericKey<8>>());
  }else{
    iter=(reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>*>
            (exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get()))->GetBeginIterator();
  }
  end=(reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>*>
         (exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get()))->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter==end){
    return false;
  }
  auto pair=*iter;
  ++iter;
  *rid=pair.second;
  exec_ctx_->GetCatalog()->GetTable(table_name_)->table_.get()->GetTuple(*rid,tuple,exec_ctx_->GetTransaction());
  return true;
}

}  // namespace bustub
