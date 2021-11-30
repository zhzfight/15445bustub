//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  LOG_INFO("ITER INIT");
  iter = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
  LOG_INFO("END INIT");
  end = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  LOG_INFO("child find next");
  if (iter == end) {
    LOG_INFO("iter=end");
    return false;
  }
  Value ok(BOOLEAN, 1);

  do {
    if (GetExecutorContext()->GetTransaction()->GetIsolationLevel()!=IsolationLevel::READ_UNCOMMITTED){
      TryShardLock(iter->GetRid());
    }
    *tuple = *iter++;

    if (plan_->GetPredicate() != nullptr) {
      ok = plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema());
    }

  } while (!ok.GetAs<bool>() && iter != end);
  if (ok.GetAs<bool>()) {
    *rid = tuple->GetRid();
  }

  return ok.GetAs<bool>();
}

}  // namespace bustub
