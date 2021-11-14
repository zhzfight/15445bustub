//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <execution/executor_factory.h>
#include <execution/executors/index_scan_executor.h>

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_name(exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->name_),
      child_executor_(std::move(child_executor)),
      index_info(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexName(), table_name)) {}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;
  std::vector<Value> join_value(GetOutputSchema()->GetColumnCount());
  while (child_executor_->Next(&left_tuple, &left_rid)) {

    Tuple index_tuple = left_tuple.KeyFromTuple(*plan_->OuterTableSchema(), index_info->key_schema_,index_info->index_->GetKeyAttrs());
    std::vector<RID> result;
    index_info->index_->ScanKey(index_tuple,&result,exec_ctx_->GetTransaction());
    if (result.size()!=0){
      exec_ctx_->GetCatalog()->GetTable(table_name)->table_->GetTuple(result[0],&right_tuple,exec_ctx_->GetTransaction());
      for (uint32_t i = 0; i < GetOutputSchema()->GetColumnCount(); ++i) {
        Value value=GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(&left_tuple,plan_->OuterTableSchema(),&right_tuple,plan_->InnerTableSchema());
        join_value[i]=value;
      }
      Tuple join_tuple=Tuple(join_value,GetOutputSchema());
      *tuple=join_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
