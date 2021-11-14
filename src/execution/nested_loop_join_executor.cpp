//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <execution/expressions/column_value_expression.h>

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),left_executor_(std::move(left_executor)),right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  LOG_INFO("LEFT INIT START");
  left_executor_->Init();
  LOG_INFO("LEFT INIT END");
  LOG_INFO("RIGHT INIT START");
  right_executor_->Init();
  LOG_INFO("RIGHT INIT END");
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {

  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;
  std::vector<Value> res(GetOutputSchema()->GetColumnCount());
  
  Value ok(BOOLEAN,0);
  while (left_executor_->Next(&left_tuple,&left_rid)){
    LOG_INFO("HERE444");
    while (right_executor_->Next(&right_tuple,&right_rid)){
      LOG_INFO("HERE333");
      ok=plan_->Predicate()->EvaluateJoin(&left_tuple,plan_->GetLeftPlan()->OutputSchema(),&right_tuple,plan_->GetRightPlan()->OutputSchema());
      if (ok.GetAs<bool>()){
        for (uint32_t i = 0; i < GetOutputSchema()->GetColumnCount(); ++i) {
          Value value=GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(&left_tuple,plan_->GetLeftPlan()->OutputSchema(),&right_tuple,plan_->GetRightPlan()->OutputSchema());
          res[i]=value;
        }

        Tuple join_tuple(res,GetOutputSchema());
        *tuple=join_tuple;
        return true;
      }
    }
    right_executor_->Init();
  }
  
  return false;

}

}  // namespace bustub
