//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      aht_end_(aht_.End()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    LOG_INFO("insert one");
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
  LOG_INFO("INIT END");
  aht_iterator_ = aht_.Begin();
  aht_end_ = aht_.End();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_end_) {
    LOG_INFO("aht iter==aht end");
    return false;
  }
  Value ok(BOOLEAN, 1);

  do {
    AggregateValue value = aht_iterator_.Val();
    AggregateKey key = aht_iterator_.Key();
    ++aht_iterator_;

    std::vector<Value> aggregate_values(GetOutputSchema()->GetColumnCount());
    LOG_INFO("AGGREGATE");
    for (size_t i = 0; i < GetOutputSchema()->GetColumnCount(); ++i) {
      aggregate_values[i] =
          GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_);
    }
    Tuple aggregate_tuple(aggregate_values, GetOutputSchema());
    LOG_INFO("AGGREGATE FINISHED");
    if (plan_->GetHaving()!= nullptr){

      ok = plan_->GetHaving()->EvaluateAggregate(key.group_bys_,value.aggregates_);

    }
    LOG_INFO("HAVING FINISHED");
    *tuple = aggregate_tuple;

  } while (!ok.GetAs<bool>() && aht_iterator_ != aht_end_);

  return ok.GetAs<bool>();
}

}  // namespace bustub
