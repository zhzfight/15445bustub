//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_engine.h
//
// Identification: src/include/execution/execution_engine.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
namespace bustub {
class ExecutionEngine {
 public:
  ExecutionEngine(BufferPoolManager *bpm, TransactionManager *txn_mgr, Catalog *catalog)
      : bpm_(bpm), txn_mgr_(txn_mgr), catalog_(catalog) {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);

  bool Execute(const AbstractPlanNode *plan, std::vector<Tuple> *result_set, Transaction *txn,
               ExecutorContext *exec_ctx) {
    // construct executor
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

    // prepare
    LOG_INFO("init1");
    executor->Init();
    LOG_INFO("init2");
    // execute
    try {
      Tuple tuple;
      RID rid;

      while (executor->Next(&tuple, &rid)) {

        if (result_set != nullptr) {
          result_set->push_back(tuple);
        }
        if (exec_ctx->GetTransaction()->GetIsolationLevel()==IsolationLevel::READ_COMMITTED){
          executor->Unlock(rid);
        }
      }
      if (exec_ctx->GetTransaction()->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ){
        for(auto iter=exec_ctx->GetTransaction()->GetSharedLockSet()->begin();iter!=exec_ctx->GetTransaction()->GetSharedLockSet()->end();){
          executor->Unlock(*iter++);
        }
        for(auto iter=exec_ctx->GetTransaction()->GetExclusiveLockSet()->begin();iter!=exec_ctx->GetTransaction()->GetExclusiveLockSet()->end();){
          executor->Unlock(*iter++);
        }
      }





    } catch (Exception &e) {
      // TODO(student): handle exceptions

    }
    LOG_INFO("HERE2");
    return true;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] TransactionManager *txn_mgr_;
  [[maybe_unused]] Catalog *catalog_;
};

}  // namespace bustub
