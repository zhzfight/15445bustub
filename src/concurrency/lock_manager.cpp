//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  if (txn->GetState()==TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  LockRequest lr(txn->GetTransactionId(), LockMode::SHARED);
  lock_table_[rid].request_queue_.push_back(lr);
  LockRequest *p;
  lock_table_[rid].cv_.wait(lk, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto iter = lock_table_[rid].request_queue_.begin(); iter != lock_table_[rid].request_queue_.end(); iter++) {
      if (iter->txn_id_ == txn->GetTransactionId()) {
        p = &(*iter);
        return true;
      } else if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return false;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  p->granted_ = true;
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {

  std::unique_lock<std::mutex> lk(latch_);
  if (txn->GetState()==TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  LockRequest lr(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_table_[rid].request_queue_.push_back(lr);

  lock_table_[rid].cv_.wait(lk, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto iter = lock_table_[rid].request_queue_.begin();
    if (iter->txn_id_ != txn->GetTransactionId()) {
      return false;
    }
    return true;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_table_[rid].request_queue_.begin()->granted_ = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // when i will upgrade the lock?
  std::unique_lock<std::mutex> lk(latch_);
  if (txn->GetState()==TransactionState::SHRINKING||txn->GetState()==TransactionState::ABORTED){
    return false;
  }
  if (lock_table_[rid].upgrading_) {
    return false;
  }
  lock_table_[rid].upgrading_ = true;
  auto src_iter = std::find_if(lock_table_[rid].request_queue_.begin(), lock_table_[rid].request_queue_.end(),
                               [&](LockRequest lr) { return lr.txn_id_ == txn->GetTransactionId(); });
  src_iter->lock_mode_ = LockMode::EXCLUSIVE;
  src_iter->granted_ = false;
  auto des_iter = std::find_if(src_iter, lock_table_[rid].request_queue_.end(), [&](LockRequest lr) {
    return lr.lock_mode_ == LockMode::EXCLUSIVE || lr.granted_ == false;
  });
  lock_table_[rid].request_queue_.splice(des_iter, lock_table_[rid].request_queue_, src_iter);
  lock_table_[rid].cv_.wait(lk, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto iter = lock_table_[rid].request_queue_.begin();
    if (iter->txn_id_ != txn->GetTransactionId()) {
      return false;
    }
    return true;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lock_table_[rid].request_queue_.begin()->granted_ = true;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  auto iter = lock_table_[rid].request_queue_.begin();
  auto end = lock_table_[rid].request_queue_.end();
  for (; iter != end; iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_table_[rid].request_queue_.erase(iter);
      break;
    }
  }
  if (iter == end) {
    return false;
  }
  if (txn->GetState()!=TransactionState::ABORTED){
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  lock_table_[rid].cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
