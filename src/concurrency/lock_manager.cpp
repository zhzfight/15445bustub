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
#include <set>
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  if (txn->GetState() == TransactionState::SHRINKING) {
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
  if (txn->GetState() == TransactionState::SHRINKING) {
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
  if (txn->GetState() == TransactionState::SHRINKING || txn->GetState() == TransactionState::ABORTED) {
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
  if (txn->GetState() != TransactionState::ABORTED) {
    txn->SetState(TransactionState::SHRINKING);
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  lock_table_[rid].cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    LOG_INFO("add edge %d to %d", t1, t2);
    waits_for_[t1].push_back(t2);
    return;
  }
  if (std::find_if(waits_for_[t1].begin(), waits_for_[t1].end(), [&](txn_id_t t) { return t == t2; }) ==
      waits_for_[t1].end()) {
    LOG_INFO("add edge %d to %d", t1, t2);
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    LOG_INFO("no edge %d to %d", t1, t2);
    return;
  }
  auto iter = std::find_if(waits_for_[t1].begin(), waits_for_[t1].end(), [&](txn_id_t t) { return t == t2; });
  if (iter != waits_for_[t1].end()) {
    LOG_INFO("remove %d to %d", t1, t2);
    waits_for_[t1].erase(iter);
    return;
  }
  LOG_INFO("no edge %d to %d", t1, t2);
}

bool LockManager::dfs(txn_id_t cur, std::unordered_set<txn_id_t> &visited) {
  visited.insert(cur);
  while (!waits_for_[cur].empty()) {
    auto lowest = std::min_element(waits_for_[cur].begin(), waits_for_[cur].end());
    if (visited.count(*lowest) != 0) {
      return true;
    }
    if (dfs(*lowest, visited)) {
      return true;
    }
    RemoveEdge(cur, *lowest);
  }
  visited.erase(cur);
  return false;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::unordered_set<txn_id_t> visited;
  auto iter = vertex.begin();
  if (dfs(*iter, visited)) {
    auto youngest = std::max_element(visited.begin(), visited.end());
    *txn_id = *youngest;
    return true;
  }
  LOG_INFO("vertex erase %d", *vertex.begin());
  vertex.erase(vertex.begin());
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edge_list;
  for (auto p2ps : waits_for_) {
    for (auto to : p2ps.second) {
      edge_list.push_back({p2ps.first, to});
    }
  }
  return edge_list;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here

      // build the graph
      LOG_INFO("build the graph");
      for (auto iter = lock_table_.begin(); iter != lock_table_.end(); iter++) {
        std::unordered_set<txn_id_t> hold_lock;
        std::unordered_set<txn_id_t> wait_lock;
        for (auto rlq_iter = iter->second.request_queue_.begin(); rlq_iter != iter->second.request_queue_.end();
             rlq_iter++) {
          if (rlq_iter->granted_) {
            hold_lock.insert(rlq_iter->txn_id_);
          } else {
            wait_lock.insert(rlq_iter->txn_id_);
          }
        }
        for (auto from : wait_lock) {
          for (auto to : hold_lock) {
            if (TransactionManager::GetTransaction(from)->GetState() == TransactionState::ABORTED ||
                TransactionManager::GetTransaction(to)->GetState() == TransactionState::ABORTED) {
              continue;
            }
            AddEdge(from, to);
          }
        }
      }
      LOG_INFO("graph finished");
      for (auto iter = waits_for_.begin(); iter != waits_for_.end(); iter++) {
        vertex.insert(iter->first);
      }
      for (auto ver : vertex) {
        LOG_INFO("%d", ver);
      }
      LOG_INFO("HERE");
      txn_id_t abort_txn_id;
      while (!vertex.empty() && HasCycle(&abort_txn_id)) {
        LOG_INFO("has cycle");
        TransactionManager::GetTransaction(abort_txn_id)->SetState(TransactionState::ABORTED);


        waits_for_.erase(abort_txn_id);
      }
      LOG_INFO("HERE2");
    }
    waits_for_.clear();
    vertex.clear();
  }
}

}  // namespace bustub
