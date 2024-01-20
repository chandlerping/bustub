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
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);

  auto is_compatible = [&] {
    for (auto &request : lock_table_[rid].request_queue_) {
      if (request.txn_id_ == txn->GetTransactionId()) {
        return true;
      }
      if (request.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return true;
  };

  lock_table_[rid].cv_.wait(lock, [&is_compatible, &txn]() -> bool { return is_compatible() || txn->GetState() == TransactionState::ABORTED; });

  txn->GetSharedLockSet()->emplace(rid);

  lock_table_[rid].cv_.notify_all();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  std::cout << "L E " << txn->GetTransactionId() << std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  auto is_compatible = [&] {
    return lock_table_[rid].request_queue_.size() <= 1;
  };

  lock_table_[rid].cv_.wait(lock, [&is_compatible, &txn]() -> bool { return is_compatible() || txn->GetState() == TransactionState::ABORTED; });

  txn->GetExclusiveLockSet()->emplace(rid);

  lock_table_[rid].cv_.notify_all();
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  if (txn->GetState() != TransactionState::ABORTED) {
    txn->SetState(TransactionState::SHRINKING);
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t> v;
  for (auto val : waits_for_[t1]) {
    if (val != t2) {
      v.push_back(val);
    }
  }
  waits_for_[t1] = v;
  // waits_for_[t1].erase(std::remove(waits_for_[t1].begin(), waits_for_[t1].end(), t2), waits_for_[t1].end());
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  for (const auto& pair : waits_for_) {
    std::unordered_map<txn_id_t, int> vis;
    txn_id_t t1 = pair.first;
    if (vis[t1] == 0) {
      std::vector<txn_id_t> status;
      txn_id_t res = dfs(t1, vis, status);
      if (res != -1) {
        *txn_id = res;
        return true;
      }
    }
  }
  return false;
}

txn_id_t LockManager::dfs(txn_id_t t1, std::unordered_map<txn_id_t, int> &vis, std::vector<txn_id_t> &status) {
  vis[t1] = 1;
  status.push_back(t1);
  for (auto t2 : waits_for_[t1]) {
    if (vis[t2] == 1) {
      size_t l = 0;
      while (status[l] != t2) {
        l++;
      }
      txn_id_t ans = status[l];
      while (l < status.size()) {
        ans = std::max(ans, status[l]);
        l++;
      }
      return ans;
    }
    txn_id_t res = dfs(t2, vis, status);
    if (res != -1) {
      return res;
    }
  }
  status.pop_back();
  return -1;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  for (const auto& pair : waits_for_) {
    txn_id_t t1 = pair.first;
    for (txn_id_t t2 : pair.second) {
      res.emplace_back(t1, t2);
    }
  }
  return res;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      // continue;
      std::cout << "RunCycleDetection" << std::endl;
      for (const auto& item : lock_table_) {
        std::vector<txn_id_t> ids;
        for (auto queue_item : item.second.request_queue_) {
          txn_id_t id = queue_item.txn_id_;
          for (auto old_id : ids) {
            std::cout << "add edge " << id << " " << old_id << std::endl;
            AddEdge(id, old_id);
          }
          ids.push_back(id);
        }
      }

      txn_id_t *txn_id = new txn_id_t;
      if (HasCycle(txn_id)) {
        std::cout << "Has cycle " << *txn_id << std::endl;
        for (auto neighbor_id : waits_for_[*txn_id]) {
          std::cout << "remove edge" << std::endl;
          RemoveEdge(*txn_id, neighbor_id);
        }
        for (auto& lock_table_item : lock_table_) {
          auto itr = lock_table_item.second.request_queue_.begin();

          while (itr != lock_table_item.second.request_queue_.end()) {
            if (itr->txn_id_ == *txn_id) {
              itr = lock_table_item.second.request_queue_.erase(itr);
            } else {
              itr++;
            }
          }
          lock_table_[lock_table_item.first].cv_.notify_all();
        }
        Transaction *txn = TransactionManager::GetTransaction(*txn_id);
        txn->SetState(TransactionState::ABORTED);
        std::cout << "State " << static_cast<int>(txn->GetState()) << std::endl;
      }
    }
  }
}
}  // namespace bustub
