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

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  id_txn_.emplace(txn_id, txn);
  std::unique_lock lock(latch_);

  // if set RU level
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  // if shrinking
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }

  // hold place for empty RQ
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue &rq = lock_table_.find(rid)->second;
  rq.insert({txn_id, LockMode::SHARED});

  // deadlock prevent
  if (rq.waiting_) {
    Prevent(txn_id, std::move(rq));
    rq.cv_.wait(lock, [&]() -> bool { return txn->GetState() == TransactionState::ABORTED || !rq.waiting_; });
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    rq.remove(txn_id);
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  txn->GetSharedLockSet()->emplace(rid);
  rq.refcount_++;
  if (auto request = rq.find(txn_id); request != nullptr) {
    request->granted_ = true;
  }

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  id_txn_.emplace(txn_id, txn);
  std::unique_lock lock(latch_);

  // if shrinking
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }

  // hold place for empty RQ
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue &rq = lock_table_.find(rid)->second;
  rq.insert({txn_id, LockMode::EXCLUSIVE});

  // deadlock prevent
  if (rq.waiting_ || rq.refcount_ > 0) {
    Prevent(txn_id, std::move(rq));
    rq.cv_.wait(lock, [&]() -> bool {
      return txn->GetState() == TransactionState::ABORTED || (!rq.waiting_ && rq.refcount_ == 0);
    });
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    rq.remove(txn_id);
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  rq.waiting_ = true;
  if (auto request = rq.find(txn_id); request != nullptr) {
    request->granted_ = true;
  }

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  std::unique_lock lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }

  LockRequestQueue &rq = lock_table_.find(rid)->second;
  if (rq.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }

  txn->GetSharedLockSet()->erase(rid);
  rq.refcount_--;
  if (auto request = rq.find(txn_id); request != nullptr) {
    request->lock_mode_ = LockMode::EXCLUSIVE;
    request->granted_ = false;
  }

  // deadlock prevent
  if (rq.waiting_ || rq.refcount_ > 0) {
    Prevent(txn_id, std::move(rq));
    rq.upgrading_ = true;
    rq.cv_.wait(lock, [&]() -> bool {
      return txn->GetState() == TransactionState::ABORTED || (!rq.waiting_ && rq.refcount_ == 0);
    });
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    rq.remove(txn_id);
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  rq.upgrading_ = false;
  rq.waiting_ = true;
  rq.find(txn_id)->granted_ = true;

  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  std::lock_guard guard(latch_);

  LockRequestQueue &rq = lock_table_.find(rid)->second;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // transfer `TransactionState`
  LockMode mode = rq.remove(txn_id).lock_mode_;
  if (!(mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  // notify others
  if (mode == LockMode::SHARED) {
    if (--rq.refcount_ == 0) {
      rq.cv_.notify_all();
    }
  } else {
    rq.waiting_ = false;
    rq.cv_.notify_all();
  }

  return true;
}

auto LockManager::Prevent(txn_id_t txn_id, LockRequestQueue &&request_queue) -> void {
  for (const auto &request : request_queue.request_queue_) {
    if (request.granted_ && request.txn_id_ == txn_id) {
      id_txn_[request.txn_id_]->SetState(TransactionState::ABORTED);
      if (request.lock_mode_ == LockMode::SHARED) {
        request_queue.refcount_--;
      } else {
        request_queue.waiting_ = false;
      }
    }
  }
}

auto LockManager::LockRequestQueue::find(txn_id_t txn_id) -> LockRequest * {
  for (auto &item : request_queue_) {
    if (item.txn_id_ == txn_id) return &item;
  }
  return nullptr;
}

auto LockManager::LockRequestQueue::remove(txn_id_t txn_id) -> LockRequest {
  for (const auto &item : request_queue_) {
    if (item.txn_id_ == txn_id) return item;
  }
  return {INVALID_TXN_ID, LockMode::SHARED};
}

}  // namespace bustub
