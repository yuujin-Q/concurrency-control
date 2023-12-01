
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.
#include "lock_manager.h"
#include "txn.h"
using std::deque;
using std::make_pair;

LockManagerA::LockManagerA(deque<Txn *> *ready_txns)
{
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn *txn, const Key &key)
{
  // look up if the key is being locked
  auto it = lock_table_.find(key);
  auto waiting_tx_it = txn_waits_.find(txn);
  if (it == lock_table_.end() || it->second->empty()) // not found
  {
    // lock the key
    LockRequest new_lock_request = LockRequest(EXCLUSIVE, txn);
    deque<LockRequest> *lock_requests = new deque<LockRequest>;
    lock_requests->push_back(new_lock_request);
    lock_table_.insert(make_pair(key, lock_requests));
    if (waiting_tx_it != txn_waits_.end()) // delete from waiting tx
    {
      txn_waits_.erase(waiting_tx_it);
    }
    return true;
  }
  vector<Txn *> owners = vector<Txn *>{};
  LockMode mode = this->Status(key, &owners);
  if (mode == SHARED && owners.size() == 1 && owners[0] == txn)
  {
    this->Release(txn, key);
    deque<LockRequest> *lock_requests = it->second;
    lock_requests->push_back(LockRequest(EXCLUSIVE, txn)); // add to lock requests in the key
    if (waiting_tx_it != txn_waits_.end())                 // delete from waiting tx
    {
      txn_waits_.erase(waiting_tx_it);
    }
    return true;
  }
  if (owners.size() > 0)
  {
    if (owners[0] == txn && mode == EXCLUSIVE)
    {
      return true;
    }
  }
  return false;
}

bool LockManagerA::ReadLock(Txn *txn, const Key &key)
{
  // look up if the key is being locked
  auto it = lock_table_.find(key);
  auto waiting_tx_it = txn_waits_.find(txn);
  if (it == lock_table_.end()) // not found
  {
    // lock the key
    LockRequest new_lock_request = LockRequest(SHARED, txn);
    deque<LockRequest> *lock_requests = new deque<LockRequest>;
    lock_requests->push_front(new_lock_request);
    lock_table_.insert(make_pair(key, lock_requests));
    if (waiting_tx_it != txn_waits_.end()) // delete from waiting tx
    {

      txn_waits_.erase(waiting_tx_it);
    }
    return true;
  }
  if (this->Status(key, new vector<Txn *>) != EXCLUSIVE || it->second->empty())
  {
    LockRequest new_lock_request = LockRequest(SHARED, txn);

    it->second->push_front(new_lock_request);
    if (waiting_tx_it != txn_waits_.end()) // delete from waiting tx
    {
      txn_waits_.erase(waiting_tx_it);
    }
    return true;
  }
  return false;
}

void LockManagerA::Release(Txn *txn, const Key &key)
{
  auto it = lock_table_.find(key);
  if (it != lock_table_.end())
  {
    deque<LockRequest> *lock_requests = it->second;
    for (auto req_it = lock_requests->begin(); req_it != lock_requests->end();)
    {
      if (req_it->txn_ == txn)
      {

        if (req_it != lock_requests->begin())
        {
          auto waiting_tx_it = txn_waits_.find(txn);
          if (waiting_tx_it != txn_waits_.end())
          {
            txn_waits_.erase(waiting_tx_it);
          }
        }
        lock_requests->erase(req_it);
      }
      else
      {
        ++req_it;
      }
    }
  }
  // Implement this method!
}

LockMode LockManagerA::Status(const Key &key, vector<Txn *> *owners)
{
  owners->clear();
  auto it = lock_table_.find(key);
  if (it != lock_table_.end())
  {
    deque<LockRequest> *lock_requests = it->second;
    if (it->second->empty())
    {
      return UNLOCKED;
    }
    LockMode mode;
    bool foundExclusive = false;
    for (auto lock_it = lock_requests->begin(); lock_it != lock_requests->end(); lock_it++)
    {
      if (lock_it->mode_ == EXCLUSIVE)
      // we dont cound exclusive locks if it s not the first one in the queue
      {
        foundExclusive = true;
      }
      else
      {
        mode = lock_it->mode_;
        owners->push_back(lock_it->txn_);
      }
    }
    if (foundExclusive && owners->empty())
    {
      owners->push_back(lock_requests->begin()->txn_);
      mode = EXCLUSIVE;
    }
    return mode;
  }
  // Implement this method!
  return UNLOCKED;
}