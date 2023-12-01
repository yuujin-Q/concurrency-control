
#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>
#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

bool LOGGING = false;

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1)
{
  if (mode_ == LOCKING)
    lm_ = new LockManagerA(&ready_txns_);
  // Create the storage
  if (mode_ == MVCC)
  {
    storage_ = new MVCCStorage();
  }
  else
  {
    storage_ = new Storage();
  }

  storage_->InitStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  for (int i = 0; i < 7; i++)
  {
    CPU_SET(i, &cpuset);
  }
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void *>(this));
}

void *TxnProcessor::StartScheduler(void *arg)
{
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor()
{
  if (mode_ == LOCKING)
    delete lm_;

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn *txn)
{
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn *TxnProcessor::GetTxnResult()
{
  Txn *txn;
  while (!txn_results_.Pop(&txn))
  {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler()
{
  switch (mode_)
  {
  case SERIAL:
    RunSerialScheduler();
    break;
  case LOCKING:
    RunLockingScheduler();
    break;
  case OCC:
    RunOCCScheduler();
    break;
  case MVCC:
    RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler()
{
  Txn *txn;
  while (tp_.Active())
  {
    // Get next txn request.
    if (txn_requests_.Pop(&txn))
    {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C)
      {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      }
      else if (txn->Status() == COMPLETED_A)
      {
        txn->status_ = ABORTED;
      }
      else
      {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler()
{
  Txn *txn;
  while (tp_.Active())
  {
    // Take transaction from requests
    if (txn_requests_.Pop(&txn))
    {
      // for the transaction taken, we run it on a new thread
      // so that each transaction runs on their own thread
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(
          this,
          &TxnProcessor::ProcessTxn,
          txn));
    }
  }
}

void TxnProcessor::ProcessTxn(Txn *txn)
{
  // we process the readset
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it)
  {
    mutex_.Lock();
    bool lockObtained = lm_->ReadLock(txn, *it);
    mutex_.Unlock();
    // we block the transaction while the lock is not obtained
    if (LOGGING)
    {
      printf("[%ld] Acquiring read lock for readset for key: %ld \n", txn->unique_id_, *it);
    }
    while (!lockObtained)
    {
      vector<Txn *> owners;
      mutex_.Lock();
      // we check the current owner of the lock
      lm_->Status(*it, &owners);
      if (owners.size() > 0)
      {
        // if the owner is an "older" transaction / arrives later
        if (owners[0]->unique_id_ < txn->unique_id_)
        {
          if (LOGGING)
            printf("[%ld] Rolling back \n", txn->unique_id_);
          // we rollback this transaction at once
          this->ReleaseLocks(txn);
          it = txn->readset_.begin();
          mutex_.Unlock();
          break;
        }
      }
      lockObtained = lm_->ReadLock(txn, *it);
      mutex_.Unlock();
    };
    if (LOGGING)
    {
      printf("[%ld] Successfully acquired read lock [%ld]\n", txn->unique_id_, *it);
    }
  }
  // now we process the writesets
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it)
  {
    if (LOGGING)
    {
      printf("[%ld] Acquiring write lock for writeset for key: %ld\n", txn->unique_id_, *it);
    }
    mutex_.Lock();
    bool lockObtained = lm_->WriteLock(txn, *it);
    mutex_.Unlock();
    while (!lockObtained) // we block the process while the lock is not obtained
    {
      mutex_.Lock();
      vector<Txn *> owners;
      lm_->Status(*it, &owners);
      // we check the current owner of the lock
      if (owners.size() > 0)
      {
        if (owners[0]->unique_id_ < txn->unique_id_)
        {
          // rollback if the owner is a younger transaction
          if (LOGGING)
            printf("[%ld] Rolling back \n", txn->unique_id_);
          this->ReleaseLocks(txn);
          it = txn->writeset_.begin();
          mutex_.Unlock();
          break;
        }
      }
      lockObtained = lm_->WriteLock(txn, *it);
      mutex_.Unlock();
    }
    if (LOGGING)
    {
      printf("[%ld] Successfully acquired write lock [%ld]\n", txn->unique_id_, *it);
    }
  }
  // at this point we have obtained all the lock for the txn we need so we can execute it
  this->ExecuteTxn(txn);
  // Commit/abort txn according to program logic's commit/abort decision.

  TxnStatus status = txn->Status();
  // we commit the transaction
  if (status == COMPLETED_C)
  {
    if (LOGGING)
    {
      printf("[!] Changing the status of transaction %ld to COMMITED\n", txn->unique_id_);
    }
    ApplyWrites(txn);
    txn->status_ = COMMITTED;
  }
  else if (status == COMPLETED_A)
  {
    txn->status_ = ABORTED;
  }
  else
  {
    // Invalid TxnStatus!
    DIE("Completed Txn has invalid TxnStatus: " << status);
  }

  // Release read locks.
  mutex_.Lock();
  this->ReleaseLocks(txn);
  mutex_.Unlock();

  // Return result to client.
  txn_results_.Push(txn);
  if (LOGGING)
  {
    printf("[!] Finished pusing to client\n");
  }
}

void TxnProcessor::ReleaseLocks(Txn *txn)
{
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it)
  {
    if (LOGGING)
    {
      printf("[%ld] Releasing lock: %ld \n", txn->unique_id_, *it);
    }
    lm_->Release(txn, *it);
  }

  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it)
  {
    if (LOGGING)
    {
      printf("[%ld] Releasing lock: %ld \n", txn->unique_id_, *it);
    }
    lm_->Release(txn, *it);
  }
}

void TxnProcessor::ExecuteTxn(Txn *txn)
{

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it)
  {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it)
  {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
  if (LOGGING)
  {
    printf("[!] Current completed txns count: %d\n", completed_txns_.Size());
  }
}

void TxnProcessor::ApplyWrites(Txn *txn)
{
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it)
  {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler()
{
  // Serial OCC/Validation-Based Protocol
  Txn *txn;

  // check for active transaction requests in pool
  while (tp_.Active())
  {
    // get next new transaction request
    if (txn_requests_.Pop(&txn))
    {
      // transaction is pending, pass to exec thread
      txn->occ_start_time_ = GetTime();

      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(this, &TxnProcessor::ExecuteTxn, txn));
    }

    // check completed transactions (not committed/aborted)
    while (completed_txns_.Pop(&txn))
    {
      bool validationFailed = false;

      // validation phase, check for transaction validity
      // check timestamp for each record whose key appears in the txn s read and write sets

      // check readset
      for (auto itr = txn->readset_.begin(); itr != txn->readset_.end(); itr++)
      {
        double recordUpdateTime = storage_->Timestamp(*itr);

        // check if the record was last updated AFTER this transaction s start time
        // valid condition: data_modify_time < txn_start_time
        if (recordUpdateTime > txn->occ_start_time_)
        {
          // failed validation
          validationFailed = true;
          break;
        }
      }

      // check writeset
      for (auto itr = txn->writeset_.begin(); itr != txn->writeset_.end(); itr++)
      {
        double recordUpdateTime = storage_->Timestamp(*itr);

        // check if the record was last updated AFTER this transaction s start time
        // valid condition: data_modify_time < txn_start_time
        if (recordUpdateTime > txn->occ_start_time_)
        {
          // failed validation
          validationFailed = true;
          break;
        }
      }

      // DECISION: abort/commit
      if (validationFailed)
      {
        // ABORT transaction, RESTART transaction
        // cleanup txn
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;

        // restart txn
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
      }
      else
      {
        // COMMIT
        // write to storage
        txn->status_ = COMPLETED_C;
        ApplyWrites(txn);

        // set as commited, push to result
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
      }
    }
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn* txn) {
  // Read all necessary data for this transaction from storage 
  //    (Note that unlike the version of MVCC from class, you should lock the key before each read)
  // read for readset
  for (auto read_key : txn->readset_) {
    Value result;
    storage_->Lock(read_key);
    if (storage_->Read(read_key, &result)) {
      txn->reads_[read_key] = result;
    }
    storage_->Unlock(read_key);
  }

  // read for writeset
  for (auto write_key : txn->writeset_) {
    Value result;
    storage_->Lock(write_key);
    if (storage_->Read(write_key, &result)) {
      txn->reads_[write_key] = result;
    }
    storage_->Unlock(write_key);
  }

  // Execute the transaction logic (i.e. call Run() on the transaction)
  txn->Run();

  // Acquire all locks for keys in the write_set_
  // Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  bool passed = true;
  for (auto write_key : txn->writeset_) {
    storage_->Lock(write_key);
    if (!storage_->CheckWrite(write_key, txn->unique_id_)) {
      passed = false;
      break;
    }
  }

  // check if writes valid
  if (passed) {
    // passed, Apply the writes
    txn->status_ = COMPLETED_C;
    ApplyWrites(txn);
    txn->status_ = COMMITTED;    
    txn_results_.Push(txn);
  } else {
    // cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // completely restart the transaction
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock(); 
  }

  // Release all locks for keys in the write_set_
  for (auto keys : txn->writeset_) {
    storage_->Unlock(keys);
  }
}

void TxnProcessor::RunMVCCScheduler() {
  // MVCC
  Txn *txn;

  // check for active transaction requests in pool
  // Pop a txn from txn_requests_, and pass it to a thread to execute. 
  while (tp_.Active()) {
    // get next new transaction request 
    if (txn_requests_.Pop(&txn)) {
      // transaction is pending, pass to exec thread
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(this, &TxnProcessor::MVCCExecuteTxn, txn));
    }
  }
}

