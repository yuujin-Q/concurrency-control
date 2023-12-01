
#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>
#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

bool LOGGING = true;

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
  case P_OCC:
    RunOCCParallelScheduler();
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
    // printf("[!] Request txn size: %d\n", txn_requests_.Size());
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn))
    {
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(
          this,
          &TxnProcessor::ProcessTxn,
          txn));
    }
  }
}

void TxnProcessor::ProcessTxn(Txn *txn)
{
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it)
  {
    if (LOGGING)
    {
      printf("[%ld] Acquiring read lock for readset for key: %ld \n", txn->unique_id_, *it);
    }
    mutex_.Lock();
    bool lockObtained = lm_->ReadLock(txn, *it);
    mutex_.Unlock();
    while (!lockObtained)
    {
      vector<Txn *> owners;
      mutex_.Lock();
      lm_->Status(*it, &owners);
      if (owners.size() > 0)
      {
        if (owners[0]->unique_id_ > txn->unique_id_)
        {
          // rollback
          for (set<Key>::iterator it = txn->readset_.begin();
               it != txn->readset_.end(); ++it)
          {
            if (LOGGING)
            {
              printf("[%ld] Releasing lock due to rollback: %ld \n", txn->unique_id_, *it);
            }
            lm_->Release(txn, *it);
          }

          for (set<Key>::iterator it = txn->writeset_.begin();
               it != txn->writeset_.end(); ++it)
          {
            if (LOGGING)
            {
              printf("[%ld] Releasing lock due to rollback: %ld \n", txn->unique_id_, *it);
            }
            lm_->Release(txn, *it);
          }
          it = txn->readset_.begin();
          mutex_.Unlock();
          break;
        }
      }
      lockObtained = lm_->ReadLock(txn, *it);
      mutex_.Unlock();
    };
  }

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
    while (!lockObtained)
    {
      mutex_.Lock();
      vector<Txn *> owners;
      lm_->Status(*it, &owners);
      if (owners.size() > 0)
      {
        if (owners[0]->unique_id_ > txn->unique_id_)
        {
          // rollback
          for (set<Key>::iterator it = txn->readset_.begin();
               it != txn->readset_.end(); ++it)
          {
            // printf("[!] Releasing lock: %ld \n", *it);
            lm_->Release(txn, *it);
          }

          for (set<Key>::iterator it = txn->writeset_.begin();
               it != txn->writeset_.end(); ++it)
          {
            // printf("[!] Releasing lock: %ld from transaction: %ld\n", *it, txn->unique_id_);
            lm_->Release(txn, *it);
          }
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
  this->ExecuteTxn(txn);
  // Commit/abort txn according to program logic's commit/abort decision.

  TxnStatus status = txn->Status();
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
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it)
  {
    lm_->Release(txn, *it);
  }
  // Release write locks.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it)
  {
    if (LOGGING)
    {
      printf("[!] Releasing write lock from transaction %ld for key %ld\n", txn->unique_id_, *it);
    }
    lm_->Release(txn, *it);
  }
  mutex_.Unlock();
  // Return result to client.
  txn_results_.Push(txn);
  if (LOGGING)
  {
    printf("[!] Finished pusing to client\n");
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
    printf("[!] Current completed txns count: %d", completed_txns_.Size());
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
  //
  // Implement this method!
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  RunSerialScheduler();
}

void TxnProcessor::RunOCCParallelScheduler()
{
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCScheduler()
{
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}
