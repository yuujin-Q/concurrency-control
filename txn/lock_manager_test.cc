#include "lock_manager.h"

#include <set>
#include <string>

#include "../utils/testing.h"

using std::set;

TEST(LockManagerA_SimpleLocking)
{
  deque<Txn *> ready_txns;
  LockManagerA lm(&ready_txns);
  vector<Txn *> owners;

  Txn *t1 = reinterpret_cast<Txn *>(1);
  Txn *t2 = reinterpret_cast<Txn *>(2);
  Txn *t3 = reinterpret_cast<Txn *>(3);

  // Txn 1 acquires read lock.
  lm.ReadLock(t1, 101);
  ready_txns.push_back(t1); // Txn 1 is ready.
  EXPECT_EQ(SHARED, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());
  EXPECT_EQ(t1, ready_txns.at(0));

  // Txn 2 requests write lock. Not granted.
  lm.WriteLock(t2, 101);
  EXPECT_EQ(SHARED, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 3 requests read lock. Granted.
  lm.ReadLock(t3, 101);
  ready_txns.push_back(t3); // Txn 1 is ready.
  EXPECT_EQ(SHARED, lm.Status(101, &owners));
  EXPECT_EQ(2, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(2, ready_txns.size());

  // Txn 1 releases lock.  Txn 2 is granted write lock.
  lm.Release(t1, 101);
  ready_txns.pop_front();
  EXPECT_EQ(SHARED, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(1, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.at(0));

  lm.Release(t3, 101);
  ready_txns.pop_front();
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t2, owners[0]);
  EXPECT_EQ(1, ready_txns.size());
  EXPECT_EQ(t2, ready_txns.at(0));

  // Txn 2 releases lock.  Txn 3 is granted read lock.
  lm.Release(t2, 101);
  EXPECT_EQ(UNLOCKED, lm.Status(101, &owners));
  EXPECT_EQ(0, owners.size());
  END;
}

int main(int argc, char **argv)
{
  LockManagerA_SimpleLocking();
}
