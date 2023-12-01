

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // get version list for key
  auto key_versions = mvcc_data_.find(key);
  if (key_versions == mvcc_data_.end()) {
    return false;
  }

  auto version_list = key_versions->second;
  if (version_list->empty()) {
    return false;
  }

  // Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
  auto latest_version = version_list->begin();
  for (auto itr = version_list->begin(); itr != version_list->end(); itr++) {
    if ((*itr)->version_id_ > (*latest_version)->version_id_ && (*itr)->version_id_ <= txn_unique_id) {
      // get latest version with timestamp less or equal to txn_unique_id
      latest_version = itr;
    }
  }
  
  // get value and update read timestamp
  *result = (*latest_version)->value_;
  if ((*latest_version)->max_read_id_ < txn_unique_id) {
    (*latest_version)->max_read_id_ = txn_unique_id;
  }
  return true;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  auto key_versions = mvcc_data_.find(key);
  
  // no key exist in version database, return true
  if (key_versions == mvcc_data_.end()) {
    return true;
  }

  auto version_list = key_versions->second;

  // Iterate the version_lists
  // get latest version where write timestamp less than or equal to txn_unique_id.
  auto valid_version = version_list->begin();
  for (auto itr = version_list->begin(); itr != version_list->end(); itr++) {
    if ((*itr)->version_id_ > (*valid_version)->version_id_ && (*itr)->version_id_ <= txn_unique_id) {
      // get latest version with timestamp less or equal to txn_unique_id
      valid_version = itr;
    }
  }

  // no valid version exists
  if ((*valid_version)->version_id_ > txn_unique_id) {
    return false;
  } else {
    return true;
  }
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // create version, assume malloc succeeds
  Version* to_write = new Version;
  to_write->value_ = value;
  to_write->version_id_ = txn_unique_id;
  to_write->max_read_id_ = txn_unique_id;

  // get key versions deque
  auto key_versions = mvcc_data_.find(key);

  if (key_versions == mvcc_data_.end()) {
    // no versions exists for key, insert first version
    deque<Version*>* versions = new deque<Version*>();
    versions->push_front(to_write);
    mvcc_data_[key] = versions;
    return;
  }

  // ELSE, versions exists for key
  auto version_list = key_versions->second;
  if (version_list->empty()) {
    // redundant check, check if version_list is empty
    version_list = new deque<Version*>();
  }

  version_list->push_front(to_write);

  // // Iterate the version_lists
  // // get latest version where write timestamp less than or equal to txn_unique_id.
  // auto latest_valid_version = version_list->begin();
  // for (auto itr = version_list->begin(); itr != version_list->end(); itr++) {
  //   if ((*itr)->version_id_ > (*latest_valid_version)->version_id_ && (*itr)->version_id_ <= txn_unique_id) {
  //     // get latest version with timestamp less or equal to txn_unique_id
  //     latest_valid_version = itr;
  //   }
  // }

  // if ((*latest_valid_version)->version_id_ == txn_unique_id) {
  //   // if same timestamp, update value
  //   (*latest_valid_version)->value_ = value;
  // } else 
    // else, create new version
}


