//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // new dir page
  auto dir_page = buffer_pool_manager->NewPage(&directory_page_id_);
  if (dir_page == nullptr) {
    LOG_WARN("NewDirectoryPage fail");
    return;
  }
  auto dir = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  dir->SetPageId(directory_page_id_);  // self-contained

  // Initially, there should exist two buckets.
  // Note: This might be different to "initializing an empty directory".
  page_id_t bucket0_page_id, bucket1_page_id;
  NewBucketPage(&bucket0_page_id);
  dir->SetBucketPageId(0, bucket0_page_id);
  dir->SetLocalDepth(0, 1);
  NewBucketPage(&bucket1_page_id);
  dir->SetBucketPageId(1, bucket0_page_id);
  dir->SetLocalDepth(1, 1);
  dir->IncrGlobalDepth();

  // unpin
  buffer_pool_manager->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket0_page_id, false);
  buffer_pool_manager_->UnpinPage(bucket1_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetGlobalDepthMask() & Hash(key);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  if (page == nullptr) {
    LOG_WARN("FetchDirectoryPage fail, id: %d", directory_page_id_);
    return nullptr;
  }
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  if (page == nullptr) {
    LOG_WARN("FetchBucketPage fail, id: %d", bucket_page_id);
    return nullptr;
  }
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::NewBucketPage(page_id_t *bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->NewPage(bucket_page_id);
  if (page == nullptr) {
    LOG_WARN("NewBucketPage fail, id: %d", *bucket_page_id);
    return nullptr;
  }
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  auto bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto bucket = FetchBucketPage(bucket_page_id);
  bool success = bucket->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto dir = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir);
  auto bucket_page_id = dir->GetBucketPageId(bucket_idx);
  auto bucket = FetchBucketPage(bucket_page_id);
  // insert directly
  if (!bucket->IsFull()) {
    bool success = bucket->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, success);
    return success;
  }
  // yield to HASH_TABLE_TYPE::SplitInsert
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // operation result, insertion has done, directory has grown(i.e. the dir_page is dirty)
  bool success = false, inserted = false, dir_grown = false;
  auto dir = FetchDirectoryPage();

  while (!inserted) {
    auto bucket_idx = KeyToDirectoryIndex(key, dir);
    auto bucket_page_id = dir->GetBucketPageId(bucket_idx);
    auto bucket = FetchBucketPage(bucket_page_id);
    uint32_t bucket_overflow_count = 0;  // how many items migrated
    if (!bucket->IsFull()) {
      success = bucket->Insert(key, value, comparator_);
      inserted = true;
    } else {
      // First, check if needing to grow the directory.
      if (dir->GetLocalDepth(bucket_idx) >= dir->GetGlobalDepth()) {
        dir->Grow();
        dir_grown = true;
      }
      // Second, initialize the image bucket.
      auto image_bucket_idx = dir->GetSplitImageIndex(bucket_idx);
      page_id_t image_bucket_page_id;
      auto image_bucket = NewBucketPage(&image_bucket_page_id);
      dir->SetBucketPageId(image_bucket_idx, image_bucket_page_id);
      dir->SetLocalDepth(image_bucket_idx, dir->GetLocalDepth(bucket_idx));
      // Third, migrate keys corespending to new_bucket_idx's HighBits from origin bucket to image bucket.
      auto local_mask = dir->GetLocalDepthMask(bucket_idx);
      auto high_bits = dir->GetLocalHighBits(image_bucket_idx);
      uint32_t size = bucket->NumReadable();
      for (uint32_t i = 0; i < size && bucket->IsReadable(i); ++i) {
        /** Equivalent to ?
         * ((Hash(key) & dir->GetLocalDepthMask(bucket_idx)) ^ image_bucket_idx) == 0
         * */
        if ((Hash(bucket->KeyAt(i)) & local_mask) == high_bits) {
          bucket_overflow_count++;
          image_bucket->Insert(bucket->KeyAt(i), bucket->ValueAt(i), comparator_);
          bucket->RemoveAt(i);
        }
      }
      buffer_pool_manager_->UnpinPage(image_bucket_page_id, true);
    }  // if
    buffer_pool_manager_->UnpinPage(bucket_page_id, bucket_overflow_count != 0);
  }  // while
  buffer_pool_manager_->UnpinPage(directory_page_id_, dir_grown);
  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto dir = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir);
  auto bucket = FetchBucketPage(bucket_page_id);
  bool success = bucket->Remove(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  // If bucket is empty, yield to HASH_TABLE_TYPE::Merge, to shrink the bucket.
  if (success && bucket->IsEmpty()) Merge(transaction, key, value);
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir = FetchDirectoryPage();
  bool dir_shrunken = false;
  // Traverse the dir and merge all empty buckets.
  for (uint32_t bucket_idx = 0; bucket_idx < dir->Size(); ++bucket_idx) {
    auto bucket_page_id = dir->GetBucketPageId(bucket_idx);
    auto bucket = FetchBucketPage(bucket_page_id);
    auto bucket_local_depth = dir->GetLocalDepth(bucket_idx);
    auto image_bucket_idx = dir->GetSplitImageIndex(bucket_idx);
    auto image_bucket_page_id = dir->GetBucketPageId(image_bucket_idx);
    if (bucket_local_depth > 1 && bucket->IsEmpty() && dir->GetLocalDepth(image_bucket_idx) == bucket_local_depth) {
      dir->DecrLocalDepth(bucket_idx);
      dir->DecrLocalDepth(image_bucket_idx);
      dir->SetBucketPageId(bucket_idx, dir->GetBucketPageId(image_bucket_idx));

      // Updating for all the buckets with same page_id
      for (uint32_t bucket_idx_j = 0; bucket_idx_j < dir->Size(); ++bucket_idx_j) {
        if (bucket_idx_j == bucket_idx || bucket_idx_j == image_bucket_idx) continue;
        auto bucket_page_id_j = dir->GetBucketPageId(bucket_idx_j);
        if (bucket_page_id_j == bucket_page_id || bucket_page_id_j == image_bucket_page_id) {
          dir->SetLocalDepth(bucket_idx_j, dir->GetLocalDepth(bucket_idx));
          dir->SetBucketPageId(bucket_idx_j, image_bucket_page_id);
        }
      }
    }
    if (dir->CanShrink()) {
      dir->Shrink();
      dir_shrunken = true;
    }
    // Just delete the image bucket, haven't modify this bucket.
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  }  // for
  buffer_pool_manager_->UnpinPage(directory_page_id_, dir_shrunken);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
