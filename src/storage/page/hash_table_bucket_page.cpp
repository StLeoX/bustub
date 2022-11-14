//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE && IsOccupied(i); ++i) {
    if (IsReadable(i) && cmp(KeyAt(i), key) == 0) result->template emplace_back(ValueAt(i));
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  std::vector<ValueType> found;
  GetValue(key, cmp, &found);
  for (const auto &v : found) {
    if(value == v) return false;  // false if duplicate
  }
  uint32_t i = 0;
  for (; i < BUCKET_ARRAY_SIZE && IsOccupied(i); ++i) {
  }
  if (i == BUCKET_ARRAY_SIZE) return false;  // false if full
  array_[i] = {key, value};
  SetOccupied(i);
  SetReadable(i);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE && IsOccupied(i); ++i) {
    if (IsReadable(i) && cmp(KeyAt(i), key) == 0 && ValueAt(i) == value) {
      UnsetReadable(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  if (!IsReadable(bucket_idx)) return {};
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  if (!IsReadable(bucket_idx)) return {};
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (!IsReadable(bucket_idx)) return;
  UnsetReadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  if (bucket_idx >= BUCKET_ARRAY_SIZE) return false;
  return occupied_[bucket_idx / 8] >> (bucket_idx % 8) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  if (bucket_idx >= BUCKET_ARRAY_SIZE) return;
  occupied_[bucket_idx / 8] |= 1 << bucket_idx % 8;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  if (bucket_idx >= BUCKET_ARRAY_SIZE || !IsOccupied(bucket_idx)) return false;
  return readable_[bucket_idx / 8] >> (bucket_idx % 8) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  if (bucket_idx >= BUCKET_ARRAY_SIZE || !IsOccupied(bucket_idx)) return;
  readable_[bucket_idx / 8] |= 1 << bucket_idx % 8;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnsetReadable(uint32_t bucket_idx) {
  if (bucket_idx >= BUCKET_ARRAY_SIZE) return;
  readable_[bucket_idx / 8] &= ~(1 << bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t cnt = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE && IsOccupied(i); ++i) {
    if (IsReadable(i)) cnt++;
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0, taken = 0, free = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE && IsOccupied((i)); i++) {
    size++;
    if (IsReadable(i)) {
      taken++;
    } else {
      free++;
    }
  }
  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
