//
// Created by StLeoX on 2022/11/15.
// Define the HashedValue
//
#pragma once

#include "value.h"
#include "common/util/hash_util.h"

namespace bustub {
struct HashedValue {
  Value value_;
  explicit HashedValue(const Value &value) : value_(value) {}
  bool operator==(const HashedValue &rhs) const { return value_.CompareEquals(rhs.value_) == CmpBool::CmpTrue; }
};
}

namespace std {
template <>
struct hash<bustub::HashedValue> {
  size_t operator()(const bustub::HashedValue &hv) const { return bustub::HashUtil::HashValue(&hv.value_); }
};
}  // namespace std