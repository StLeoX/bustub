//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <cassert>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : lru_size_(num_pages), lru_list_(0) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);

  if (lru_list_.empty()) {
    return false;
  } else {
    *frame_id = lru_list_.front();
    lru_list_.pop_front();
    lru_map_.erase(*frame_id);
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  auto target = lru_map_.find(frame_id);
  if (target == lru_map_.cend()) return;
  lru_list_.erase(target->second);
  lru_map_.erase(target);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  auto target = lru_map_.find(frame_id);
  if (target != lru_map_.cend()) return;
  lru_list_.push_back(frame_id);
  lru_map_[frame_id] = --lru_list_.end();  // note: end() is the sentinel, so using --end()
  if (lru_list_.size() > lru_size_) {
    frame_id_t victim;
    assert(Victim(&victim));
  }
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);

  return lru_list_.size();
}

}  // namespace bustub
