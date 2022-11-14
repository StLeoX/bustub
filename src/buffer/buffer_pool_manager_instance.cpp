//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);  // defer mutex

  frame_id_t target_frame = FindPage(page_id);
  if (target_frame == INVALID_PAGE_ID) return false;
  FlushPageReally(page_id);
  pages_[target_frame].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (const auto &item : page_table_) {
    FlushPgImp(item.first);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::lock_guard<std::mutex> guard(latch_);  // defer mutex

  auto fresh_frame = FindFreshPage();
  if (fresh_frame == INVALID_PAGE_ID) return nullptr;
  page_id_t new_page = AllocatePage();
  *page_id = new_page;
  pages_[fresh_frame].page_id_ = new_page;
  pages_[fresh_frame].pin_count_++;  // After newing it, we are using it.
  pages_[fresh_frame].ResetMemory();
  pages_[fresh_frame].is_dirty_ = false;
  page_table_[new_page] = fresh_frame;
  return &pages_[fresh_frame];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::lock_guard<std::mutex> guard(latch_);  // defer mutex

  frame_id_t available_frame = FindPage(page_id);
  if (available_frame != INVALID_PAGE_ID) {  // hit, use it
    replacer_->Pin(available_frame);
    pages_[available_frame].pin_count_++;
    pages_[available_frame].is_dirty_ = true;
    return &pages_[available_frame];
  }
  available_frame = FindFreshPage();  // miss, alloc one
  if (available_frame == INVALID_PAGE_ID) return nullptr;
  page_table_[page_id] = available_frame;
  replacer_->Pin(available_frame);
  pages_[available_frame].page_id_ = page_id;
  pages_[available_frame].pin_count_++;
  pages_[available_frame].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[available_frame].GetData());
  return &pages_[available_frame];
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::lock_guard<std::mutex> guard(latch_);  // defer mutex

  DeallocatePage(page_id);
  frame_id_t target_frame = FindPage(page_id);
  if (target_frame == INVALID_PAGE_ID) return true;
  if (pages_[target_frame].GetPinCount() > 0) return false;
  page_table_.erase(page_id);
  pages_[target_frame].page_id_ = INVALID_PAGE_ID;
  pages_[target_frame].is_dirty_ = false;
  pages_[target_frame].ResetMemory();
  free_list_.push_back(target_frame);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);  // defer mutex

  frame_id_t target_frame = FindPage(page_id);
  if (target_frame == INVALID_PAGE_ID || pages_[target_frame].GetPinCount() == 0) return false;
  if (is_dirty) pages_[target_frame].is_dirty_ = is_dirty;
  if (--pages_[target_frame].pin_count_ == 0) {
    replacer_->Unpin(target_frame);
    FlushPageReally(page_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

frame_id_t BufferPoolManagerInstance::FindPage(page_id_t page_id) const {
  auto item = page_table_.find(page_id);
  if (item == page_table_.cend()) return INVALID_PAGE_ID;
  return item->second;
}

frame_id_t BufferPoolManagerInstance::FindFreshPage() {
  frame_id_t frame_id;
  if (!free_list_.empty()) {  // free list available
    frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }
  if (replacer_->Victim(&frame_id)) {
    auto page_id = pages_[frame_id].GetPageId();
    FlushPageReally(page_id);
    page_table_.erase(page_id);
    pages_[frame_id].is_dirty_ = false;  // now it's clean
    return frame_id;
  }
  return INVALID_PAGE_ID;
}

void BufferPoolManagerInstance::FlushPageReally(page_id_t page_id) {
  frame_id_t target_frame = FindPage(page_id);
  if (target_frame == INVALID_PAGE_ID) return;
  if (pages_[target_frame].IsDirty()) disk_manager_->WritePage(page_id, pages_[target_frame].GetData());
}

}  // namespace bustub
