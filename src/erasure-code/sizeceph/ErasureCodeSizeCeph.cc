// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph-based Erasure Code Plugin - Minimal Implementation for Direct ErasureCodeInterface

#include "ErasureCodeSizeCeph.h"
#include "common/debug.h"
#include "common/strtol.h"
#include "crush/CrushWrapper.h"
#include <iostream>
#include <algorithm>
#include <dlfcn.h>
#include <cstring>
#include <cstdlib>
#include <vector>

// Memory safety constants
static const size_t MAX_CHUNK_SIZE = 16 * 1024 * 1024; // 16MB max per chunk

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeSizeCeph: ";
}

// Static member initialization
void* ErasureCodeSizeCeph::sizeceph_handle = nullptr;
bool ErasureCodeSizeCeph::library_loaded = false;
int ErasureCodeSizeCeph::library_ref_count = 0;
std::mutex ErasureCodeSizeCeph::library_mutex;
ErasureCodeSizeCeph::size_split_fn_t ErasureCodeSizeCeph::size_split_func = nullptr;
ErasureCodeSizeCeph::size_restore_fn_t ErasureCodeSizeCeph::size_restore_func = nullptr;
ErasureCodeSizeCeph::size_can_get_restore_fn_t ErasureCodeSizeCeph::size_can_get_restore_func = nullptr;

ErasureCodeSizeCeph::ErasureCodeSizeCeph() {
  dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Constructor called" << dendl;
  dout(10) << "ErasureCodeSizeCeph constructor: direct ErasureCodeInterface implementation" << dendl;
  
  // Thread-safe reference counting for library management
  std::lock_guard<std::mutex> lock(library_mutex);
  library_ref_count++;
  dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Instance created, ref_count=" << library_ref_count 
          << " library_loaded=" << library_loaded << dendl;
  
  // Initialize default profile
  profile["k"] = std::to_string(SIZECEPH_K);
  profile["m"] = std::to_string(SIZECEPH_M);
  profile["technique"] = "sizeceph";
  
  // Initialize chunk mapping (identity mapping for SizeCeph)
  chunk_mapping.resize(SIZECEPH_N);
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    chunk_mapping[i] = shard_id_t(i);
  }
}

ErasureCodeSizeCeph::~ErasureCodeSizeCeph() {
  dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Destructor called" << dendl;
  dout(10) << "ErasureCodeSizeCeph destructor" << dendl;
  
  // Thread-safe reference counting for library management
  std::lock_guard<std::mutex> lock(library_mutex);
  library_ref_count--;
  dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Instance destroyed, ref_count=" << library_ref_count 
          << " library_loaded=" << library_loaded << dendl;
  
  // Only unload library when no instances remain
  if (library_ref_count <= 0) {
    dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Last instance - calling unload_sizeceph_library_unsafe" << dendl;
    dout(10) << "Last SizeCeph instance - unloading library" << dendl;
    unload_sizeceph_library_unsafe();
    library_ref_count = 0; // Ensure it doesn't go negative
  }
}

int ErasureCodeSizeCeph::init(ceph::ErasureCodeProfile &profile_arg, std::ostream *ss) {
  dout(10) << "SizeCeph init: k=" << SIZECEPH_K << " m=" << SIZECEPH_M << " direct implementation" << dendl;
  
  // Merge provided profile with defaults
  profile = profile_arg;
  
  // Check for force_all_chunks mode
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  // Validate k and m values
  auto k_iter = profile.find("k");
  auto m_iter = profile.find("m");
  
  if (force_all_chunks) {
    // In force_all_chunks mode, accept k=9, m=0 (all chunks treated as data)
    dout(10) << "SizeCeph force_all_chunks mode enabled" << dendl;
    if (k_iter != profile.end() && std::stoi(k_iter->second) != 9) {
      *ss << "SizeCeph force_all_chunks mode requires k=9, got k=" << k_iter->second;
      return -EINVAL;
    }
    if (m_iter != profile.end() && std::stoi(m_iter->second) != 0) {
      *ss << "SizeCeph force_all_chunks mode requires m=0, got m=" << m_iter->second;
      return -EINVAL;
    }
  } else {
    // Standard mode: k=4, m=5
    if (k_iter != profile.end() && std::stoi(k_iter->second) != (int)SIZECEPH_K) {
      *ss << "SizeCeph only supports k=" << SIZECEPH_K << ", got k=" << k_iter->second;
      return -EINVAL;
    }
    
    if (m_iter != profile.end() && std::stoi(m_iter->second) != (int)SIZECEPH_M) {
      *ss << "SizeCeph only supports m=" << SIZECEPH_M << ", got m=" << m_iter->second;
      return -EINVAL;
    }
  }
  
  // Load the SizeCeph library
  if (!load_sizeceph_library()) {
    *ss << "Failed to load SizeCeph library";
    return -ENOENT;
  }
  
  dout(10) << "SizeCeph initialized successfully with always-decode architecture" << dendl;
  return 0;
}

const ceph::ErasureCodeProfile &ErasureCodeSizeCeph::get_profile() const {
  return profile;
}

int ErasureCodeSizeCeph::create_rule(const std::string &name, CrushWrapper &crush, std::ostream *ss) const {
  dout(10) << "SizeCeph create_rule: " << name << dendl;
  
  if (crush.rule_exists(name)) {
    dout(10) << "Rule " << name << " already exists" << dendl;
    return crush.get_rule_id(name);
  }
  
  // Create a simple host-level rule for SizeCeph
  int ruleid = crush.add_simple_rule(name, "default", "host", "", 
                                     "indep", pg_pool_t::TYPE_ERASURE, ss);
  
  if (ruleid < 0) {
    *ss << "Failed to create crush rule " << name << ": error " << ruleid;
    return ruleid;
  }
  
  dout(10) << "Created crush rule " << name << " with id " << ruleid << dendl;
  return ruleid;
}

unsigned int ErasureCodeSizeCeph::get_chunk_count() const {
  return SIZECEPH_N;
}

unsigned int ErasureCodeSizeCeph::get_data_chunk_count() const {
  // Check for force_all_chunks mode - same logic as get_chunk_size()
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  // In force_all_chunks mode, all 9 chunks are treated as data chunks
  return force_all_chunks ? SIZECEPH_N : SIZECEPH_K;
}

unsigned int ErasureCodeSizeCeph::get_coding_chunk_count() const {
  // Check for force_all_chunks mode - same logic as get_chunk_size()
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  // In force_all_chunks mode, there are no traditional coding chunks (m=0)
  return force_all_chunks ? 0 : SIZECEPH_M;
}

int ErasureCodeSizeCeph::get_sub_chunk_count() {
  return 1; // SizeCeph doesn't use sub-chunks
}

unsigned int ErasureCodeSizeCeph::get_chunk_size(unsigned int stripe_width) const {
  // For the OSD backend, chunk_size must satisfy:
  // get_data_chunk_count() * chunk_size == stripe_width
  // With SizeCeph k=9, this means chunk_size = stripe_width / 9
  
  unsigned int data_chunks = get_data_chunk_count();
  unsigned int chunk_size = stripe_width / data_chunks;
  
  dout(15) << "SizeCeph get_chunk_size: stripe_width=" << stripe_width 
           << " data_chunks=" << data_chunks << " chunk_size=" << chunk_size << dendl;
  return chunk_size;
}

int ErasureCodeSizeCeph::calculate_aligned_size(int original_size) const {
  // Align to 4-byte boundary for SizeCeph algorithm
  int aligned_to_4 = ((original_size + 3) / 4) * 4;
  
  // Then align to 512-byte block boundary for storage efficiency
  int aligned_to_512 = ((aligned_to_4 + 511) / 512) * 512;
  
  dout(15) << "SizeCeph calculate_aligned_size: original=" << original_size 
           << " aligned=" << aligned_to_512 << dendl;
  return aligned_to_512;
}

bool ErasureCodeSizeCeph::load_sizeceph_library() {
  std::lock_guard<std::mutex> lock(library_mutex);
  
  if (library_loaded) {
    dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Library already loaded, ref_count=" << library_ref_count << dendl;
    dout(20) << "SizeCeph library already loaded" << dendl;
    return true;
  }
  
  dout(0) << "SIZECEPH_REFCOUNT_DEBUG: Loading SizeCeph library, ref_count=" << library_ref_count << dendl;
  dout(10) << "Loading SizeCeph library..." << dendl;
  
  // Try multiple possible library paths
  std::vector<std::string> lib_paths = {
    "/home/joseph/code/sizeceph/sizeceph.so",
    "./sizeceph.so",
    "/usr/local/lib/sizeceph.so",
    "/usr/lib/sizeceph.so"
  };
  
  for (const auto& path : lib_paths) {
    sizeceph_handle = dlopen(path.c_str(), RTLD_LAZY);
    if (sizeceph_handle) {
      dout(10) << "SizeCeph library loaded from: " << path << dendl;
      break;
    } else {
      dout(20) << "Failed to load from " << path << ": " << dlerror() << dendl;
    }
  }
  
  if (!sizeceph_handle) {
    dout(0) << "Cannot load sizeceph library from any location: " << dlerror() << dendl;
    return false;
  }
  
  // Load function symbols
  dout(10) << "Loading SizeCeph function symbols..." << dendl;
  size_split_func = (size_split_fn_t) dlsym(sizeceph_handle, "size_split");
  size_restore_func = (size_restore_fn_t) dlsym(sizeceph_handle, "size_restore");
  size_can_get_restore_func = (size_can_get_restore_fn_t) dlsym(sizeceph_handle, "size_can_get_restore_fn");
  
  if (!size_split_func || !size_restore_func || !size_can_get_restore_func) {
    dout(0) << "Cannot load sizeceph functions: " << dlerror() << dendl;
    dout(0) << "size_split_func: " << (void*)size_split_func << dendl;
    dout(0) << "size_restore_func: " << (void*)size_restore_func << dendl;
    dout(0) << "size_can_get_restore_func: " << (void*)size_can_get_restore_func << dendl;
    dlclose(sizeceph_handle);
    sizeceph_handle = nullptr;
    return false;
  }
  
  dout(10) << "SizeCeph library functions loaded successfully" << dendl;
  library_loaded = true;
  return true;
}

void ErasureCodeSizeCeph::unload_sizeceph_library() {
  std::lock_guard<std::mutex> lock(library_mutex);
  unload_sizeceph_library_unsafe();
}

void ErasureCodeSizeCeph::unload_sizeceph_library_unsafe() {
  if (sizeceph_handle) {
    dout(10) << "Unloading SizeCeph library" << dendl;
    dlclose(sizeceph_handle);
    sizeceph_handle = nullptr;
    library_loaded = false;
    size_split_func = nullptr;
    size_restore_func = nullptr;
    size_can_get_restore_func = nullptr;
    dout(10) << "SizeCeph library unloaded" << dendl;
  }
}

// Always-decode minimum requirement
int ErasureCodeSizeCeph::minimum_to_decode(const shard_id_set &want_to_read,
                                           const shard_id_set &available,
                                           shard_id_set &minimum_set,
                                           mini_flat_map<shard_id_t, std::vector<std::pair<int, int>>> *minimum_sub_chunks) {
  dout(15) << "SizeCeph minimum_to_decode: always-decode architecture requires all available chunks" << dendl;
  
  // For SizeCeph, we need ALL available chunks for proper reconstruction
  // This implements the always-decode architecture
  minimum_set = available;
  
  // Check for force_all_chunks mode
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  // Check if we have enough chunks (need at least K chunks, or ALL in force_all_chunks mode)
  unsigned int required_chunks = force_all_chunks ? SIZECEPH_N : SIZECEPH_K;
  if (available.size() < required_chunks) {
    dout(0) << "SizeCeph minimum_to_decode: insufficient chunks, need at least " 
            << required_chunks << ", got " << available.size() 
            << " (force_all_chunks=" << force_all_chunks << ")" << dendl;
    return -EIO;
  }
  
  // No sub-chunks for SizeCeph
  if (minimum_sub_chunks) {
    minimum_sub_chunks->clear();
  }
  
  dout(15) << "SizeCeph minimum_to_decode: returning " << minimum_set.size() << " chunks" << dendl;
  return 0;
}

[[deprecated]]
int ErasureCodeSizeCeph::minimum_to_decode(const std::set<int> &want_to_read,
                                           const std::set<int> &available,
                                           std::map<int, std::vector<std::pair<int, int>>> *minimum) {
  // Convert to new interface
  shard_id_set want_set, available_set, minimum_set;
  for (int i : want_to_read) want_set.insert(shard_id_t(i));
  for (int i : available) available_set.insert(shard_id_t(i));
  
  int ret = minimum_to_decode(want_set, available_set, minimum_set, nullptr);
  
  if (minimum && ret == 0) {
    minimum->clear();
    for (auto id : minimum_set) {
      (*minimum)[id.id] = std::vector<std::pair<int, int>>();
    }
  }
  
  return ret;
}

int ErasureCodeSizeCeph::minimum_to_decode_with_cost(const shard_id_set &want_to_read,
                                                     const shard_id_map<int> &available,
                                                     shard_id_set *minimum) {
  dout(15) << "SizeCeph minimum_to_decode_with_cost: always-decode ignores costs" << dendl;
  
  // Extract available chunks
  shard_id_set available_set;
  for (const auto& pair : available) {
    available_set.insert(pair.first);
  }
  
  // Use standard minimum_to_decode logic
  return minimum_to_decode(want_to_read, available_set, *minimum, nullptr);
}

[[deprecated]]
int ErasureCodeSizeCeph::minimum_to_decode_with_cost(const std::set<int> &want_to_read,
                                                     const std::map<int, int> &available,
                                                     std::set<int> *minimum) {
  // Convert to new interface
  shard_id_set want_set, minimum_set;
  shard_id_map<int> available_map(SIZECEPH_N);
  
  for (int i : want_to_read) want_set.insert(shard_id_t(i));
  for (const auto& pair : available) available_map[shard_id_t(pair.first)] = pair.second;
  
  int ret = minimum_to_decode_with_cost(want_set, available_map, &minimum_set);
  
  if (minimum && ret == 0) {
    minimum->clear();
    for (auto id : minimum_set) {
      minimum->insert(id.id);
    }
  }
  
  return ret;
}

size_t ErasureCodeSizeCeph::get_minimum_granularity() {
  return SIZECEPH_MIN_BLOCK_SIZE;
}

// SizeCeph encode implementation using the actual SizeCeph algorithm
int ErasureCodeSizeCeph::encode(const shard_id_set &want_to_encode,
                                const ceph::bufferlist &in,
                                shard_id_map<ceph::bufferlist> *encoded) {
  dout(10) << "SizeCeph encode: size=" << in.length() 
           << " want=" << want_to_encode.size() << dendl;
  
  // Check for force_all_chunks mode
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  if (force_all_chunks) {
    dout(10) << "SizeCeph encode: force_all_chunks mode - ensuring all 9 chunks are requested" << dendl;
    
    // In force_all_chunks mode, verify that all 9 chunks are being requested
    if (want_to_encode.size() != SIZECEPH_N) {
      dout(0) << "SizeCeph encode: force_all_chunks mode requires all " << SIZECEPH_N 
              << " chunks to be requested, got " << want_to_encode.size() << dendl;
      return -EINVAL;
    }
    
    // Verify that chunk IDs are 0-8
    for (const auto& shard : want_to_encode) {
      if (shard.id < 0 || shard.id >= (int)SIZECEPH_N) {
        dout(0) << "SizeCeph encode: invalid chunk ID " << shard.id 
                << " in force_all_chunks mode" << dendl;
        return -EINVAL;
      }
    }
  }
  
  if (in.length() == 0) {
    dout(10) << "SizeCeph encode: empty input, creating empty chunks" << dendl;
    for (const auto& shard : want_to_encode) {
      (*encoded)[shard] = ceph::bufferlist();
    }
    return 0;
  }
  
  // SizeCeph requires input length to be divisible by 4 (processes 4 bytes at a time)
  // Also align to 512-byte blocks for storage efficiency
  unsigned int original_length = in.length();
  unsigned int padded_length = original_length;
  
  // First, pad to 4-byte boundary for SizeCeph
  if (padded_length % 4 != 0) {
    padded_length = ((padded_length + 3) / 4) * 4;
  }
  
  // Then, align to 512-byte block boundary for storage efficiency
  if (padded_length % 512 != 0) {
    padded_length = ((padded_length + 511) / 512) * 512;
  }
  
  dout(10) << "SizeCeph encode: original=" << original_length 
           << " padded=" << padded_length << dendl;
  
  // Create padded input if needed
  ceph::bufferlist padded_input;
  if (padded_length > original_length) {
    padded_input.append(in);
    
    // Add zero padding
    unsigned int padding_needed = padded_length - original_length;
    ceph::bufferptr zero_pad = ceph::buffer::create(padding_needed);
    zero_pad.zero();
    padded_input.append(zero_pad);
    
    dout(15) << "SizeCeph encode: added " << padding_needed << " bytes of padding" << dendl;
  } else {
    padded_input.append(in);
  }
  
  // Store original size in the first chunk's metadata for decode
  // This is a simple approach - in production, you'd want more robust metadata
  
  // Calculate chunk size consistent with get_chunk_size() method
  // For OSD backend compatibility: chunk_size = stripe_width / data_chunk_count
  unsigned int data_chunks = get_data_chunk_count();
  unsigned int chunk_size = padded_length / data_chunks;
  
  dout(10) << "SizeCeph encode: chunk_size=" << chunk_size 
           << " padded_length=" << padded_length << " data_chunks=" << data_chunks 
           << " force_all_chunks=" << force_all_chunks << dendl;
  
  // Load SizeCeph library if not already loaded
  if (!load_sizeceph_library()) {
    dout(0) << "SizeCeph encode: failed to load SizeCeph library" << dendl;
    return -ENOENT;
  }
  
  // =====================================================
  // AGGRESSIVE MEMORY BOUNDS CHECKING AND CRASH-ON-VIOLATION
  // =====================================================
  
  // Prepare input buffer for SizeCeph algorithm with bounds checking
  ceph::bufferlist input_copy;
  input_copy.append(padded_input);
  unsigned char* input_data = (unsigned char*)input_copy.c_str();
  
  // MEMORY SAFETY CHECK 1: Validate input buffer
  ceph_assert(input_data != nullptr);
  ceph_assert(input_copy.length() == padded_length);
  ceph_assert(padded_length > 0 && padded_length <= MAX_CHUNK_SIZE * SIZECEPH_N);
  
  dout(0) << "MEMORY_SAFETY: Input buffer validation passed:" << dendl;
  dout(0) << "  input_data = " << (void*)input_data << dendl;
  dout(0) << "  padded_length = " << padded_length << dendl;
  dout(0) << "  input_copy.length() = " << input_copy.length() << dendl;
  
  // Add memory guards around input buffer to detect overruns
  const uint32_t GUARD_PATTERN = 0xDEADBEEF;
  const size_t GUARD_SIZE = 64; // 64 bytes guard on each side
  
  // Create protected input buffer with guards
  ceph::bufferptr protected_input = ceph::buffer::create(padded_length + 2 * GUARD_SIZE);
  unsigned char* protected_input_ptr = (unsigned char*)protected_input.c_str();
  
  // Set up memory guards
  for (size_t i = 0; i < GUARD_SIZE; i += sizeof(uint32_t)) {
    *(uint32_t*)(protected_input_ptr + i) = GUARD_PATTERN;
    *(uint32_t*)(protected_input_ptr + GUARD_SIZE + padded_length + i) = GUARD_PATTERN;
  }
  
  // Copy input data to protected buffer
  memcpy(protected_input_ptr + GUARD_SIZE, input_data, padded_length);
  unsigned char* guarded_input_data = protected_input_ptr + GUARD_SIZE;
  
  dout(0) << "MEMORY_SAFETY: Protected input buffer created:" << dendl;
  dout(0) << "  protected_input_ptr = " << (void*)protected_input_ptr << dendl;
  dout(0) << "  guarded_input_data = " << (void*)guarded_input_data << dendl;
  dout(0) << "  guard_size = " << GUARD_SIZE << dendl;
  
  // Prepare output buffers for all 9 chunks with guards
  std::vector<ceph::bufferptr> output_chunks(SIZECEPH_N);
  std::vector<unsigned char*> output_ptrs(SIZECEPH_N);
  std::vector<unsigned char*> protected_output_ptrs(SIZECEPH_N);
  
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    // Create protected output buffer with guards
    output_chunks[i] = ceph::buffer::create(chunk_size + 2 * GUARD_SIZE);
    unsigned char* chunk_ptr = (unsigned char*)output_chunks[i].c_str();
    
    // MEMORY SAFETY CHECK 2: Validate output buffer allocation
    ceph_assert(chunk_ptr != nullptr);
    ceph_assert(output_chunks[i].length() >= chunk_size + 2 * GUARD_SIZE);
    
    // Set up memory guards for output buffer
    for (size_t j = 0; j < GUARD_SIZE; j += sizeof(uint32_t)) {
      *(uint32_t*)(chunk_ptr + j) = GUARD_PATTERN;
      *(uint32_t*)(chunk_ptr + GUARD_SIZE + chunk_size + j) = GUARD_PATTERN;
    }
    
    protected_output_ptrs[i] = chunk_ptr;
    output_ptrs[i] = chunk_ptr + GUARD_SIZE; // Point to actual data area
    
    dout(0) << "MEMORY_SAFETY: Protected output buffer " << i << " created:" << dendl;
    dout(0) << "  protected_ptr = " << (void*)protected_output_ptrs[i] << dendl;
    dout(0) << "  data_ptr = " << (void*)output_ptrs[i] << dendl;
    dout(0) << "  chunk_size = " << chunk_size << dendl;
  }
  
  // MEMORY SAFETY CHECK 3: Validate all pointers before SizeCeph call
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    ceph_assert(output_ptrs[i] != nullptr);
    ceph_assert(protected_output_ptrs[i] != nullptr);
    ceph_assert(output_ptrs[i] == protected_output_ptrs[i] + GUARD_SIZE);
    
    // Verify guards are still intact
    for (size_t j = 0; j < GUARD_SIZE; j += sizeof(uint32_t)) {
      uint32_t guard1 = *(uint32_t*)(protected_output_ptrs[i] + j);
      uint32_t guard2 = *(uint32_t*)(protected_output_ptrs[i] + GUARD_SIZE + chunk_size + j);
      ceph_assert_always(guard1 == GUARD_PATTERN);
      ceph_assert_always(guard2 == GUARD_PATTERN);
    }
  }
  
  dout(0) << "MEMORY_SAFETY: All pre-call validations passed. Calling size_split_func..." << dendl;
  
  // Call the actual SizeCeph split algorithm with protected buffers
  size_split_func(output_ptrs.data(), guarded_input_data, padded_length);
  
  dout(0) << "MEMORY_SAFETY: size_split_func completed. Checking for violations..." << dendl;
  
  // MEMORY SAFETY CHECK 4: Validate guards after SizeCeph call
  // Check input buffer guards
  for (size_t i = 0; i < GUARD_SIZE; i += sizeof(uint32_t)) {
    uint32_t guard1 = *(uint32_t*)(protected_input_ptr + i);
    uint32_t guard2 = *(uint32_t*)(protected_input_ptr + GUARD_SIZE + padded_length + i);
    if (guard1 != GUARD_PATTERN) {
      dout(0) << "MEMORY_VIOLATION: Input buffer underrun detected at offset " << i 
              << " expected " << std::hex << GUARD_PATTERN 
              << " got " << guard1 << std::dec << dendl;
      ceph_abort_msg("SizeCeph input buffer underrun detected!");
    }
    if (guard2 != GUARD_PATTERN) {
      dout(0) << "MEMORY_VIOLATION: Input buffer overrun detected at offset " 
              << (GUARD_SIZE + padded_length + i) 
              << " expected " << std::hex << GUARD_PATTERN 
              << " got " << guard2 << std::dec << dendl;
      ceph_abort_msg("SizeCeph input buffer overrun detected!");
    }
  }
  
  // Check output buffer guards
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    for (size_t j = 0; j < GUARD_SIZE; j += sizeof(uint32_t)) {
      uint32_t guard1 = *(uint32_t*)(protected_output_ptrs[i] + j);
      uint32_t guard2 = *(uint32_t*)(protected_output_ptrs[i] + GUARD_SIZE + chunk_size + j);
      if (guard1 != GUARD_PATTERN) {
        dout(0) << "MEMORY_VIOLATION: Output buffer " << i << " underrun detected at offset " << j 
                << " expected " << std::hex << GUARD_PATTERN 
                << " got " << guard1 << std::dec << dendl;
        ceph_abort_msg("SizeCeph output buffer underrun detected!");
      }
      if (guard2 != GUARD_PATTERN) {
        dout(0) << "MEMORY_VIOLATION: Output buffer " << i << " overrun detected at offset " 
                << (GUARD_SIZE + chunk_size + j)
                << " expected " << std::hex << GUARD_PATTERN 
                << " got " << guard2 << std::dec << dendl;
        ceph_abort_msg("SizeCeph output buffer overrun detected!");
      }
    }
  }
  
  dout(0) << "MEMORY_SAFETY: All post-call guard validations passed - no buffer overruns detected!" << dendl;
  
  // DEBUG: Verify output_ptrs after size_split_func
  dout(0) << "SIZECEPH_DEBUG: After size_split_func call:" << dendl;
  dout(0) << "  SIZECEPH_N = " << SIZECEPH_N << dendl;
  dout(0) << "  output_ptrs.size() = " << output_ptrs.size() << dendl;
  for (unsigned int i = 0; i < SIZECEPH_N && i < 5; ++i) {
    dout(0) << "  output_ptrs[" << i << "] = " << (void*)output_ptrs[i] 
            << " (first 4 bytes: " << std::hex 
            << *(uint32_t*)output_ptrs[i] << std::dec << ")" << dendl;
  }
  
  // Return only the requested chunks with aggressive safety checks
  dout(0) << "SIZECEPH_BUFFER_DEBUG: Starting chunk creation for " << want_to_encode.size() << " shards" << dendl;
  
  for (const auto& wanted_shard : want_to_encode) {
    if ((unsigned int)wanted_shard.id < SIZECEPH_N) {
      dout(0) << "SIZECEPH_BUFFER_DEBUG: Processing shard " << wanted_shard.id << dendl;
      
      // MEMORY SAFETY CHECK 5: Re-validate guards before each buffer copy
      const unsigned int shard_id = wanted_shard.id;
      for (size_t j = 0; j < GUARD_SIZE; j += sizeof(uint32_t)) {
        uint32_t guard1 = *(uint32_t*)(protected_output_ptrs[shard_id] + j);
        uint32_t guard2 = *(uint32_t*)(protected_output_ptrs[shard_id] + GUARD_SIZE + chunk_size + j);
        if (guard1 != GUARD_PATTERN || guard2 != GUARD_PATTERN) {
          dout(0) << "MEMORY_VIOLATION: Buffer " << shard_id << " guards corrupted during processing!" << dendl;
          dout(0) << "  guard1 at offset " << j << ": expected " << std::hex << GUARD_PATTERN 
                  << " got " << guard1 << std::dec << dendl;
          dout(0) << "  guard2 at offset " << (GUARD_SIZE + chunk_size + j) << ": expected " 
                  << std::hex << GUARD_PATTERN << " got " << guard2 << std::dec << dendl;
          ceph_abort_msg("SizeCeph buffer corruption detected during chunk processing!");
        }
      }
      
      ceph::bufferlist chunk_bl;
      
      // Create a new buffer and copy the data to avoid ownership issues
      ceph::bufferptr new_chunk = ceph::buffer::create(chunk_size);
      
      // COMPREHENSIVE BUFFER DEBUG WITH MEMORY SAFETY
      dout(0) << "SIZECEPH_BUFFER_DEBUG: Buffer creation details:" << dendl;
      dout(0) << "  chunk_size = " << chunk_size << dendl;
      dout(0) << "  new_chunk.length() = " << new_chunk.length() << dendl;
      dout(0) << "  new_chunk.c_str() = " << (void*)new_chunk.c_str() << dendl;
      dout(0) << "  new_chunk raw nref = " << new_chunk.raw_nref() << dendl;
      dout(0) << "  source_ptr (protected) = " << (void*)output_ptrs[shard_id] << dendl;
      dout(0) << "  new_chunk buffer valid = " << (new_chunk.c_str() != nullptr) << dendl;
      
      // MEMORY SAFETY CHECK 6: Validate destination buffer
      ceph_assert_always(new_chunk.c_str() != nullptr);
      ceph_assert_always(new_chunk.length() >= chunk_size);
      ceph_assert_always(output_ptrs[shard_id] != nullptr);
      
      // Only proceed if buffer is valid
      if (new_chunk.c_str() != nullptr && new_chunk.length() >= chunk_size) {
        dout(0) << "SIZECEPH_BUFFER_DEBUG: Pre-memcpy validation:" << dendl;
        dout(0) << "  wanted_shard.id = " << shard_id << dendl;
        dout(0) << "  source_ptr = " << (void*)output_ptrs[shard_id] << dendl;
        dout(0) << "  dest_ptr = " << (void*)new_chunk.c_str() << dendl;
        dout(0) << "  copy_size = " << chunk_size << dendl;
        
        // Check source data validity
        if (output_ptrs[shard_id] != nullptr) {
          dout(0) << "SIZECEPH_BUFFER_DEBUG: Source data preview: " 
                  << std::hex << *(uint32_t*)output_ptrs[shard_id] << std::dec << dendl;
        }
        
        // MEMORY SAFETY CHECK 7: Validate memory regions don't overlap
        uintptr_t src_start = (uintptr_t)output_ptrs[shard_id];
        uintptr_t src_end = src_start + chunk_size;
        uintptr_t dst_start = (uintptr_t)new_chunk.c_str();
        uintptr_t dst_end = dst_start + chunk_size;
        
        bool overlap = (src_start < dst_end) && (dst_start < src_end);
        if (overlap) {
          dout(0) << "MEMORY_VIOLATION: Buffer overlap detected!" << dendl;
          dout(0) << "  src range: " << (void*)src_start << " - " << (void*)src_end << dendl;
          dout(0) << "  dst range: " << (void*)dst_start << " - " << (void*)dst_end << dendl;
          ceph_abort_msg("SizeCeph buffer overlap detected!");
        }
        
        // Perform the copy with additional validation
        memcpy(new_chunk.c_str(), output_ptrs[shard_id], chunk_size);
        
        // MEMORY SAFETY CHECK 8: Verify copy integrity
        if (memcmp(new_chunk.c_str(), output_ptrs[shard_id], chunk_size) != 0) {
          dout(0) << "MEMORY_VIOLATION: memcpy data corruption detected!" << dendl;
          ceph_abort_msg("SizeCeph memcpy integrity check failed!");
        }
        
        dout(0) << "SIZECEPH_BUFFER_DEBUG: Post-memcpy validation:" << dendl;
        dout(0) << "  memcpy completed successfully" << dendl;
        dout(0) << "  dest data preview: " 
                << std::hex << *(uint32_t*)new_chunk.c_str() << std::dec << dendl;
        dout(0) << "  buffer still valid = " << (new_chunk.c_str() != nullptr) << dendl;
        dout(0) << "  data integrity verified = " << (memcmp(new_chunk.c_str(), output_ptrs[shard_id], chunk_size) == 0) << dendl;
      } else {
        dout(0) << "SIZECEPH_ERROR: Buffer creation failed!" << dendl;
        return -ENOMEM;
      }
      
      dout(0) << "SIZECEPH_BUFFER_DEBUG: Adding buffer to bufferlist:" << dendl;
      dout(0) << "  buffer address before append = " << (void*)new_chunk.c_str() << dendl;
      dout(0) << "  buffer length = " << new_chunk.length() << dendl;
      dout(0) << "  buffer raw refcount = " << new_chunk.raw_nref() << dendl;
      
      chunk_bl.append(new_chunk);
      
      dout(0) << "SIZECEPH_BUFFER_DEBUG: Buffer added to bufferlist:" << dendl;
      dout(0) << "  chunk_bl length = " << chunk_bl.length() << dendl;
      dout(0) << "  buffer raw refcount after append = " << new_chunk.raw_nref() << dendl;
      
      (*encoded)[wanted_shard] = chunk_bl;
      
      dout(0) << "SIZECEPH_BUFFER_DEBUG: Shard " << wanted_shard.id << " added to encoded map" << dendl;
      dout(15) << "SizeCeph encode: returning chunk " << wanted_shard.id 
               << " size=" << chunk_bl.length() << dendl;
    } else {
      dout(0) << "SizeCeph encode: invalid chunk id " << wanted_shard.id << dendl;
      return -EINVAL;
    }
  }
  
  // MEMORY SAFETY CHECK 9: Final validation before return
  dout(0) << "MEMORY_SAFETY: Performing final memory validation before return..." << dendl;
  
  // Final check of all input and output guards
  for (size_t i = 0; i < GUARD_SIZE; i += sizeof(uint32_t)) {
    uint32_t guard1 = *(uint32_t*)(protected_input_ptr + i);
    uint32_t guard2 = *(uint32_t*)(protected_input_ptr + GUARD_SIZE + padded_length + i);
    ceph_assert_always(guard1 == GUARD_PATTERN);
    ceph_assert_always(guard2 == GUARD_PATTERN);
  }
  
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    for (size_t j = 0; j < GUARD_SIZE; j += sizeof(uint32_t)) {
      uint32_t guard1 = *(uint32_t*)(protected_output_ptrs[i] + j);
      uint32_t guard2 = *(uint32_t*)(protected_output_ptrs[i] + GUARD_SIZE + chunk_size + j);
      ceph_assert_always(guard1 == GUARD_PATTERN);
      ceph_assert_always(guard2 == GUARD_PATTERN);
    }
  }
  
  dout(0) << "MEMORY_SAFETY: All final memory validations passed! No corruption detected." << dendl;
  dout(10) << "SizeCeph encode: successfully encoded " << encoded->size() 
           << " chunks using SizeCeph algorithm with full memory protection" << dendl;
  return 0;
}

[[deprecated]]
int ErasureCodeSizeCeph::encode(const std::set<int> &want_to_encode,
                                const ceph::bufferlist &in,
                                std::map<int, ceph::bufferlist> *encoded) {
  // Convert old interface to new interface
  shard_id_set want_set;
  for (int i : want_to_encode) {
    want_set.insert(shard_id_t(i));
  }
  
  shard_id_map<ceph::bufferlist> encoded_map(SIZECEPH_N);
  int ret = encode(want_set, in, &encoded_map);
  
  if (ret == 0 && encoded) {
    encoded->clear();
    for (const auto& pair : encoded_map) {
      (*encoded)[pair.first.id] = pair.second;
    }
  }
  
  return ret;
}

[[deprecated]]  
int ErasureCodeSizeCeph::encode_chunks(const std::set<int> &want_to_encode,
                                       std::map<int, ceph::bufferlist> *encoded) {
  return -ENOTSUP;
}

int ErasureCodeSizeCeph::encode_chunks(const shard_id_map<ceph::bufferptr> &in,
                                       shard_id_map<ceph::bufferptr> &out) {
  return -ENOTSUP;
}

void ErasureCodeSizeCeph::encode_delta(const ceph::bufferptr &old_data,
                                       const ceph::bufferptr &new_data,  
                                       ceph::bufferptr *delta_maybe_in_place) {
  // Simple XOR-based delta implementation
  dout(15) << "SizeCeph encode_delta: XOR-based delta" << dendl;
  
  if (old_data.length() != new_data.length()) {
    dout(0) << "SizeCeph encode_delta: buffer size mismatch" << dendl;
    return;
  }
  
  int len = old_data.length();
  ceph::bufferptr delta = ceph::buffer::create(len);
  
  const char* old_ptr = old_data.c_str();
  const char* new_ptr = new_data.c_str();
  char* delta_ptr = delta.c_str();
  
  for (int i = 0; i < len; i++) {
    delta_ptr[i] = old_ptr[i] ^ new_ptr[i];
  }
  
  *delta_maybe_in_place = std::move(delta);
}

void ErasureCodeSizeCeph::apply_delta(const shard_id_map<ceph::bufferptr> &in,
                                      shard_id_map<ceph::bufferptr> &out) {
  dout(15) << "SizeCeph apply_delta: applying deltas to parity chunks" << dendl;
  
  // For always-decode architecture, we need to re-encode completely
  // This is a simplified implementation
  for (const auto& pair : in) {
    auto out_iter = out.find(pair.first);
    if (out_iter != out.end()) {
      // Apply delta (XOR operation)
      const char* delta_ptr = pair.second.c_str();
      char* out_ptr = out_iter->second.c_str();
      int len = std::min(pair.second.length(), out_iter->second.length());
      
      for (int i = 0; i < len; i++) {
        out_ptr[i] ^= delta_ptr[i];
      }
    }
  }
}

// SizeCeph decode implementation - ALWAYS decodes since data is transformed on disk
int ErasureCodeSizeCeph::decode(const shard_id_set &want_to_read,
                                const shard_id_map<ceph::bufferlist> &chunks,
                                shard_id_map<ceph::bufferlist> *decoded, int chunk_size) {
  dout(10) << "SizeCeph decode: want=" << want_to_read.size() 
           << " available=" << chunks.size() << " chunk_size=" << chunk_size << dendl;
  
  // Check for force_all_chunks mode
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  // Check if we have enough chunks to restore
  unsigned int required_chunks = force_all_chunks ? SIZECEPH_N : SIZECEPH_K;
  if (chunks.size() < required_chunks) {
    dout(0) << "SizeCeph decode: insufficient chunks " << chunks.size() 
            << " (need " << required_chunks << ", force_all_chunks=" << force_all_chunks << ")" << dendl;
    return -ENOENT;
  }
  
  // Load SizeCeph library if not already loaded
  if (!load_sizeceph_library()) {
    dout(0) << "SizeCeph decode: failed to load SizeCeph library" << dendl;
    return -ENOENT;
  }
  
  // Get chunk size from parameter or available chunks
  unsigned int effective_chunk_size = chunk_size;
  if (effective_chunk_size <= 0 && !chunks.empty()) {
    effective_chunk_size = chunks.begin()->second.length();
  }
  
  if (effective_chunk_size == 0) {
    dout(0) << "SizeCeph decode: all chunks are empty" << dendl;
    return -EINVAL;
  }
  
  dout(10) << "SizeCeph decode: effective_chunk_size=" << effective_chunk_size << dendl;
  
  // Prepare input chunks for SizeCeph restore
  std::vector<unsigned char*> input_chunks(SIZECEPH_N);
  std::vector<ceph::bufferlist> chunk_copies(SIZECEPH_N);
  std::vector<ceph::bufferptr> zero_chunks(SIZECEPH_N);
  std::vector<bool> available(SIZECEPH_N, false);
  
  // Initialize all chunks with zeros to avoid null pointers in some cases
  // For missing chunks, we'll set them to NULL after checking
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    zero_chunks[i] = ceph::buffer::create(effective_chunk_size);
    zero_chunks[i].zero();
    input_chunks[i] = (unsigned char*)zero_chunks[i].c_str();
  }
  
  // Set up available chunks (override zeros where we have real data)
  for (const auto& chunk_pair : chunks) {
    shard_id_t shard_id = chunk_pair.first;
    if ((unsigned int)shard_id.id < SIZECEPH_N) {
      chunk_copies[shard_id.id].append(chunk_pair.second);
      input_chunks[shard_id.id] = (unsigned char*)chunk_copies[shard_id.id].c_str();
      available[shard_id.id] = true;
      
      dout(15) << "SizeCeph decode: loaded chunk " << shard_id.id 
               << " size=" << chunk_pair.second.length() << dendl;
    }
  }
  
  // Now set missing chunks to NULL for SizeCeph algorithm
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    if (!available[i]) {
      input_chunks[i] = nullptr;
    }
  }
  
  // Check which chunks we can restore with SizeCeph algorithm
  int available_bitmask = 0;
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    if (available[i]) {
      available_bitmask |= (1 << i);
    }
  }

  dout(10) << "SizeCeph decode: available bitmask=" << std::hex << available_bitmask << std::dec << dendl;

  // Convert to const unsigned char** for SizeCeph function
  std::vector<const unsigned char*> const_input_chunks(SIZECEPH_N);
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    const_input_chunks[i] = input_chunks[i];
  }

  // FIXED: Use the corrected SizeCeph library's own validation
  // The library's validation bug has been fixed - it now correctly reports
  // which patterns can actually be restored without segfaulting.
  dout(10) << "SizeCeph decode: using fixed library validation for bitmask " 
           << std::hex << available_bitmask << std::dec << dendl;
  
  // Special case: if all chunks are available, we can reconstruct directly
  if (available_bitmask == 0x1FF) {
    dout(10) << "SizeCeph decode: all chunks available, proceeding with restore" << dendl;
  }
  
  // Use the fixed SizeCeph validation - this now works correctly
  if (!size_can_get_restore_func(const_input_chunks.data())) {
    dout(0) << "SizeCeph decode: pattern " << std::hex << available_bitmask 
            << std::dec << " cannot be restored (validated by fixed SizeCeph library)" << dendl;
    return -ENOTSUP;
  }
  
  dout(10) << "SizeCeph decode: pattern " << std::hex << available_bitmask 
           << std::dec << " validated as restorable by fixed library" << dendl;

  // Calculate original data size: for SizeCeph with k=9 chunks, 
  // original_size = data_chunk_count * effective_chunk_size
  unsigned int data_chunks = get_data_chunk_count();
  unsigned int original_size = data_chunks * effective_chunk_size;
  
  dout(10) << "SizeCeph decode: original_size=" << original_size 
           << " (data_chunks=" << data_chunks << " * effective_chunk_size=" << effective_chunk_size << ")" << dendl;
  
  // Prepare output buffer for restored original data
  ceph::bufferptr restored_data = ceph::buffer::create(original_size);
  unsigned char* output_ptr = (unsigned char*)restored_data.c_str();
  
  // Call SizeCeph restore algorithm with additional safety checks
  dout(15) << "SizeCeph decode: calling size_restore_func with " << available_bitmask 
           << " available chunks" << dendl;
           
  int restore_result = size_restore_func(output_ptr, const_input_chunks.data(), original_size);
  
  if (restore_result != 0) {
    dout(0) << "SizeCeph decode: size_restore_func failed with code " << restore_result 
            << " for bitmask " << std::hex << available_bitmask << std::dec << dendl;
    return -EIO;
  }
  
  // Additional validation: check if output data looks reasonable
  bool all_zero = true;
  for (unsigned int i = 0; i < std::min(original_size, 64u); ++i) {
    if (output_ptr[i] != 0) {
      all_zero = false;
      break;
    }
  }
  
  if (all_zero && original_size > 0) {
    dout(0) << "SizeCeph decode: warning - restored data is all zeros, possible corruption" << dendl;
    // Don't fail here as zeros might be valid data, but log the warning
  }  // Now handle requests - SizeCeph ALWAYS requires full decode
  for (const auto& wanted_shard : want_to_read) {
    shard_id_t shard_id = wanted_shard;
    
    // In force_all_chunks mode, all chunks are data chunks; otherwise only 0-3 are data
    unsigned int data_chunk_limit = force_all_chunks ? SIZECEPH_N : SIZECEPH_K;
    
    if ((unsigned int)shard_id.id < data_chunk_limit) {
      // Data chunk request: extract from RESTORED ORIGINAL data
      // Note: This is NOT the same as the on-disk chunk data!
      ceph::bufferlist chunk_bl;
      ceph::bufferlist original_data_bl;
      original_data_bl.append(restored_data);
      chunk_bl.substr_of(original_data_bl, shard_id.id * effective_chunk_size, effective_chunk_size);
      (*decoded)[shard_id] = chunk_bl;
      
      dout(15) << "SizeCeph decode: returning ORIGINAL data chunk " << shard_id.id 
               << " size=" << chunk_bl.length() << " (force_all_chunks=" << force_all_chunks << ")" << dendl;
    } else if ((unsigned int)shard_id.id < SIZECEPH_N) {
      // Parity chunk request: return the actual stored chunk
      if (available[shard_id.id]) {
        (*decoded)[shard_id] = chunk_copies[shard_id.id];
        dout(15) << "SizeCeph decode: returning stored parity chunk " << shard_id.id << dendl;
      } else {
        // Need to re-encode to get missing parity chunk
        // Re-encode the restored original data to get parity chunks
        std::vector<unsigned char*> reencoded_chunks(SIZECEPH_N);
        std::vector<ceph::bufferptr> output_buffers(SIZECEPH_N);
        
        for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
          output_buffers[i] = ceph::buffer::create(effective_chunk_size);
          reencoded_chunks[i] = (unsigned char*)output_buffers[i].c_str();
        }
        
        // Re-encode original data to get all chunks
        size_split_func(reencoded_chunks.data(), output_ptr, original_size);
        
        // Return the requested parity chunk
        ceph::bufferlist parity_bl;
        parity_bl.append(output_buffers[shard_id.id]);
        (*decoded)[shard_id] = parity_bl;
        
        dout(15) << "SizeCeph decode: re-encoded parity chunk " << shard_id.id << dendl;
      }
    } else {
      dout(0) << "SizeCeph decode: invalid chunk id " << shard_id.id << dendl;
      return -EINVAL;
    }
  }
  
  dout(10) << "SizeCeph decode: successfully decoded " << decoded->size() 
           << " chunks using SizeCeph always-decode algorithm" << dendl;
  return 0;
}

[[deprecated]]
int ErasureCodeSizeCeph::decode(const std::set<int> &want_to_read,
                                const std::map<int, ceph::bufferlist> &chunks,
                                std::map<int, ceph::bufferlist> *decoded, int chunk_size) {
  // Convert old interface to new interface
  shard_id_set want_set;
  for (int i : want_to_read) {
    want_set.insert(shard_id_t(i));
  }
  
  shard_id_map<ceph::bufferlist> chunks_map(SIZECEPH_N);
  for (const auto& pair : chunks) {
    chunks_map[shard_id_t(pair.first)] = pair.second;
  }
  
  shard_id_map<ceph::bufferlist> decoded_map(SIZECEPH_N);
  int ret = decode(want_set, chunks_map, &decoded_map, chunk_size);
  
  if (ret == 0 && decoded) {
    decoded->clear();
    for (const auto& pair : decoded_map) {
      (*decoded)[pair.first.id] = pair.second;
    }
  }
  
  return ret;
}

int ErasureCodeSizeCeph::decode_chunks(const shard_id_set &want_to_read,
                                       shard_id_map<ceph::bufferptr> &in,
                                       shard_id_map<ceph::bufferptr> &out) {
  return -ENOTSUP;
}

[[deprecated]]
int ErasureCodeSizeCeph::decode_chunks(const std::set<int> &want_to_read,
                                       const std::map<int, ceph::bufferlist> &chunks,
                                       std::map<int, ceph::bufferlist> *decoded) {
  return -ENOTSUP;
}

const std::vector<shard_id_t> &ErasureCodeSizeCeph::get_chunk_mapping() const {
  return chunk_mapping;
}

[[deprecated]]
int ErasureCodeSizeCeph::decode_concat(const std::set<int>& want_to_read,
                                       const std::map<int, ceph::bufferlist> &chunks,
                                       ceph::bufferlist *decoded) {
  dout(10) << "SizeCeph decode_concat: deprecated function called with want_to_read=" 
           << want_to_read.size() << " chunks=" << chunks.size() << dendl;
  
  if (!decoded) {
    dout(0) << "SizeCeph decode_concat: decoded buffer is null" << dendl;
    return -EINVAL;
  }
  
  // Convert to modern interface
  shard_id_set want_set;
  for (int i : want_to_read) {
    want_set.insert(shard_id_t(i));
  }
  
  shard_id_map<ceph::bufferlist> chunks_map(SIZECEPH_N);
  for (const auto& pair : chunks) {
    chunks_map[shard_id_t(pair.first)] = pair.second;
  }
  
  shard_id_map<ceph::bufferlist> decoded_map(SIZECEPH_N);
  int chunk_size = chunks.empty() ? 0 : chunks.begin()->second.length();
  int ret = decode(want_set, chunks_map, &decoded_map, chunk_size);
  
  if (ret == 0) {
    decoded->clear();
    
    // ECCommonL.cc expects shards to be concatenated in the order they appear in want_to_read
    // We must return ALL requested shards (data AND parity) in sequential order
    // This is required for the trim_offset calculation to work correctly
    for (int shard_id : want_to_read) {
      auto it = decoded_map.find(shard_id_t(shard_id));
      if (it != decoded_map.end()) {
        decoded->claim_append(it->second);
        dout(20) << "SizeCeph decode_concat: appending shard " << shard_id 
                 << " with length " << it->second.length() << dendl;
      } else {
        dout(5) << "SizeCeph decode_concat: WARNING - requested shard " << shard_id 
                << " not found in decoded_map" << dendl;
        // Append empty buffer to maintain shard ordering
        ceph::bufferlist empty_shard;
        empty_shard.append_zero(chunk_size);
        decoded->claim_append(empty_shard);
      }
    }
    
    dout(15) << "SizeCeph decode_concat: successfully decoded " 
             << decoded->length() << " bytes (all requested shards in order)" << dendl;
  }
  
  return ret;
}

[[deprecated]]
int ErasureCodeSizeCeph::decode_concat(const std::map<int, ceph::bufferlist> &chunks,
                                       ceph::bufferlist *decoded) {
  dout(10) << "SizeCeph decode_concat: deprecated function called with chunks=" 
           << chunks.size() << dendl;
  
  if (!decoded) {
    dout(0) << "SizeCeph decode_concat: decoded buffer is null" << dendl;
    return -EINVAL;
  }
  
  // For this version, we want to read all data chunks
  // In force_all_chunks mode, all chunks (0 to N-1) are data; otherwise only (0 to K-1)
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  std::set<int> want_to_read;
  unsigned int data_chunk_limit = force_all_chunks ? SIZECEPH_N : SIZECEPH_K;
  for (unsigned int i = 0; i < data_chunk_limit; ++i) {
    want_to_read.insert(i);
  }
  
  return decode_concat(want_to_read, chunks, decoded);
}

ErasureCodeSizeCeph::plugin_flags ErasureCodeSizeCeph::get_supported_optimizations() const {
  // SizeCeph supports basic operations but not the typical optimizations
  // due to its always-decode architecture
  return FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED;
}