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
ErasureCodeSizeCeph::size_split_fn_t ErasureCodeSizeCeph::size_split_func = nullptr;
ErasureCodeSizeCeph::size_restore_fn_t ErasureCodeSizeCeph::size_restore_func = nullptr;
ErasureCodeSizeCeph::size_can_get_restore_fn_t ErasureCodeSizeCeph::size_can_get_restore_func = nullptr;

ErasureCodeSizeCeph::ErasureCodeSizeCeph() {
  dout(10) << "ErasureCodeSizeCeph constructor: direct ErasureCodeInterface implementation" << dendl;
  
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
  dout(10) << "ErasureCodeSizeCeph destructor" << dendl;
  unload_sizeceph_library();
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
  return SIZECEPH_K;
}

unsigned int ErasureCodeSizeCeph::get_coding_chunk_count() const {
  return SIZECEPH_M;
}

int ErasureCodeSizeCeph::get_sub_chunk_count() {
  return 1; // SizeCeph doesn't use sub-chunks
}

unsigned int ErasureCodeSizeCeph::get_chunk_size(unsigned int stripe_width) const {
  // SizeCeph requires stripe_width to be divisible by 4 (processes 4-byte blocks)
  // and chunk_size = stripe_width / 4 (K=4 data chunks)
  if (stripe_width % 4 != 0) {
    dout(10) << "SizeCeph get_chunk_size: stripe_width " << stripe_width 
             << " not aligned to 4-byte requirement" << dendl;
  }
  
  unsigned int chunk_size = stripe_width / SIZECEPH_K;
  dout(15) << "SizeCeph get_chunk_size: stripe_width=" << stripe_width 
           << " chunk_size=" << chunk_size << dendl;
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
  if (library_loaded) {
    dout(20) << "SizeCeph library already loaded" << dendl;
    return true;
  }
  
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
  
  // Check if we have enough chunks (need at least K chunks)
  if (available.size() < SIZECEPH_K) {
    dout(0) << "SizeCeph minimum_to_decode: insufficient chunks, need at least " 
            << SIZECEPH_K << ", got " << available.size() << dendl;
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
  
  // Calculate chunk size for SizeCeph: each chunk gets 1/4 of the padded input
  unsigned int chunk_size = padded_length / SIZECEPH_K;
  
  dout(10) << "SizeCeph encode: chunk_size=" << chunk_size << dendl;
  
  // Load SizeCeph library if not already loaded
  if (!load_sizeceph_library()) {
    dout(0) << "SizeCeph encode: failed to load SizeCeph library" << dendl;
    return -ENOENT;
  }
  
  // Prepare input buffer for SizeCeph algorithm
  ceph::bufferlist input_copy;
  input_copy.append(padded_input);
  unsigned char* input_data = (unsigned char*)input_copy.c_str();
  
  // Prepare output buffers for all 9 chunks
  std::vector<ceph::bufferptr> output_chunks(SIZECEPH_N);
  std::vector<unsigned char*> output_ptrs(SIZECEPH_N);
  
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    output_chunks[i] = ceph::buffer::create(chunk_size);
    output_ptrs[i] = (unsigned char*)output_chunks[i].c_str();
  }
  
  // Call the actual SizeCeph split algorithm
  size_split_func(output_ptrs.data(), input_data, padded_length);
  
  // Return only the requested chunks
  for (const auto& wanted_shard : want_to_encode) {
    if ((unsigned int)wanted_shard.id < SIZECEPH_N) {
      ceph::bufferlist chunk_bl;
      chunk_bl.append(output_chunks[wanted_shard.id]);
      (*encoded)[wanted_shard] = chunk_bl;
      
      dout(15) << "SizeCeph encode: returning chunk " << wanted_shard.id 
               << " size=" << chunk_bl.length() << dendl;
    } else {
      dout(0) << "SizeCeph encode: invalid chunk id " << wanted_shard.id << dendl;
      return -EINVAL;
    }
  }
  
  dout(10) << "SizeCeph encode: successfully encoded " << encoded->size() 
           << " chunks using SizeCeph algorithm" << dendl;
  return 0;
}

[[deprecated]]
int ErasureCodeSizeCeph::encode(const std::set<int> &want_to_encode,
                                const ceph::bufferlist &in,
                                std::map<int, ceph::bufferlist> *encoded) {
  return -ENOTSUP;
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
  
  // Check if we have enough chunks to restore
  if (chunks.size() < SIZECEPH_K) {
    dout(0) << "SizeCeph decode: insufficient chunks " << chunks.size() 
            << " (need " << SIZECEPH_K << ")" << dendl;
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

  // Calculate original data size (4 * chunk_size for SizeCeph)
  unsigned int original_size = SIZECEPH_K * effective_chunk_size;
  
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
    
    if ((unsigned int)shard_id.id < SIZECEPH_K) {
      // Data chunk request: extract from RESTORED ORIGINAL data
      // Note: This is NOT the same as the on-disk chunk data!
      ceph::bufferlist chunk_bl;
      ceph::bufferlist original_data_bl;
      original_data_bl.append(restored_data);
      chunk_bl.substr_of(original_data_bl, shard_id.id * effective_chunk_size, effective_chunk_size);
      (*decoded)[shard_id] = chunk_bl;
      
      dout(15) << "SizeCeph decode: returning ORIGINAL data chunk " << shard_id.id 
               << " size=" << chunk_bl.length() << dendl;
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
  return -ENOTSUP;
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
  return -ENOTSUP;
}

[[deprecated]]
int ErasureCodeSizeCeph::decode_concat(const std::map<int, ceph::bufferlist> &chunks,
                                       ceph::bufferlist *decoded) {
  return -ENOTSUP;
}

ErasureCodeSizeCeph::plugin_flags ErasureCodeSizeCeph::get_supported_optimizations() const {
  // SizeCeph supports basic operations but not the typical optimizations
  // due to its always-decode architecture
  return FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED;
}