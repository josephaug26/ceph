// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph-based Erasure Code Plugin - Minimal Implementation for Direct ErasureCodeInterface
//
// ================================================================================
// CRITICAL ARCHITECTURAL NOTE: SizeCeph Buffer Size Compatibility Fix
// ================================================================================
// 
// This implementation contains a CRITICAL FIX for buffer overflow crashes that
// were occurring due to an architectural mismatch between SizeCeph's algorithm
// and Ceph's standard erasure coding framework assumptions.
//
// THE PROBLEM:
// - Standard erasure codes: chunk_size = total_data_size / k_data_chunks
// - SizeCeph algorithm: ALWAYS produces len/4 bytes per chunk (hardcoded)
// - This mismatch caused +125% buffer overflows during encode operations
// - Result: tcmalloc heap corruption and OSD crashes during write operations
//
// THE FIX (Lines ~445-480):
// Modified chunk size calculation from Ceph's expectation (padded_length/data_chunks)
// to SizeCeph's actual behavior (padded_length/4) to prevent buffer overflows.
//
// COMPATIBILITY IMPACT:
// This breaks standard Ceph erasure coding assumptions but is REQUIRED for
// SizeCeph to function without crashing. The architectural incompatibility
// is fundamental to SizeCeph's unique algorithm design.
//
// ================================================================================
// PERFORMANCE WARNING: SizeCeph Partial Operation Inefficiency
// ================================================================================
//
// IMPORTANT: SizeCeph pools are NOT optimized for partial operations:
//
// INEFFICIENT OPERATIONS:
// - rados append [object] [file]     # Requires full object re-encoding
// - rados put [object] [file] --offset N  # Requires full object re-encoding  
// - Small random writes             # Each change requires full object re-encoding
// - Frequent incremental updates    # No delta encoding support
//
// EFFICIENT OPERATIONS:
// - rados put [object] [file]       # Complete object replacement
// - Large bulk writes               # Amortizes re-encoding overhead
// - Write-once, read-many patterns  # Optimal for SizeCeph design
//
// REASON: SizeCeph's always-decode architecture requires complete object
// reconstruction for ANY modification, making partial updates extremely inefficient
// compared to traditional Reed-Solomon codes that support incremental updates.
//
// CRASH ANALYSIS REFERENCE:
// See /home/joseph/code/gdb_crash_analysis.log for detailed GDB analysis
// of the original tcmalloc corruption crashes that this fix resolves.
// ================================================================================

#include "ErasureCodeSizeCeph.h"
#include "common/debug.h"
#include "common/strtol.h"
#include "crush/CrushWrapper.h"
#include "include/intarith.h"
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
  // Thread-safe reference counting for library management
  std::lock_guard<std::mutex> lock(library_mutex);
  library_ref_count++;
  
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
  // Thread-safe reference counting for library management
  std::lock_guard<std::mutex> lock(library_mutex);
  library_ref_count--;
  
  // Only unload library when no instances remain
  if (library_ref_count <= 0) {
    unload_sizeceph_library_unsafe();
    library_ref_count = 0; // Ensure it doesn't go negative
  }
}

int ErasureCodeSizeCeph::init(ceph::ErasureCodeProfile &profile_arg, std::ostream *ss) {
  // Merge provided profile with defaults
  profile = profile_arg;
  
  // Check for force_all_chunks mode
  auto force_all_iter = profile.find("force_all_chunks");
  bool force_all_chunks = (force_all_iter != profile.end() && force_all_iter->second == "true");
  
  // Validate k and m values
  auto k_iter = profile.find("k");
  auto m_iter = profile.find("m");
  
  if (force_all_chunks) {
    // In force_all_chunks mode we accept two compatible configurations:
    //  - legacy all-as-data mode: k=9, m=0
    //  - advertised K/M for pool creation (k=4,m=5) while plugin still
    //    requires all 9 chunks at runtime (force_all_chunks=true)
    if (k_iter != profile.end() && m_iter != profile.end()) {
      int k_val = std::stoi(k_iter->second);
      int m_val = std::stoi(m_iter->second);
      if (!((k_val == 9 && m_val == 0) || (k_val == (int)SIZECEPH_K && m_val == (int)SIZECEPH_M))) {
        *ss << "SizeCeph force_all_chunks mode requires either k=9,m=0 (legacy) or k=" << SIZECEPH_K
            << ",m=" << SIZECEPH_M << ", got k=" << k_iter->second << ", m=" << m_iter->second;
        return -EINVAL;
      }
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
  
  return 0;
}

const ceph::ErasureCodeProfile &ErasureCodeSizeCeph::get_profile() const {
  return profile;
}

int ErasureCodeSizeCeph::create_rule(const std::string &name, CrushWrapper &crush, std::ostream *ss) const {
  if (crush.rule_exists(name)) {
    return crush.get_rule_id(name);
  }
  
  // Create a simple host-level rule for SizeCeph
  int ruleid = crush.add_simple_rule(name, "default", "host", "", 
                                     "indep", pg_pool_t::TYPE_ERASURE, ss);
  
  if (ruleid < 0) {
    *ss << "Failed to create crush rule " << name << ": error " << ruleid;
    return ruleid;
  }
  
  return ruleid;
}

unsigned int ErasureCodeSizeCeph::get_chunk_count() const {
  return SIZECEPH_N;
}

unsigned int ErasureCodeSizeCeph::get_data_chunk_count() const {
  // ================================================================================
  // STANDARD K=4, M=5 CONFIGURATION with Force All Chunks Behavior
  // ================================================================================
  // 
  // SizeCeph Configuration:
  // - Reports K=4 data chunks and M=5 parity chunks to Ceph
  // - But encode/decode operations still require all 9 chunks due to algorithm
  // - This matches SHEC/Jerasure pattern: logical K/M but operational requirements
  // 
  // Pool Creation: stripe_width = K * get_chunk_size(stripe_unit * K)
  // Runtime: chunk_size = stripe_width / K (Ceph's expectation)
  // SizeCeph: requires all 9 chunks but produces stripe_width/4 per chunk
  // ================================================================================
  
  return SIZECEPH_K;  // Return 4 (standard data chunks), but encode/decode handle all 9
}

unsigned int ErasureCodeSizeCeph::get_coding_chunk_count() const {
  // Standard K=4, M=5 configuration - return M=5 parity chunks
  return SIZECEPH_M;
}

int ErasureCodeSizeCeph::get_sub_chunk_count() {
  return 1; // SizeCeph doesn't use sub-chunks
}

unsigned ErasureCodeSizeCeph::get_alignment() const {
  // Return the required alignment for SizeCeph algorithm
  // SizeCeph requires 4-byte alignment for its algorithm
  return SIZECEPH_ALGORITHM_ALIGNMENT;  // Returns 4, not 16
}

unsigned int ErasureCodeSizeCeph::get_chunk_size(unsigned int stripe_width) const {
  // ================================================================================
  // MATHEMATICAL CONSISTENCY FIX: Following SHEC/Clay pattern
  // ================================================================================
  // 
  // ALIGNMENT STRATEGY (like SHEC and Clay):
  // 1. Get base alignment from get_alignment() method (4 bytes)
  // 2. Calculate K-aligned boundary: K * alignment = K * 4 = 16 bytes  
  // 3. Use round_up_to() to pad stripe_width to k_alignment boundary
  // 4. Return padded_length / K
  // 
  // MATHEMATICAL GUARANTEE:
  // Because k_alignment = K * alignment, padded_length is always divisible by K
  // This ensures: K * get_chunk_size(stripe_width) == padded_stripe_width
  // ================================================================================
  
  unsigned alignment = get_alignment();  // 4 bytes (SIZECEPH_ALGORITHM_ALIGNMENT)
  unsigned k_alignment = SIZECEPH_K * alignment;  // K * 4 = 16 bytes
  
  // Use round_up_to() like other EC plugins (Clay, etc)
  unsigned padded_length = round_up_to(stripe_width, k_alignment);
  unsigned chunk_size = padded_length / SIZECEPH_K;
  
  return chunk_size;
}

int ErasureCodeSizeCeph::calculate_aligned_size(int original_size) const {
  // Align to algorithm boundary for SizeCeph (4-byte alignment only)
  int aligned_to_algorithm = round_up_to(original_size, get_alignment());
  
  return aligned_to_algorithm;
}

bool ErasureCodeSizeCeph::load_sizeceph_library() {
  std::lock_guard<std::mutex> lock(library_mutex);
  
  if (library_loaded) {
    return true;
  }
  
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
      break;
    }
  }
  
  if (!sizeceph_handle) {
    return false;
  }
  
  // Load function symbols
  size_split_func = (size_split_fn_t) dlsym(sizeceph_handle, "size_split");
  size_restore_func = (size_restore_fn_t) dlsym(sizeceph_handle, "size_restore");
  size_can_get_restore_func = (size_can_get_restore_fn_t) dlsym(sizeceph_handle, "size_can_get_restore_fn");
  
  if (!size_split_func || !size_restore_func || !size_can_get_restore_func) {
    dlclose(sizeceph_handle);
    sizeceph_handle = nullptr;
    return false;
  }
  
  library_loaded = true;
  return true;
}

void ErasureCodeSizeCeph::unload_sizeceph_library() {
  std::lock_guard<std::mutex> lock(library_mutex);
  unload_sizeceph_library_unsafe();
}

void ErasureCodeSizeCeph::unload_sizeceph_library_unsafe() {
  if (sizeceph_handle) {
    dlclose(sizeceph_handle);
    sizeceph_handle = nullptr;
    library_loaded = false;
    size_split_func = nullptr;
    size_restore_func = nullptr;
    size_can_get_restore_func = nullptr;
  }
}

// Always-decode minimum requirement
int ErasureCodeSizeCeph::minimum_to_decode(const shard_id_set &want_to_read,
                                           const shard_id_set &available,
                                           shard_id_set &minimum_set,
                                           mini_flat_map<shard_id_t, std::vector<std::pair<int, int>>> *minimum_sub_chunks) {
  
  // CRITICAL: SizeCeph algorithm has ABSOLUTE requirement for ALL 9 chunks
  // This is not negotiable - the algorithm cannot function with missing chunks
  
  // Check if we have ALL required chunks (all 9, chunks 0-8)
  if (available.size() < SIZECEPH_N) {
    return -EIO;
  }
  
  // Verify we have exactly chunks 0 through 8
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    shard_id_t expected_shard(i);
    if (available.find(expected_shard) == available.end()) {
      return -EIO;
    }
  }
  
  // Return ALL available chunks as minimum requirement
  minimum_set = available;
  
  // No sub-chunks for SizeCeph
  if (minimum_sub_chunks) {
    minimum_sub_chunks->clear();
  }
  
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
      // For SizeCeph, we need to read the entire chunk
      // Each chunk contains the full content length divided by algorithm alignment
      std::vector<std::pair<int, int>> chunk_ranges;
      chunk_ranges.push_back(std::make_pair(0, get_sub_chunk_count()));
      (*minimum)[id.id] = chunk_ranges;
    }
  }
  
  return ret;
}

int ErasureCodeSizeCeph::minimum_to_decode_with_cost(const shard_id_set &want_to_read,
                                                     const shard_id_map<int> &available,
                                                     shard_id_set *minimum) {
  
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
  // Return minimum granularity for partial writes. Since SizeCeph requires
  // full re-encoding for ANY change, we use get_alignment() to ensure consistency
  // with the algorithm requirements.
  // 
  // Note: Even updates of this size will still require full object re-encoding
  // due to SizeCeph's always-decode architecture.
  return get_alignment();  // Use get_alignment() method for consistency
}

// SizeCeph encode implementation using the actual SizeCeph algorithm
int ErasureCodeSizeCeph::encode(const shard_id_set &want_to_encode,
                                const ceph::bufferlist &in,
                                shard_id_map<ceph::bufferlist> *encoded) {
  // ================================================================================
  // INPUT VALIDATION
  // ================================================================================
  
  // Load SizeCeph library first
  if (!load_sizeceph_library()) {
    return -ENOENT;
  }
  
  // Validate all 9 chunks are requested (SizeCeph requires all chunks for algorithm)
  if (want_to_encode.size() != SIZECEPH_N) {
    return -EINVAL;
  }
  
  // Validate chunk IDs are 0-8
  for (const auto& shard : want_to_encode) {
    if (shard.id < 0 || shard.id >= (int)SIZECEPH_N) {
      return -EINVAL;
    }
  }
  
  // Handle empty input
  if (in.length() == 0) {
    for (const auto& shard : want_to_encode) {
      (*encoded)[shard] = ceph::bufferlist();
    }
    return 0;
  }

  // Validate input alignment using get_alignment() method
  unsigned int required_alignment = get_alignment();
  if (in.length() % required_alignment != 0) {
    dout(0) << "SizeCeph encode: input size " << in.length() 
            << " not divisible by " << required_alignment 
            << " (required by SizeCeph algorithm via get_alignment())" << dendl;
    return -EINVAL;
  }

  // ================================================================================
  // ENCODE PROCESSING - Use get_chunk_size() for proper size calculation
  // ================================================================================
  
  // Calculate expected chunk size using get_chunk_size() method
  unsigned int input_length = in.length();
  unsigned int expected_chunk_size = get_chunk_size(input_length);
  
  // Verify the SizeCeph algorithm formula: chunk_size should equal input_length / alignment
  unsigned int algorithm_chunk_size = input_length / get_alignment();
  if (expected_chunk_size != algorithm_chunk_size) {
    dout(10) << "SizeCeph encode: get_chunk_size()=" << expected_chunk_size 
             << " vs algorithm=" << algorithm_chunk_size 
             << " (using get_chunk_size() result)" << dendl;
    // return -EIO; // This is just a debug check - comment out to proceed
    ceph_assert(expected_chunk_size == algorithm_chunk_size);
  }
  
  // Use the get_chunk_size() result for buffer allocation
  unsigned int chunk_size = expected_chunk_size;
  
  // Buffer allocation - OSD provides empty shard_id_map, plugin allocates actual buffers
  // Use SIMD-aligned allocation for optimal performance (consistent with ErasureCode.cc)
  std::vector<unsigned char*> output_ptrs(SIZECEPH_N);
  

  for (const auto& wanted_shard : want_to_encode) {
    // Allocate SIMD-aligned buffer for optimal vectorized operations (SIMD_ALIGN = 64)
    // ceph::bufferptr chunk_buffer = ceph::buffer::create_aligned(chunk_size, 64);
    ceph::bufferptr chunk_buffer = ceph::buffer::create (chunk_size);
    output_ptrs[wanted_shard.id] = (unsigned char*)chunk_buffer.c_str();
    (*encoded)[wanted_shard].append(chunk_buffer);
  }
  
  //Zero out output buffers for safety
  for (const auto& wanted_shard : want_to_encode) {
    memset((*encoded)[wanted_shard].c_str(), 0, chunk_size);
    // assert if encoded buffer is not same as output_ptrs
    if ((*encoded)[wanted_shard].c_str() != (char*)output_ptrs[wanted_shard.id]) {
      dout(0) << "SizeCeph encode: output buffer pointer mismatch for chunk " 
              << wanted_shard.id << dendl;
      ceph_assert(false); // This should never happen
    }
  }

  // Execute SizeCeph encoding directly on Ceph's aligned input
  // Create contiguous buffer for SizeCeph (it needs contiguous memory)
  ceph::bufferptr contiguous_input = ceph::buffer::create(input_length);
  in.begin().copy(input_length, contiguous_input.c_str());

  size_split_func(output_ptrs.data(), (unsigned char*)contiguous_input.c_str(), input_length);

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
  // Delta encoding is not meaningful for SizeCeph's always-decode architecture
  // SizeCeph transforms data in complex, non-linear ways that don't support
  // incremental updates. Any change requires full re-encoding.
  
  // Return an empty buffer to indicate no delta support
  *delta_maybe_in_place = ceph::bufferptr();
}

void ErasureCodeSizeCeph::apply_delta(const shard_id_map<ceph::bufferptr> &in,
                                      shard_id_map<ceph::bufferptr> &out) {
  // Delta application is not supported for SizeCeph's always-decode architecture
  // SizeCeph's non-linear transformation algorithm requires complete re-encoding
  // for any data changes, making incremental parity updates meaningless.
  
  // Clear output to indicate no delta support
  out.clear();
}

// SizeCeph decode implementation - ALWAYS decodes since data is transformed on disk
int ErasureCodeSizeCeph::decode(const shard_id_set &want_to_read,
                                const shard_id_map<ceph::bufferlist> &chunks,
                                shard_id_map<ceph::bufferlist> *decoded, int chunk_size) {
  // ================================================================================
  // INPUT VALIDATION
  // ================================================================================
  
  // Load SizeCeph library first
  if (!load_sizeceph_library()) {
    return -ENOENT;
  }


  // ABSOLUTE REQUIREMENT: SizeCeph algorithm requires ALL 9 chunks for decode
  // This is not a preference - it's a fundamental requirement of the algorithm
  if (chunks.size() < SIZECEPH_N) {
    dout(0) << "SizeCeph decode: ABSOLUTE REQUIREMENT - need ALL " << SIZECEPH_N 
            << " chunks, got only " << chunks.size() 
            << " (SizeCeph algorithm cannot function with missing chunks)" << dendl;
    return -ENOENT;
  }
  
  // Verify we have exactly the chunks we need (0 through 8)
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    shard_id_t expected_shard(i);
    if (chunks.find(expected_shard) == chunks.end()) {
      dout(0) << "SizeCeph decode: missing required chunk " << i 
              << " (ALL 9 chunks required for SizeCeph algorithm)" << dendl;
      return -ENOENT;
    }
  }
  
  // Determine chunk size
  unsigned int effective_chunk_size = chunk_size;
  if (effective_chunk_size <= 0 && !chunks.empty()) {
    effective_chunk_size = chunks.begin()->second.length();
  }
  if (effective_chunk_size == 0) {
    return -EINVAL;
  }
  
  
  // ================================================================================
  // DECODE PROCESSING
  // ================================================================================
  
  // Prepare input chunks for SizeCeph restore
  std::vector<unsigned char*> input_chunks(SIZECEPH_N);
  std::vector<ceph::bufferlist> chunk_copies(SIZECEPH_N);
  std::vector<bool> available(SIZECEPH_N, false);
  
  // Initialize with nullptr for missing chunks
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    input_chunks[i] = nullptr;
  }
  // Load available chunks
  for (const auto& chunk_pair : chunks) {
    shard_id_t shard_id = chunk_pair.first;
    if ((unsigned int)shard_id.id < SIZECEPH_N) {
      chunk_copies[shard_id.id].append(chunk_pair.second);
      input_chunks[shard_id.id] = (unsigned char*)chunk_copies[shard_id.id].c_str();
      available[shard_id.id] = true;
    }
  }

  
  // Check restore capability
  std::vector<const unsigned char*> const_input_chunks(SIZECEPH_N);
  for (unsigned int i = 0; i < SIZECEPH_N; ++i) {
    const_input_chunks[i] = input_chunks[i];
  }
  
  if (!size_can_get_restore_func(const_input_chunks.data())) {
    return -ENOTSUP;
  }
  
  // Execute restore - size_restore reconstructs the ORIGINAL data that was encoded
  // Calculate original data size using get_alignment() method
  unsigned int original_data_size = get_alignment() * effective_chunk_size;  // size_split input size
  ceph::bufferptr restored_data = ceph::buffer::create(original_data_size);
  unsigned char* output_ptr = (unsigned char*)restored_data.c_str();
  
  int restore_result = size_restore_func(output_ptr, const_input_chunks.data(), original_data_size);
  if (restore_result != 0) {
    return -EIO;
  }

  // Handle chunk requests
  for (const auto& wanted_shard : want_to_read) {
    shard_id_t shard_id = wanted_shard;
    
    if ((unsigned int)shard_id.id >= SIZECEPH_N) {
      return -EINVAL;
    }
    
    // CRITICAL UNDERSTANDING: SizeCeph doesn't have traditional data/parity separation
    // - All 9 chunks (0-8) are required for decode and contain transformed data
    // - The algorithm reconstructs the ORIGINAL data from all 9 chunks
    // - For K=4, M=5 compatibility, we simulate data chunks by dividing the original data
    
    ceph::bufferlist chunk_bl;
    
    if ((unsigned int)shard_id.id < SIZECEPH_K) {
      // Data chunks (0-3): Extract from restored original data
      // This simulates traditional data chunks by dividing the original data into K parts
      ceph::bufferlist original_data_bl;
      original_data_bl.append(restored_data);
      
      // Calculate the portion of original data for this "data chunk"
      unsigned int original_data_per_chunk = original_data_size / SIZECEPH_K;
      unsigned int start_offset = shard_id.id * original_data_per_chunk;
      unsigned int length = (shard_id.id == SIZECEPH_K - 1) ? 
                             (original_data_size - start_offset) : original_data_per_chunk;
      
      chunk_bl.substr_of(original_data_bl, start_offset, length);
      dout(15) << "SizeCeph decode: returning simulated data chunk " << shard_id.id 
               << " offset=" << start_offset << " length=" << length << dendl;
    } else {
      // Parity chunks (4-8): SizeCeph doesn't have traditional parity chunks
      // These are stored on disk but don't represent separable parity data
      // For compatibility, return empty chunks when parity is requested for read
      dout(15) << "SizeCeph decode: WARNING - parity chunk " << shard_id.id 
               << " requested, but SizeCeph stores transformed data, not traditional parity" << dendl;
      // Return empty buffer for parity chunks during read operations
      // (The actual chunks are needed for decode, but don't contain readable parity data)
      chunk_bl.clear();
    }
    
    (*decoded)[shard_id] = chunk_bl;
  }
  
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
  
  if (!decoded) {
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
                << " not found in decoded_map; appending zeros of chunk_size=" << chunk_size << dendl;
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
  
  if (!decoded) {
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
  // SizeCeph EXPLICITLY DISABLES partial operations that are inefficient
  // for its always-decode architecture. This forces Ceph to use full
  // encode/decode cycles instead of attempting partial updates.
  //
  // DISABLED optimizations:
  // - FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION: SizeCeph transforms data, so cannot read directly from chunks
  // - FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION: Any write requires full re-encoding
  // - FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION: Delta operations are meaningless for SizeCeph
  // 
  // ENABLED optimizations:
  // - FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED: Basic optimized EC is supported
  // - FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION: We can handle zero-length buffers
  //
  
  return FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED | FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION;
}