// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph-based Erasure Code Plugin Implementation integrated with Jerasure

#include "ErasureCodeSizeCeph.h"
#include "common/debug.h"
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

using std::ostream;
using ceph::ErasureCodeProfile;

// Static member initialization
void* ErasureCodeSizeCeph::sizeceph_handle = nullptr;
bool ErasureCodeSizeCeph::library_loaded = false;
ErasureCodeSizeCeph::size_split_fn_t ErasureCodeSizeCeph::size_split_func = nullptr;
ErasureCodeSizeCeph::size_restore_fn_t ErasureCodeSizeCeph::size_restore_func = nullptr;
ErasureCodeSizeCeph::size_can_get_restore_fn_t ErasureCodeSizeCeph::size_can_get_restore_func = nullptr;

bool ErasureCodeSizeCeph::load_sizeceph_library() {
  if (library_loaded) {
    dout(20) << "SizeCeph library already loaded" << dendl;
    return true;
  }
  
  dout(10) << "Loading SizeCeph library..." << dendl;
  
  // Check for environment variable override first
  const char* env_path = std::getenv("SIZECEPH_LIBRARY_PATH");
  if (env_path) {
    dout(1) << "Attempting to load SizeCeph library from environment path: " << env_path << dendl;
    sizeceph_handle = dlopen(env_path, RTLD_LAZY);
    if (sizeceph_handle) {
      dout(1) << "SizeCeph library loaded from environment path: " << env_path << dendl;
    } else {
      dout(0) << "Failed to load SizeCeph library from environment path: " << env_path 
                << " - " << dlerror() << dendl;
    }
  }
  
  // If environment override failed or wasn't set, try standard paths
  if (!sizeceph_handle) {
    dout(10) << "Environment path not available or failed, trying standard paths..." << dendl;
    // Try to load the sizeceph library from standard system paths first,
    // then fall back to development locations
    const char* lib_paths[] = {
      // Standard system library paths (installed via 'make install')
      "/usr/local/lib/sizeceph.so",      // Primary install location
      "/usr/lib/sizeceph.so",            // Alternative system location
      "/usr/lib/x86_64-linux-gnu/sizeceph.so",  // Debian/Ubuntu multiarch path
      // Library search path (let system find it)
      "sizeceph.so",                     // Uses LD_LIBRARY_PATH and system paths
      // Development fallback paths
      "/home/joseph/code/sizeceph/sizeceph.so",  // Development directory
      "./sizeceph.so",                   // Current directory
      nullptr
    };
    
    for (int i = 0; lib_paths[i] != nullptr; i++) {
      dout(15) << "Trying to load SizeCeph library from: " << lib_paths[i] << dendl;
      sizeceph_handle = dlopen(lib_paths[i], RTLD_LAZY);
      if (sizeceph_handle) {
        dout(1) << "SizeCeph library loaded from: " << lib_paths[i] << dendl;
        break;
      } else {
        dout(20) << "Failed to load from " << lib_paths[i] << ": " << dlerror() << dendl;
      }
    }
    
    if (!sizeceph_handle) {
      dout(0) << "Cannot load sizeceph library from any location: " << dlerror() << dendl;
      dout(0) << "Tried paths:" << dendl;
      if (env_path) {
        dout(0) << "  - " << env_path << " (from SIZECEPH_LIBRARY_PATH)" << dendl;
      }
      for (int i = 0; lib_paths[i] != nullptr; i++) {
        dout(0) << "  - " << lib_paths[i] << dendl;
      }
      dout(0) << "Install sizeceph.so using: cd /path/to/sizeceph && make install" << dendl;
      dout(0) << "Or set SIZECEPH_LIBRARY_PATH environment variable" << dendl;
      return false;
    }
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
  
  dout(1) << "SizeCeph library functions loaded successfully" << dendl;
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

void ErasureCodeSizeCeph::jerasure_encode(char **data, char **coding, int blocksize) {
  dout(10) << "SizeCeph encode: blocksize=" << blocksize << " k=" << k << " m=" << m << dendl;
  
  // SizeCeph requires disk block alignment (following original block driver design)
  // Original driver: block_size *= 4 and uses blk_queue_logical_block_size()
  // Check both basic 4-byte alignment and reasonable disk block alignment
  if (blocksize % 4 != 0) {
    dout(0) << "SizeCeph encode: blocksize " << blocksize << " is not aligned to 4-byte boundary" << dendl;
    throw std::runtime_error("SizeCeph requires blocksize to be aligned to 4-byte boundaries");
  }
  
  if (blocksize % 512 != 0) {
    dout(1) << "SizeCeph encode: Warning - blocksize " << blocksize << " is not aligned to 512-byte disk sectors" << dendl;
  }
  
  if (!load_sizeceph_library()) {
    dout(0) << "SizeCeph encode: failed to load sizeceph library" << dendl;
    throw std::runtime_error("Failed to load sizeceph library");
  }
  
  dout(15) << "SizeCeph encode: library loaded, starting encoding process" << dendl;
  
  // SizeCeph expects interleaved input data but we need to work with Ceph's chunk arrays
  // Create interleaved input buffer from the k data chunks
  int input_size = blocksize * k;
  unsigned char *input_buffer = new unsigned char[input_size];
  
  dout(20) << "SizeCeph encode: creating interleaved input buffer of size " << input_size << dendl;
  
  // Interleave the k data chunks byte by byte into input buffer
  // This matches how SizeCeph library expects the data to be organized
  for (int i = 0; i < blocksize; i++) {
    for (int j = 0; j < k; j++) {
      input_buffer[i * k + j] = ((unsigned char*)data[j])[i];
    }
  }
  
  // Allocate temporary output chunks for sizeceph (9 chunks total: k data + m coding)
  unsigned char *temp_chunks[9];
  for (int i = 0; i < 9; i++) {
    temp_chunks[i] = new unsigned char[blocksize];
    memset(temp_chunks[i], 0, blocksize); // Initialize to zeros
  }
  
  dout(15) << "SizeCeph encode: calling size_split function with input_size=" << input_size << dendl;
  
  // Additional alignment check for SizeCeph library requirements
  if (input_size % 4 != 0) {
    dout(0) << "SizeCeph encode: input_size " << input_size << " is not aligned to 4-byte boundary" << dendl;
    // Cleanup before returning
    for (int i = 0; i < 9; i++) {
      delete[] temp_chunks[i];
    }
    delete[] input_buffer;
    throw std::runtime_error("SizeCeph input_size must be aligned to 4-byte boundaries");
  }
  
  // Call sizeceph split function to generate 9 chunks from interleaved input
  size_split_func(temp_chunks, input_buffer, input_size);
  dout(15) << "SizeCeph encode: size_split completed successfully" << dendl;
  
  // Copy the first k chunks back to data arrays (these should match input data)
  for (int i = 0; i < k; i++) {
    memcpy(data[i], temp_chunks[i], blocksize);
    dout(20) << "SizeCeph encode: copied data chunk " << i << dendl;
  }
  
  // Copy the remaining m chunks to coding arrays
  for (int i = 0; i < m; i++) {
    memcpy(coding[i], temp_chunks[k + i], blocksize);
    dout(20) << "SizeCeph encode: copied coding chunk " << (k + i) << dendl;
  }
  
  dout(10) << "SizeCeph encode: encoding completed successfully" << dendl;
  
  // Cleanup
  for (int i = 0; i < 9; i++) {
    delete[] temp_chunks[i];
  }
  delete[] input_buffer;
}

int ErasureCodeSizeCeph::jerasure_decode(int *erasures, char **data, char **coding, int blocksize) {
  dout(10) << "SizeCeph decode: blocksize=" << blocksize << " k=" << k << " m=" << m << dendl;
  
  // SizeCeph requires disk block alignment (following original block driver design)
  // Check both basic 4-byte alignment and disk sector alignment
  if (blocksize % 4 != 0) {
    dout(0) << "SizeCeph decode: blocksize " << blocksize << " is not aligned to 4-byte boundary" << dendl;
    return -1;
  }
  
  if (blocksize % 512 != 0) {
    dout(1) << "SizeCeph decode: Warning - blocksize " << blocksize << " is not aligned to 512-byte disk sectors" << dendl;
  }
  
  if (!load_sizeceph_library()) {
    dout(0) << "SizeCeph decode: failed to load sizeceph library" << dendl;
    return -1;
  }
  
  // Verify required functions are loaded
  if (!size_restore_func || !size_can_get_restore_func) {
    dout(0) << "SizeCeph decode: required functions not loaded from library" << dendl;
    return -1;
  }
  
  // Count erasures
  int num_erasures = 0;
  while (erasures[num_erasures] != -1) {
    num_erasures++;
  }
  
  dout(10) << "SizeCeph decode: number of erasures=" << num_erasures << dendl;
  for (int i = 0; i < num_erasures; i++) {
    dout(15) << "SizeCeph decode: erased chunk " << erasures[i] << dendl;
  }
  
  if (num_erasures > m) {
    dout(0) << "SizeCeph decode: too many erasures (" << num_erasures << " > " << m << ")" << dendl;
    return -1; // Too many erasures to recover
  }
  
  // Handle the case where no erasures exist (simple copy)
  if (num_erasures == 0) {
    dout(10) << "SizeCeph decode: no erasures, data already complete" << dendl;
    return 0;
  }
  
  // Set up input chunks array for SizeCeph (9 chunks total)
  // Copy available chunks into temporary array
  unsigned char *temp_chunks[9];
  const unsigned char *input_chunks[9];
  
  // Initialize all pointers
  for (int i = 0; i < 9; i++) {
    temp_chunks[i] = nullptr;
    input_chunks[i] = nullptr;
  }
  
  dout(15) << "SizeCeph decode: setting up available chunks for SizeCeph library" << dendl;
  
  // Process all 9 chunks (k data + m coding)
  for (int i = 0; i < k + m; i++) {
    bool is_erased = false;
    for (int j = 0; j < num_erasures; j++) {
      if (erasures[j] == i) {
        is_erased = true;
        break;
      }
    }
    
    if (!is_erased) {
      // Copy available chunk
      temp_chunks[i] = new unsigned char[blocksize];
      if (i < k) {
        // Data chunk
        memcpy(temp_chunks[i], data[i], blocksize);
        dout(20) << "SizeCeph decode: copied available data chunk " << i << dendl;
      } else {
        // Coding chunk
        memcpy(temp_chunks[i], coding[i - k], blocksize);
        dout(20) << "SizeCeph decode: copied available coding chunk " << i << dendl;
      }
      input_chunks[i] = temp_chunks[i];
    } else {
      // Leave as nullptr for erased chunks
      dout(20) << "SizeCeph decode: chunk " << i << " is erased (NULL)" << dendl;
    }
  }
  
  dout(15) << "SizeCeph decode: checking if restoration is possible" << dendl;
  // Check if restoration is possible with available chunks
  if (!size_can_get_restore_func(input_chunks)) {
    dout(0) << "SizeCeph decode: restoration not possible with available chunks" << dendl;
    // Cleanup allocated chunks
    for (int i = 0; i < 9; i++) {
      if (temp_chunks[i] != nullptr) {
        delete[] temp_chunks[i];
      }
    }
    return -1;
  }
  
  dout(15) << "SizeCeph decode: restoration possible, proceeding with restore" << dendl;
  
  // Create output buffer for restored data
  int output_size = blocksize * k;
  unsigned char *output_buffer = new unsigned char[output_size];
  
  dout(15) << "SizeCeph decode: calling size_restore function with output_size=" << output_size << dendl;
  
  // Additional alignment check for SizeCeph library requirements
  if (output_size % 4 != 0) {
    dout(0) << "SizeCeph decode: output_size " << output_size << " is not aligned to 4-byte boundary" << dendl;
    delete[] output_buffer;
    // Cleanup allocated chunks
    for (int i = 0; i < 9; i++) {
      if (temp_chunks[i] != nullptr) {
        delete[] temp_chunks[i];
      }
    }
    return -1;
  }
  
  // Restore the original data using SizeCeph library
  int result = size_restore_func(output_buffer, input_chunks, output_size);
  
  if (result != 0) {
    dout(0) << "SizeCeph decode: size_restore failed with result=" << result << dendl;
    delete[] output_buffer;
    // Cleanup allocated chunks
    for (int i = 0; i < 9; i++) {
      if (temp_chunks[i] != nullptr) {
        delete[] temp_chunks[i];
      }
    }
    return -1;
  }
  
  dout(15) << "SizeCeph decode: size_restore completed successfully, de-interleaving data" << dendl;
  
  // De-interleave restored data back to individual data chunks
  // The output_buffer contains the restored data in interleaved format
  for (int i = 0; i < blocksize; i++) {
    for (int j = 0; j < k; j++) {
      // Only restore erased data chunks - leave non-erased ones as they are
      bool chunk_was_erased = false;
      for (int e = 0; e < num_erasures; e++) {
        if (erasures[e] == j) {
          chunk_was_erased = true;
          break;
        }
      }
      if (chunk_was_erased) {
        ((unsigned char*)data[j])[i] = output_buffer[i * k + j];
      }
    }
  }
  
  dout(10) << "SizeCeph decode: decoding completed successfully" << dendl;
  
  // Cleanup
  delete[] output_buffer;
  for (int i = 0; i < 9; i++) {
    if (temp_chunks[i] != nullptr) {
      delete[] temp_chunks[i];
    }
  }
  return 0;
}

unsigned ErasureCodeSizeCeph::get_alignment() const {
  // SizeCeph requires disk block alignment (minimum 512 bytes)
  // The original block driver uses LCM of source device block sizes * 4
  // For Ceph integration, we use a conservative 4KB alignment (typical modern disk block size)
  return 4096; // 4KB disk block alignment
}

void ErasureCodeSizeCeph::prepare() {
  dout(10) << "SizeCeph prepare: initializing plugin" << dendl;
  // Load the library when preparing
  if (!load_sizeceph_library()) {
    dout(0) << "SizeCeph prepare: failed to load library" << dendl;
    throw std::runtime_error("Failed to load sizeceph library during prepare()");
  }
  dout(10) << "SizeCeph prepare: plugin initialized successfully" << dendl;
}

size_t ErasureCodeSizeCeph::get_minimum_granularity() {
  // SizeCeph requires disk block alignment, following the original block driver design
  // Block driver used logical_block_size * 4 (NUM_DATA_SHARDS)
  // Conservative 4KB alignment ensures compatibility with modern storage
  return 4096; // 4KB minimum granularity for disk block alignment
}

void ErasureCodeSizeCeph::apply_delta(const shard_id_map<ceph::bufferptr> &in,
                                     shard_id_map<ceph::bufferptr> &out) {
  // For now, implement a simple approach - just re-encode
  // This could be optimized later to use actual delta operations
  
  // Extract the delta data
  ceph::bufferptr data_delta;
  for (const auto& pair : in) {
    if (pair.first < k) { // This is a data shard
      data_delta = pair.second;
      break;
    }
  }
  
  if (data_delta.length() == 0) {
    return; // No data delta to apply
  }
  
  // For simplicity, just copy the delta to output
  // In a real implementation, this would apply XOR deltas
  out = in;
}

int ErasureCodeSizeCeph::parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) {
  dout(10) << "SizeCeph parse: parsing profile" << dendl;
  
  int err = ErasureCodeJerasure::parse(profile, ss);
  if (err) {
    dout(0) << "SizeCeph parse: ErasureCodeJerasure::parse failed with error " << err << dendl;
    return err;
  }
  
  dout(15) << "SizeCeph parse: k=" << k << " m=" << m << dendl;
  
  // Validate that k=4 and m=5 for SizeCeph
  if (k != 4) {
    dout(0) << "SizeCeph parse: invalid k=" << k << " (must be 4)" << dendl;
    if (ss) {
      *ss << "SizeCeph requires exactly k=4 data chunks, got k=" << k << std::endl;
    }
    return -EINVAL;
  }
  
  if (m != 5) {
    dout(0) << "SizeCeph parse: invalid m=" << m << " (must be 5)" << dendl;
    if (ss) {
      *ss << "SizeCeph requires exactly m=5 coding chunks, got m=" << m << std::endl;
    }
    return -EINVAL;
  }
  
  dout(10) << "SizeCeph parse: profile parsed successfully" << dendl;
  return 0;
}

int ErasureCodeSizeCeph::_minimum_to_decode(const shard_id_set &want_to_read,
                                            const shard_id_set &available_chunks,
                                            shard_id_set *minimum) {
  dout(15) << "SizeCeph _minimum_to_decode: available_chunks.size()=" << available_chunks.size() 
           << " want_to_read.size()=" << want_to_read.size() << dendl;
  
  // SizeCeph requires exactly k (4) chunks to decode
  // We need to select any 4 available chunks for decoding
  
  if (available_chunks.size() < static_cast<size_t>(k)) {
    dout(0) << "SizeCeph _minimum_to_decode: not enough chunks available (" 
            << available_chunks.size() << " < " << k << ")" << dendl;
    return -EIO; // Not enough chunks available
  }
  
  minimum->clear();
  
  dout(20) << "SizeCeph _minimum_to_decode: trying data chunks first" << dendl;
  // Try to include data chunks first (0, 1, 2, 3)
  for (unsigned int i = 0; i < static_cast<unsigned int>(k) && minimum->size() < static_cast<size_t>(k); i++) {
    if (available_chunks.count(shard_id_t(i))) {
      minimum->insert(shard_id_t(i));
      dout(20) << "SizeCeph _minimum_to_decode: added data chunk " << i << dendl;
    }
  }
  
  dout(20) << "SizeCeph _minimum_to_decode: trying coding chunks if needed" << dendl;
  // If we don't have enough data chunks, add coding chunks (4, 5, 6, 7, 8)
  for (unsigned int i = static_cast<unsigned int>(k); i < get_chunk_count() && minimum->size() < static_cast<size_t>(k); i++) {
    if (available_chunks.count(shard_id_t(i))) {
      minimum->insert(shard_id_t(i));
      dout(20) << "SizeCeph _minimum_to_decode: added coding chunk " << i << dendl;
    }
  }
  
  if (minimum->size() < static_cast<size_t>(k)) {
    dout(0) << "SizeCeph _minimum_to_decode: still not enough chunks after coding selection (" 
            << minimum->size() << " < " << k << ")" << dendl;
    return -EIO; // Still not enough chunks
  }
  
  dout(15) << "SizeCeph _minimum_to_decode: selected " << minimum->size() << " chunks for decoding" << dendl;
  return 0;
}