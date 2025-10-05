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
  
  if (!load_sizeceph_library()) {
    dout(0) << "Failed to load sizeceph library for encoding" << dendl;
    throw std::runtime_error("Failed to load sizeceph library");
  }
  
  dout(15) << "SizeCeph encode: library loaded, starting encoding process" << dendl;
  
  // SizeCeph expects 4 data chunks and produces 9 total chunks (4 data + 5 coding)
  // The input should be interleaved 4-byte groups, and each output chunk gets blocksize bytes
  
  // Create input buffer by interleaving data chunks
  // SizeCeph processes data in 4-byte groups
  int input_size = blocksize * k;
  unsigned char *input_buffer = new unsigned char[input_size];
  
  dout(20) << "SizeCeph encode: creating input buffer of size " << input_size << dendl;
  
  // Interleave the k data chunks into input buffer
  for (int i = 0; i < blocksize; i++) {
    for (int j = 0; j < k; j++) {
      input_buffer[i * k + j] = ((unsigned char*)data[j])[i];
    }
  }
  
  // Allocate temporary output chunks for sizeceph (each gets blocksize bytes)
  unsigned char *temp_chunks[9];
  for (int i = 0; i < 9; i++) {
    temp_chunks[i] = new unsigned char[blocksize];
  }
  
  dout(15) << "SizeCeph encode: calling size_split function" << dendl;
  // Call sizeceph split function
  size_split_func(temp_chunks, input_buffer, input_size);
  dout(15) << "SizeCeph encode: size_split completed successfully" << dendl;
  
  // Copy the data chunks back (should be identical to input data chunks)
  for (int i = 0; i < k; i++) {
    memcpy(data[i], temp_chunks[i], blocksize);
  }
  
  // Copy the coding chunks back
  for (int i = 0; i < m; i++) {
    memcpy(coding[i], temp_chunks[k + i], blocksize);
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
  
  if (!load_sizeceph_library()) {
    dout(0) << "Failed to load sizeceph library for decoding" << dendl;
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
  
  // Set up input chunks array for sizeceph (9 chunks total)
  // Allocate temporary chunks for available data
  unsigned char *temp_chunks[9];
  for (int i = 0; i < 9; i++) {
    temp_chunks[i] = new unsigned char[blocksize];
  }
  
  dout(15) << "SizeCeph decode: copying available data chunks" << dendl;
  // Copy available data chunks
  for (int i = 0; i < k; i++) {
    bool is_erased = false;
    for (int j = 0; j < num_erasures; j++) {
      if (erasures[j] == i) {
        is_erased = true;
        break;
      }
    }
    if (!is_erased) {
      memcpy(temp_chunks[i], data[i], blocksize);
      dout(20) << "SizeCeph decode: copied data chunk " << i << dendl;
    } else {
      memset(temp_chunks[i], 0, blocksize); // Zero out erased chunks
      dout(20) << "SizeCeph decode: zeroed erased data chunk " << i << dendl;
    }
  }
  
  dout(15) << "SizeCeph decode: copying available coding chunks" << dendl;
  // Copy available coding chunks
  for (int i = 0; i < m; i++) {
    bool is_erased = false;
    for (int j = 0; j < num_erasures; j++) {
      if (erasures[j] == k + i) {
        is_erased = true;
        break;
      }
    }
    if (!is_erased) {
      memcpy(temp_chunks[k + i], coding[i], blocksize);
      dout(20) << "SizeCeph decode: copied coding chunk " << (k + i) << dendl;
    } else {
      memset(temp_chunks[k + i], 0, blocksize); // Zero out erased chunks
      dout(20) << "SizeCeph decode: zeroed erased coding chunk " << (k + i) << dendl;
    }
  }
  
  // Set up input pointers for sizeceph restore (NULL for erased chunks)
  const unsigned char *input_chunks[9];
  for (int i = 0; i < 9; i++) {
    bool is_erased = false;
    for (int j = 0; j < num_erasures; j++) {
      if (erasures[j] == i) {
        is_erased = true;
        break;
      }
    }
    input_chunks[i] = is_erased ? nullptr : temp_chunks[i];
  }
  
  dout(15) << "SizeCeph decode: checking if restoration is possible" << dendl;
  // Check if restoration is possible
  if (!size_can_get_restore_func(input_chunks)) {
    dout(0) << "SizeCeph decode: restoration not possible with available chunks" << dendl;
    // Cleanup
    for (int i = 0; i < 9; i++) {
      delete[] temp_chunks[i];
    }
    return -1;
  }
  
  dout(15) << "SizeCeph decode: restoration possible, proceeding with restore" << dendl;
  
  // Create output buffer for restored data
  int output_size = blocksize * k;
  unsigned char *output_buffer = new unsigned char[output_size];
  
  dout(15) << "SizeCeph decode: calling size_restore function" << dendl;
  // Restore the original data
  int result = size_restore_func(output_buffer, input_chunks, output_size);
  
  if (result != 0) {
    dout(0) << "SizeCeph decode: size_restore failed with result=" << result << dendl;
    delete[] output_buffer;
    for (int i = 0; i < 9; i++) {
      delete[] temp_chunks[i];
    }
    return -1;
  }
  
  dout(15) << "SizeCeph decode: size_restore completed successfully" << dendl;
  
  // De-interleave restored data back to individual chunks
  for (int i = 0; i < blocksize; i++) {
    for (int j = 0; j < k; j++) {
      ((unsigned char*)data[j])[i] = output_buffer[i * k + j];
    }
  }
  
  dout(10) << "SizeCeph decode: decoding completed successfully" << dendl;
  
  // Cleanup
  delete[] output_buffer;
  for (int i = 0; i < 9; i++) {
    delete[] temp_chunks[i];
  }
  return 0;
}

unsigned ErasureCodeSizeCeph::get_alignment() const {
  return 4; // SizeCeph processes data in 4-byte groups
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
  return 4; // SizeCeph requires data to be aligned to 4-byte boundaries
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