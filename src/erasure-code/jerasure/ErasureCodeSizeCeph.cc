// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph-based Erasure Code Plugin Implementation integrated with Jerasure

#include "ErasureCodeSizeCeph.h"
#include <iostream>
#include <algorithm>
#include <dlfcn.h>
#include <cstring>

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
    return true;
  }
  
  // Try to load the sizeceph library from several possible locations
  const char* lib_paths[] = {
    "/home/joseph/code/sizeceph/sizeceph.so",
    "./sizeceph.so",
    "sizeceph.so",
    nullptr
  };
  
  for (int i = 0; lib_paths[i] != nullptr; i++) {
    sizeceph_handle = dlopen(lib_paths[i], RTLD_LAZY);
    if (sizeceph_handle) {
      break;
    }
  }
  
  if (!sizeceph_handle) {
    std::cerr << "Cannot load sizeceph library: " << dlerror() << std::endl;
    return false;
  }
  
  // Load function symbols
  size_split_func = (size_split_fn_t) dlsym(sizeceph_handle, "size_split");
  size_restore_func = (size_restore_fn_t) dlsym(sizeceph_handle, "size_restore");
  size_can_get_restore_func = (size_can_get_restore_fn_t) dlsym(sizeceph_handle, "size_can_get_restore_fn");
  
  if (!size_split_func || !size_restore_func || !size_can_get_restore_func) {
    std::cerr << "Cannot load sizeceph functions: " << dlerror() << std::endl;
    dlclose(sizeceph_handle);
    sizeceph_handle = nullptr;
    return false;
  }
  
  library_loaded = true;
  return true;
}

void ErasureCodeSizeCeph::unload_sizeceph_library() {
  if (sizeceph_handle) {
    dlclose(sizeceph_handle);
    sizeceph_handle = nullptr;
    library_loaded = false;
    size_split_func = nullptr;
    size_restore_func = nullptr;
    size_can_get_restore_func = nullptr;
  }
}

void ErasureCodeSizeCeph::jerasure_encode(char **data, char **coding, int blocksize) {
  if (!load_sizeceph_library()) {
    throw std::runtime_error("Failed to load sizeceph library");
  }
  
  // SizeCeph expects 4 data chunks and produces 9 total chunks (4 data + 5 coding)
  // The input should be interleaved 4-byte groups, and each output chunk gets blocksize bytes
  
  // Create input buffer by interleaving data chunks
  // SizeCeph processes data in 4-byte groups
  int input_size = blocksize * k;
  unsigned char *input_buffer = new unsigned char[input_size];
  
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
  
  // Call sizeceph split function
  size_split_func(temp_chunks, input_buffer, input_size);
  
  // Copy the data chunks back (should be identical to input data chunks)
  for (int i = 0; i < k; i++) {
    memcpy(data[i], temp_chunks[i], blocksize);
  }
  
  // Copy the coding chunks back
  for (int i = 0; i < m; i++) {
    memcpy(coding[i], temp_chunks[k + i], blocksize);
  }
  
  // Cleanup
  for (int i = 0; i < 9; i++) {
    delete[] temp_chunks[i];
  }
  delete[] input_buffer;
}

int ErasureCodeSizeCeph::jerasure_decode(int *erasures, char **data, char **coding, int blocksize) {
  if (!load_sizeceph_library()) {
    return -1;
  }
  
  // Count erasures
  int num_erasures = 0;
  while (erasures[num_erasures] != -1) {
    num_erasures++;
  }
  
  if (num_erasures > m) {
    return -1; // Too many erasures to recover
  }
  
  // Set up input chunks array for sizeceph (9 chunks total)
  // Allocate temporary chunks for available data
  unsigned char *temp_chunks[9];
  for (int i = 0; i < 9; i++) {
    temp_chunks[i] = new unsigned char[blocksize];
  }
  
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
    } else {
      memset(temp_chunks[i], 0, blocksize); // Zero out erased chunks
    }
  }
  
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
    } else {
      memset(temp_chunks[k + i], 0, blocksize); // Zero out erased chunks
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
  
  // Check if restoration is possible
  if (!size_can_get_restore_func(input_chunks)) {
    // Cleanup
    for (int i = 0; i < 9; i++) {
      delete[] temp_chunks[i];
    }
    return -1;
  }
  
  // Create output buffer for restored data
  int output_size = blocksize * k;
  unsigned char *output_buffer = new unsigned char[output_size];
  
  // Restore the original data
  int result = size_restore_func(output_buffer, input_chunks, output_size);
  
  if (result != 0) {
    delete[] output_buffer;
    for (int i = 0; i < 9; i++) {
      delete[] temp_chunks[i];
    }
    return -1;
  }
  
  // De-interleave restored data back to individual chunks
  for (int i = 0; i < blocksize; i++) {
    for (int j = 0; j < k; j++) {
      ((unsigned char*)data[j])[i] = output_buffer[i * k + j];
    }
  }
  
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
  // Load the library when preparing
  if (!load_sizeceph_library()) {
    throw std::runtime_error("Failed to load sizeceph library during prepare()");
  }
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
  int err = ErasureCodeJerasure::parse(profile, ss);
  if (err) {
    return err;
  }
  
  // Validate that k=4 and m=5 for SizeCeph
  if (k != 4) {
    if (ss) {
      *ss << "SizeCeph requires exactly k=4 data chunks, got k=" << k << std::endl;
    }
    return -EINVAL;
  }
  
  if (m != 5) {
    if (ss) {
      *ss << "SizeCeph requires exactly m=5 coding chunks, got m=" << m << std::endl;
    }
    return -EINVAL;
  }
  
  return 0;
}

int ErasureCodeSizeCeph::_minimum_to_decode(const shard_id_set &want_to_read,
                                            const shard_id_set &available_chunks,
                                            shard_id_set *minimum) {
  // SizeCeph requires exactly k (4) chunks to decode
  // We need to select any 4 available chunks for decoding
  
  if (available_chunks.size() < static_cast<size_t>(k)) {
    return -EIO; // Not enough chunks available
  }
  
  minimum->clear();
  
  // Try to include data chunks first (0, 1, 2, 3)
  for (unsigned int i = 0; i < static_cast<unsigned int>(k) && minimum->size() < static_cast<size_t>(k); i++) {
    if (available_chunks.count(shard_id_t(i))) {
      minimum->insert(shard_id_t(i));
    }
  }
  
  // If we don't have enough data chunks, add coding chunks (4, 5, 6, 7, 8)
  for (unsigned int i = static_cast<unsigned int>(k); i < get_chunk_count() && minimum->size() < static_cast<size_t>(k); i++) {
    if (available_chunks.count(shard_id_t(i))) {
      minimum->insert(shard_id_t(i));
    }
  }
  
  if (minimum->size() < static_cast<size_t>(k)) {
    return -EIO; // Still not enough chunks
  }
  
  return 0;
}