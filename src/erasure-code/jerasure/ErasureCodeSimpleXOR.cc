// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// Simple XOR-based Erasure Code Plugin Implementation integrated with Jerasure

#include "ErasureCodeSimpleXOR.h"
#include <iostream>
#include <algorithm>

using std::ostream;
using ceph::ErasureCodeProfile;

// Implement Jerasure virtual methods
void ErasureCodeSimpleXOR::jerasure_encode(char **data, char **coding, int blocksize) {
  // Simple XOR: coding[0] = data[0] XOR data[1]
  xor_encode_internal(data[0], data[1], coding[0], blocksize);
}

int ErasureCodeSimpleXOR::jerasure_decode(int *erasures, char **data, char **coding, int blocksize) {
  // Count erasures
  int num_erasures = 0;
  while (erasures[num_erasures] != -1) {
    num_erasures++;
  }
  
  if (num_erasures > m) {
    return -1; // Too many erasures to recover
  }
  
  // Reconstruct missing chunks
  for (int i = 0; i < num_erasures; i++) {
    int missing = erasures[i];
    
    if (missing == 0) {
      // Reconstruct data[0]: data[0] = data[1] XOR coding[0]
      xor_encode_internal(data[1], coding[0], data[0], blocksize);
    } else if (missing == 1) {
      // Reconstruct data[1]: data[1] = data[0] XOR coding[0]  
      xor_encode_internal(data[0], coding[0], data[1], blocksize);
    } else if (missing == 2) {
      // Reconstruct coding[0]: coding[0] = data[0] XOR data[1]
      xor_encode_internal(data[0], data[1], coding[0], blocksize);
    }
  }
  
  return 0;
}

unsigned ErasureCodeSimpleXOR::get_alignment() const {
  return sizeof(int); // Simple alignment requirement
}

void ErasureCodeSimpleXOR::prepare() {
  // No special preparation needed for XOR
}

size_t ErasureCodeSimpleXOR::get_minimum_granularity() {
  return sizeof(int); // Simple granularity requirement
}

void ErasureCodeSimpleXOR::apply_delta(const shard_id_map<ceph::bufferptr> &in,
                                       shard_id_map<ceph::bufferptr> &out) {
  // For SimpleXOR, we use matrix-based delta application with identity matrix
  // This is a simple implementation - for production, optimize as needed
  int matrix[k * k];
  for (int i = 0; i < k; i++) {
    for (int j = 0; j < k; j++) {
      matrix[i * k + j] = (i == j) ? 1 : 0; // Identity matrix
    }
  }
  matrix_apply_delta(in, out, k, w, matrix);
}

int ErasureCodeSimpleXOR::parse(ErasureCodeProfile &profile, ostream *ss) {
  int err = ErasureCodeJerasure::parse(profile, ss);
  
  if (k != 2 || m != 1) {
    *ss << "SimpleXOR only supports k=2, m=1 configuration, got k=" 
        << k << " m=" << m << std::endl;
    return -EINVAL;
  }
  
  return err;
}

void ErasureCodeSimpleXOR::xor_encode_internal(char *data1, char *data2, char *result, int size) {
  for (int i = 0; i < size; i++) {
    result[i] = data1[i] ^ data2[i];
  }
}