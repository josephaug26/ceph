// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph-based Erasure Code Plugin Implementation integrated with Jerasure
// This implements the SIZE algorithm with k=4, m=5 configuration

#ifndef CEPH_ERASURE_CODE_SIZE_CEPH_H
#define CEPH_ERASURE_CODE_SIZE_CEPH_H

#include "erasure-code/jerasure/ErasureCodeJerasure.h"

class ErasureCodeSizeCeph : public ErasureCodeJerasure {
public:
  ErasureCodeSizeCeph() : ErasureCodeJerasure("sizeceph") {
    DEFAULT_K = "4";  // NUM_DATA_SHARDS
    DEFAULT_M = "5";  // NUM_SRC_DISK - NUM_DATA_SHARDS = 9 - 4 = 5
    DEFAULT_W = "8";  // Word size in bits
  }
  ~ErasureCodeSizeCeph() override {}
  
  // Implement Jerasure virtual methods
  void jerasure_encode(char **data, char **coding, int blocksize) override;
  int jerasure_decode(int *erasures, char **data, char **coding, int blocksize) override;
  unsigned get_alignment() const override;
  void prepare() override;
  size_t get_minimum_granularity() override;
  void apply_delta(const shard_id_map<ceph::bufferptr> &in,
                   shard_id_map<ceph::bufferptr> &out) override;
                   
  // Override minimum_to_decode for SizeCeph-specific requirements
  int _minimum_to_decode(const shard_id_set &want_to_read,
                         const shard_id_set &available_chunks,
                         shard_id_set *minimum) override;
  using ErasureCode::_minimum_to_decode; // Bring other overloads into scope

private:
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
  
  // SizeCeph library interface functions
  static void* sizeceph_handle;
  static bool library_loaded;
  
  // Function pointers for dynamic loading
  typedef void (*size_split_fn_t)(unsigned char **pp_dst, unsigned char *p_src, unsigned int len);
  typedef int (*size_restore_fn_t)(unsigned char *p_dst, const unsigned char **pp_src, unsigned int len);
  typedef int (*size_can_get_restore_fn_t)(const unsigned char **pp_src);
  
  static size_split_fn_t size_split_func;
  static size_restore_fn_t size_restore_func;
  static size_can_get_restore_fn_t size_can_get_restore_func;
  
  bool load_sizeceph_library();
  void unload_sizeceph_library();
  
  // Helper function for internal padding
  static inline int calculate_aligned_size(int original_size);
};

#endif // CEPH_ERASURE_CODE_SIZE_CEPH_H