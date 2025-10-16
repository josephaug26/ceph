// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph-based Erasure Code Plugin - Direct ErasureCodeInterface Implementation
// This implements the SIZE algorithm independently of Jerasure framework

#ifndef CEPH_ERASURE_CODE_SIZE_CEPH_H
#define CEPH_ERASURE_CODE_SIZE_CEPH_H

#include "erasure-code/ErasureCodeInterface.h"
#include "osd/osd_types.h"
#include <mutex>

class ErasureCodeSizeCeph : public ceph::ErasureCodeInterface {
public:
  ErasureCodeSizeCeph();
  ~ErasureCodeSizeCeph() override;
  
  // ErasureCodeInterface required virtual methods
  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;
  const ceph::ErasureCodeProfile &get_profile() const override;
  int create_rule(const std::string &name, CrushWrapper &crush, std::ostream *ss) const override;
  
  unsigned int get_chunk_count() const override;
  unsigned int get_data_chunk_count() const override;
  unsigned int get_coding_chunk_count() const override;
  int get_sub_chunk_count() override;
  unsigned int get_chunk_size(unsigned int stripe_width) const override;
  
  int minimum_to_decode(const shard_id_set &want_to_read,
                        const shard_id_set &available,
                        shard_id_set &minimum_set,
                        mini_flat_map<shard_id_t, std::vector<std::pair<int, int>>> *minimum_sub_chunks) override;
  
  [[deprecated]]
  int minimum_to_decode(const std::set<int> &want_to_read,
                        const std::set<int> &available,
                        std::map<int, std::vector<std::pair<int, int>>> *minimum) override;
  
  int minimum_to_decode_with_cost(const shard_id_set &want_to_read,
                                  const shard_id_map<int> &available,
                                  shard_id_set *minimum) override;
  
  [[deprecated]]
  int minimum_to_decode_with_cost(const std::set<int> &want_to_read,
                                  const std::map<int, int> &available,
                                  std::set<int> *minimum) override;
  
  size_t get_minimum_granularity() override;
  
  // Encode methods
  int encode(const shard_id_set &want_to_encode,
             const ceph::bufferlist &in,
             shard_id_map<ceph::bufferlist> *encoded) override;
  
  [[deprecated]]
  int encode(const std::set<int> &want_to_encode,
             const ceph::bufferlist &in,
             std::map<int, ceph::bufferlist> *encoded) override;
  
  [[deprecated]]
  int encode_chunks(const std::set<int> &want_to_encode,
                    std::map<int, ceph::bufferlist> *encoded) override;
  
  int encode_chunks(const shard_id_map<ceph::bufferptr> &in,
                    shard_id_map<ceph::bufferptr> &out) override;
  
  void encode_delta(const ceph::bufferptr &old_data,
                    const ceph::bufferptr &new_data,
                    ceph::bufferptr *delta_maybe_in_place) override;
  
  void apply_delta(const shard_id_map<ceph::bufferptr> &in,
                   shard_id_map<ceph::bufferptr> &out) override;
  
  // Decode methods
  int decode(const shard_id_set &want_to_read,
             const shard_id_map<ceph::bufferlist> &chunks,
             shard_id_map<ceph::bufferlist> *decoded, int chunk_size) override;
  
  [[deprecated]]
  int decode(const std::set<int> &want_to_read,
             const std::map<int, ceph::bufferlist> &chunks,
             std::map<int, ceph::bufferlist> *decoded, int chunk_size) override;
  
  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<ceph::bufferptr> &in,
                    shard_id_map<ceph::bufferptr> &out) override;
  
  [[deprecated]]
  int decode_chunks(const std::set<int> &want_to_read,
                    const std::map<int, ceph::bufferlist> &chunks,
                    std::map<int, ceph::bufferlist> *decoded) override;
  
  const std::vector<shard_id_t> &get_chunk_mapping() const override;
  
  [[deprecated]]
  int decode_concat(const std::set<int>& want_to_read,
                    const std::map<int, ceph::bufferlist> &chunks,
                    ceph::bufferlist *decoded) override;
  
  [[deprecated]]
  int decode_concat(const std::map<int, ceph::bufferlist> &chunks,
                    ceph::bufferlist *decoded) override;
  
  plugin_flags get_supported_optimizations() const override;

private:
  // SizeCeph configuration
  static const unsigned int SIZECEPH_K = 4;    // Data chunks
  static const unsigned int SIZECEPH_M = 5;    // Parity chunks  
  static const unsigned int SIZECEPH_N = 9;    // Total chunks (K+M)
  static const unsigned int SIZECEPH_ALGORITHM_ALIGNMENT = 4;    // SizeCeph processes 4 bytes at a time
  static const unsigned int SIZECEPH_MIN_BLOCK_SIZE = 512;       // Storage block alignment
  
  ceph::ErasureCodeProfile profile;
  std::vector<shard_id_t> chunk_mapping;
  
  // SizeCeph library interface
  static void* sizeceph_handle;
  static bool library_loaded;
  static int library_ref_count;
  static std::mutex library_mutex;
  
  typedef void (*size_split_fn_t)(unsigned char **pp_dst, unsigned char *p_src, unsigned int len);
  typedef int (*size_restore_fn_t)(unsigned char *p_dst, const unsigned char **pp_src, unsigned int len);
  typedef int (*size_can_get_restore_fn_t)(const unsigned char **pp_src);
  
  static size_split_fn_t size_split_func;
  static size_restore_fn_t size_restore_func;
  static size_can_get_restore_fn_t size_can_get_restore_func;
  
  bool load_sizeceph_library();
  void unload_sizeceph_library();
  void unload_sizeceph_library_unsafe(); // Internal version without mutex
  
  // Helper methods
  int calculate_aligned_size(int original_size) const;
  void interleave_data(const char *src, char **dst_chunks, int chunk_size, int num_chunks) const;
  void deinterleave_data(char **src_chunks, char *dst, int chunk_size, int num_chunks) const;
  
  // Internal encode/decode with always-decode architecture
  int sizeceph_encode_internal(const ceph::bufferlist &in, shard_id_map<ceph::bufferlist> *encoded);
  int sizeceph_decode_internal(const shard_id_map<ceph::bufferlist> &chunks, 
                               ceph::bufferlist *decoded, int original_size);
};

#endif // CEPH_ERASURE_CODE_SIZE_CEPH_H