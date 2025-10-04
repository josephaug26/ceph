// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// Simple XOR-based Erasure Code Plugin Example
// This implements a basic (k=2, m=1) XOR parity code integrated with Jerasure

#ifndef CEPH_ERASURE_CODE_SIMPLE_XOR_H
#define CEPH_ERASURE_CODE_SIMPLE_XOR_H

#include "erasure-code/jerasure/ErasureCodeJerasure.h"

class ErasureCodeSimpleXOR : public ErasureCodeJerasure {
public:
  ErasureCodeSimpleXOR() : ErasureCodeJerasure("simple_xor") {
    DEFAULT_K = "2";
    DEFAULT_M = "1"; 
    DEFAULT_W = "8";
  }
  ~ErasureCodeSimpleXOR() override {}
  
  // Implement Jerasure virtual methods
  void jerasure_encode(char **data, char **coding, int blocksize) override;
  int jerasure_decode(int *erasures, char **data, char **coding, int blocksize) override;
  unsigned get_alignment() const override;
  void prepare() override;
  size_t get_minimum_granularity() override;
  void apply_delta(const shard_id_map<ceph::bufferptr> &in,
                   shard_id_map<ceph::bufferptr> &out) override;

private:
  void xor_encode_internal(char *data1, char *data2, char *parity, int size);
  int parse(ceph::ErasureCodeProfile& profile, std::ostream *ss) override;
};

#endif // CEPH_ERASURE_CODE_SIMPLE_XOR_H