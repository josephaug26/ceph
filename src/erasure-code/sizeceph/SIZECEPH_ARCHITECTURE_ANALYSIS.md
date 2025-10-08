# SizeCeph Architecture Analysis: Jerasure Integration Issues

## Executive Summary

This document analyzes the fundamental architectural incompatibility between the SizeCeph erasure coding algorithm and Ceph's Jerasure framework, documenting why the current integration approach fails and proposing a new direct integration strategy.

## Problem Statement

### Current Integration Issues
- **Data Corruption**: Large objects (10MB+) show MD5 hash mismatches during read operations
- **Missing Decode Operations**: SizeCeph decode logic never executes during normal read operations
- **Architectural Mismatch**: SizeCeph's data transformation model conflicts with Jerasure's data preservation model

### Root Cause Analysis

#### Jerasure Framework Assumptions
1. **Data Preservation Model**: Jerasure assumes first `k` chunks contain original data unchanged
2. **Conditional Decode**: Decode is only called when erasures exist (`erasures_count > 0`)
3. **Reed-Solomon Compatibility**: Framework designed for algorithms where data chunks remain intact

#### SizeCeph Algorithm Reality
1. **Data Transformation Model**: All 9 chunks contain transformed/mixed data from original
2. **Always-Decode Requirement**: Original data reconstruction requires processing all chunks
3. **Non-Reed-Solomon Design**: Algorithm fundamentally different from traditional RS codes

## Technical Evidence

### Ceph Decode Calling Logic
```cpp
// From ErasureCodeJerasure.cc decode() function
ceph_assert(erasures_count > 0);  // Decode ONLY called with missing chunks
```

### SizeCeph Data Mixing Example
```c
// From size_split_func in SizeCeph library
pp_dst[1][i] = p_src[offset + 2];  // Data rearranged, not preserved
```

### Integration Failure Points
1. **Encode Phase**: Current implementation corrupts data by copying transformed chunks back to original data arrays
2. **Read Phase**: Normal reads bypass decode entirely, returning corrupted transformed data
3. **Decode Phase**: When decode is called (erasures only), it assumes Reed-Solomon behavior

## Test Results

### Small Object Tests (1KB)
- **Status**: ✅ PASS
- **Reason**: Small objects fit in single chunk, minimal transformation impact

### Large Object Tests (10MB) 
- **Status**: ❌ FAIL
- **Original MD5**: `ca3883326ab39538af01738aa0e07376`
- **Retrieved MD5**: `33702aa64cf2c4570126cd10f7c1fe7c`
- **Corruption Confirmed**: Data integrity compromised

## Proposed Solution: Direct ErasureCodeInterface Integration

### New Architecture Design
```
Ceph Erasure Code Framework
├── ErasureCodeInterface (Base)
├── ErasureCodeJerasure (Reed-Solomon variants)
│   ├── rs
│   ├── lrc
│   └── shec
└── ErasureCodeSizeCeph (Direct implementation)  ← NEW
    └── sizeceph
```

### Key Implementation Changes

#### 1. Remove Jerasure Inheritance
- Create standalone `ErasureCodeSizeCeph` class
- Implement `ErasureCodeInterface` directly
- Remove dependency on Jerasure framework

#### 2. Always-Decode Architecture
- Implement proper encode/decode cycle for all reads
- Ensure decode is called for every read operation
- Handle erasures through SizeCeph's native reconstruction

#### 3. Plugin Registration
- Register as independent erasure code type
- Add to CMakeLists.txt as separate library
- Configure proper plugin loading mechanism

## Implementation Strategy

### Phase 1: Remove Current Integration
1. Delete current `ErasureCodeSizeCeph.cc` implementation
2. Remove references from Jerasure plugin registration
3. Clean up build configuration

### Phase 2: Create Direct Implementation
1. Create new `ErasureCodeSizeCeph` inheriting from `ErasureCodeInterface`
2. Implement all required virtual methods
3. Add proper encode/decode cycle with SizeCeph library calls

### Phase 3: Plugin Integration
1. Create separate CMakeLists.txt for SizeCeph plugin
2. Add plugin registration in main erasure code factory
3. Configure proper library loading and symbol resolution

## Benefits of New Architecture

### Performance Considerations
- **Always-Decode Impact**: Additional CPU overhead for all reads
- **Mitigation Strategy**: Leverage SizeCeph's efficiency optimizations
- **Trade-off**: Computational cost vs storage efficiency gains

### Architectural Advantages
1. **Clean Separation**: SizeCeph operates independently of Reed-Solomon assumptions
2. **Proper Integration**: Follows Ceph's plugin architecture correctly
3. **Maintainability**: Easier to debug and extend without Jerasure conflicts
4. **Flexibility**: Can implement SizeCeph-specific optimizations

## Technical Specifications

### SizeCeph Library Interface
- **Encode**: `size_split_func(src, k=4, m=5, dst_chunks)`
- **Decode**: `size_restore_func(src_chunks, k=4, m=5, dst)`
- **Configuration**: k=4, m=5 (4 data + 5 parity chunks)

### Ceph Integration Points
- **ErasureCodeInterface**: Base class providing framework interface
- **Plugin System**: Dynamic loading via `.so` library
- **CRUSH Integration**: Proper placement group distribution

## Conclusion

The current SizeCeph integration through Jerasure is fundamentally flawed due to conflicting architectural assumptions. The proposed direct integration approach aligns with SizeCeph's always-decode requirement and provides a clean, maintainable solution within Ceph's erasure code framework.

---

**Date**: October 7, 2025  
**Analysis**: Based on comprehensive testing and code examination  
**Recommendation**: Implement direct ErasureCodeInterface integration  
**Status**: Architecture redesign required