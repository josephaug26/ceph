# SizeCeph erasure code plugin

set(sizeceph_plugin_src
  ErasureCodePluginSizeCeph.cc
  ErasureCodeSizeCeph.cc)

add_library(sizeceph_plugin_obj OBJECT ${sizeceph_plugin_src})
target_link_libraries(sizeceph_plugin_obj legacy-option-headers)

add_library(ec_sizeceph SHARED $<TARGET_OBJECTS:sizeceph_plugin_obj>)
target_link_libraries(ec_sizeceph ${CMAKE_DL_LIBS})
set_target_properties(ec_sizeceph PROPERTIES
  INSTALL_RPATH ""
  VERSION 2.0.0
  SOVERSION 2
  CXX_VISIBILITY_PRESET hidden)

install(TARGETS ec_sizeceph DESTINATION ${erasure_code_dir})