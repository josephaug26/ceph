// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// SizeCeph Erasure Code Plugin Registration

#include "ceph_ver.h"
#include "common/debug.h"
#include "ErasureCodeSizeCeph.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginSizeCeph: ";
}

class ErasureCodePluginSizeCeph : public ceph::ErasureCodePlugin {
public:
  int factory(const std::string& directory,
              ceph::ErasureCodeProfile &profile,
              ceph::ErasureCodeInterfaceRef *erasure_code,
              std::ostream *ss) override {
    
    dout(10) << "SizeCeph plugin factory: creating direct ErasureCodeInterface instance" << dendl;
    
    ErasureCodeSizeCeph *interface = new ErasureCodeSizeCeph();
    
    dout(20) << __func__ << ": profile=" << profile << dendl;
    int r = interface->init(profile, ss);
    if (r) {
      delete interface;
      dout(0) << "SizeCeph plugin factory: init failed with error " << r << dendl;
      return r;
    }
    
    *erasure_code = ceph::ErasureCodeInterfaceRef(interface);
    dout(10) << "SizeCeph plugin factory: instance created successfully" << dendl;
    return 0;
  }
};

const char *__erasure_code_version() { 
  return CEPH_GIT_NICE_VER; 
}

int __erasure_code_init(char *plugin_name, char *directory)
{
  auto& instance = ceph::ErasureCodePluginRegistry::instance();
  auto plugin = std::make_unique<ErasureCodePluginSizeCeph>();
  int r = instance.add(plugin_name, plugin.get());
  if (r == 0) {
    plugin.release();
  }
  dout(10) << "SizeCeph plugin registered with name: " << plugin_name << dendl;
  return r;
}