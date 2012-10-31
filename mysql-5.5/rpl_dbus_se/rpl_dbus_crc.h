#ifndef RPL_DBUS_CRC_H
#define RPL_DBUS_CRC_H

#include "my_global.h"

// non-standard crc algorithm that was mistakenly thought
// to be CRC-32
uint32 rpl_dbus_crc(uchar* buf, uint len);

#endif