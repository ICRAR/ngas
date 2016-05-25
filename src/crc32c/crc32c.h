#ifndef _CRC32C_H_
#define _CRC32C_H_

#include <stdint.h>

uint32_t _crc32c_intel(uint32_t crc, unsigned const char *data, unsigned long length);

#endif
