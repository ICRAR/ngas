#ifndef _CRC32C_CONSUME_H_
#define _CRC32C_CONSUME_H_

int _crc32c_read_crc_write(int fd_in, int fd_out,
                           float in_timeout,
                           size_t buffsize, uint64_t total,
                           int crc_type, uint32_t* crc,
                           unsigned long *crc_time,
                           unsigned long *write_time);

#endif
