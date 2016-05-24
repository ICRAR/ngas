/*
 *   This module provides crc32c checksum (http://www.rfc-editor.org/rfc/rfc3385.txt)
 *   based on the Intel CRC32 instruction
 *   provided in the Intel SSE4.2 instruction set
 *
 *   If SSE4.2 is not supported on the platform, this module will not be able to get installed
 *
 *    ICRAR - International Centre for Radio Astronomy Research
 *    (c) UWA - The University of Western Australia, 2014
 *    Copyright by UWA (in the framework of the ICRAR)
 *    All rights reserved
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *    MA 02111-1307  USA
 *
 * Who       When        What
 * --------  ----------  -------------------------------------------------------
 * mark	    30/June/2014	ported from Intel C code
 * cwu      1/July/2014  Created python extension module
 * dpallot  19/Feb/2015  Allow for execution on 32 and 64 bit platforms
 */

#include "Python.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <aio.h>

#ifdef __WORDSIZE
#define BITS_PER_LONG	__WORDSIZE
#else
#define BITS_PER_LONG	32
#endif

#if BITS_PER_LONG == 64
#define REX_PRE "0x48, "
#define SCALE_F 8
#else
#define REX_PRE
#define SCALE_F 4
#endif

static inline
uint32_t crc32c_intel_le_hw_byte(uint32_t crc, unsigned const char *data,
										unsigned long length) {
	while (length--) {
		__asm__ __volatile__(
			".byte 0xf2, 0xf, 0x38, 0xf0, 0xf1"
			:"=S"(crc)
			:"0"(crc), "c"(*data)
		);
		data++;
	}

	return crc;
}

static inline
uint32_t crc32c_intel(uint32_t crc, unsigned const char *data, unsigned long length) {
	unsigned int iquotient = length / SCALE_F;
	unsigned int iremainder = length % SCALE_F;

#if BITS_PER_LONG == 64
    uint64_t *ptmp = (uint64_t *) data;
#else
    uint32_t *ptmp = (uint32_t *) data;
#endif

	while (iquotient--) {
		__asm__ __volatile__(
			".byte 0xf2, " REX_PRE "0xf, 0x38, 0xf1, 0xf1;"
			:"=S"(crc)
			:"0"(crc), "c"(*ptmp)
		);
		ptmp++;
	}

	if (iremainder)
		crc = crc32c_intel_le_hw_byte(crc, (unsigned char *)ptmp,
				 iremainder);

	return crc;
}


static int read_write_crc(int fd_in, int fd_out,
                          float in_timeout,
                          size_t buffsize, uint64_t total,
                          int crc_type, uint32_t* crc,
                          unsigned long *crc_time,
								  unsigned long *write_time)
{

	struct timespec start, end;
	int stat;

	uint64_t remainder = total;
	int orig_in_flags = fcntl(fd_in, F_GETFL);
	stat = fcntl(fd_in, F_SETFL, orig_in_flags & ~O_NONBLOCK );
	if( stat ) {
		return -1;
	}
	int orig_out_flags = fcntl(fd_out, F_GETFL);
	stat = fcntl(fd_out, F_SETFL, orig_out_flags & ~O_DIRECT );
	if( stat ) {
		return -1;
	}

	struct timeval original_to, to;
	socklen_t size = sizeof(to);
	to.tv_sec = (long)floor(in_timeout);
	to.tv_usec = (long)(in_timeout - to.tv_sec);
	stat = getsockopt(fd_in, SOL_SOCKET, SO_RCVTIMEO, &original_to, &size);
	if( stat ) {
		return -1;
	}
	size = sizeof(to);
	stat = setsockopt(fd_in, SOL_SOCKET, SO_RCVTIMEO, &to, size);
	if( stat ) {
		return -1;
	}

	stat = 0;
#undef _POSIX_ASYNCHRONOUS_IO
#ifdef _POSIX_ASYNCHRONOUS_IO
	void* tmp_buff = malloc(2*buffsize);
#else
	void* tmp_buff = malloc(buffsize);
#endif
	if (tmp_buff == NULL) {
		return -1;
	}

#ifdef _POSIX_ASYNCHRONOUS_IO
	struct aiocb write_cb;
	const struct aiocb *aio_list[] = {&write_cb};
	memset(&write_cb, 0, sizeof(write_cb));
	unsigned long written = 0;
	unsigned int flip = 0;
	bool wrote_something = false;
	struct timespec one_sec = {1, 0};
#endif
	while (remainder) {

		size_t count = remainder >= buffsize ? buffsize : remainder;
#ifdef _POSIX_ASYNCHRONOUS_IO
		ssize_t readin = read(fd_in, tmp_buff + (flip ? 0 : buffsize), count);
#else
		ssize_t readin = read(fd_in, tmp_buff, count);
#endif
		if (readin == -1 || readin == 0) {
			stat = -2;
			break;
		}
		remainder -= readin;

#ifdef _POSIX_ASYNCHRONOUS_IO

		/* TODO: What to do?! */
		clock_gettime(CLOCK_MONOTONIC, &start);
		if( wrote_something ) {
			stat = aio_suspend(aio_list, 1, &one_sec);
			if( stat ) {
				errno = stat;
				break;
			}
			stat = aio_error(&write_cb);
			if( stat ) {
				errno = stat;
				break;
			}
			written += aio_return(&write_cb);
		}

		memset(&write_cb, 0, sizeof(write_cb));
		write_cb.aio_nbytes = readin;
		write_cb.aio_fildes = fd_out;
		write_cb.aio_offset = written;
		write_cb.aio_buf = tmp_buff + (flip ? 0 : buffsize);
		write_cb.aio_sigevent.sigev_notify = SIGEV_NONE;
		stat = aio_write(&write_cb);
		clock_gettime(CLOCK_MONOTONIC, &end);
		if( stat ) {
			stat = -3;
			break;
		}

		flip = !flip;
		wrote_something = true;
#else
		clock_gettime(CLOCK_MONOTONIC, &start);
		ssize_t writeout = write(fd_out, tmp_buff, readin);
		clock_gettime(CLOCK_MONOTONIC, &end);
		if (writeout < readin) {
			stat = -3;
			break;
		}
#endif
		*write_time += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_nsec - start.tv_nsec)/1000;

		/* Run the CRC32C and time it */
		clock_gettime(CLOCK_MONOTONIC, &start);
		*crc = crc32c_intel(*crc, tmp_buff, readin);
		clock_gettime(CLOCK_MONOTONIC, &end);
		*crc_time += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_nsec - start.tv_nsec)/1000;
	}

#ifdef _POSIX_ASYNCHRONOUS_IO
	if( wrote_something ) {
		clock_gettime(CLOCK_MONOTONIC, &start);
		stat = aio_suspend(aio_list, 1, &one_sec);
		if( !stat ) {
			stat = aio_error(&write_cb);
		}
		if( stat ) {
			errno = stat;
			stat = -4;
		}
		written += aio_return(&write_cb);
		clock_gettime(CLOCK_MONOTONIC, &end);
		*write_time += (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_nsec - start.tv_nsec)/1000;
	}

	if( written != total ) {
		stat = -4;
		printf("Didn't write all bytes? That's strange! Wrote %lu v/s %lu\n", written, total);
	}
#endif

	// TODO: check these two
	fcntl(fd_in, F_SETFL, orig_in_flags);
	fcntl(fd_out, F_SETFL, orig_out_flags);
	setsockopt(fd_in, SOL_SOCKET, SO_RCVTIMEO, &original_to, size);

	free(tmp_buff);
	return stat;
}

static PyObject *
crc32c_crc32(PyObject *self, PyObject *args) {
	Py_buffer pbin;
	unsigned char *bin_data = NULL;
	uint32_t crc = 0U;      /* initial value of CRC for getting input */

	if (!PyArg_ParseTuple(args, "s*|I:crc32", &pbin, &crc) )
		return NULL;

	bin_data = pbin.buf;
	uint32_t result = crc32c_intel(crc, bin_data, pbin.len);

	PyBuffer_Release(&pbin);

	return PyInt_FromLong(result);
}


static
PyObject* crc32c_crc32_and_consume(PyObject *self, PyObject *args) {

	int fd_in, fd_out;
	unsigned long buffsize;
	unsigned long total;
	unsigned short crc_type;
	uint32_t crc;
	int stat;
	float timeout;
	unsigned long crc_time = 0;
	unsigned long write_time = 0;
	Py_buffer pbin;
	unsigned char *buff_data;

	if (!PyArg_ParseTuple(args, "is*ifkkH", &fd_in, &pbin, &fd_out, &timeout, &buffsize, &total, &crc_type) )
		return NULL;

	buff_data = pbin.buf;
	crc = crc32c_intel(0U, buff_data, pbin.len);
	stat = write(fd_out, buff_data, pbin.len);
	if( stat == -1 || stat != pbin.len ) {
		PyBuffer_Release(&pbin);
		char *error = strerror(errno);
		char *fmt = "Error while writing initial data: %s";
		char *msg = (char *)malloc(strlen(error) + strlen(fmt) - 1);
		sprintf(msg, fmt, error);
		PyErr_SetString(PyExc_Exception, msg);
		free(msg);
		return NULL;
	}
	total -= pbin.len;
	PyBuffer_Release(&pbin);

	Py_BEGIN_ALLOW_THREADS
	stat = read_write_crc(fd_in, fd_out, timeout, buffsize, total, crc_type, &crc, &crc_time, &write_time);
	Py_END_ALLOW_THREADS

	if( stat ) {
		stat = -1*stat - 1;
		char *error = strerror(errno);
		char *action[] = {"preparing to loop", "reading", "writing", "completing writing"};
		char *fmt = "Error while %s: %s";
		char *msg = (char *)malloc(strlen(error) + strlen(fmt) + strlen(action[stat]) - 1);
		sprintf(msg, fmt, action[stat], error);
		PyErr_SetString(PyExc_Exception, msg);
		free(msg);
		return NULL;
	}


	PyObject* res = PyTuple_New(3);
	PyTuple_SetItem(res, 0, PyInt_FromLong(crc));
	PyTuple_SetItem(res, 1, PyInt_FromLong(crc_time));
	PyTuple_SetItem(res, 2, PyInt_FromLong(write_time));
	return res;
}

static PyMethodDef CRC32CMethods[] = {
	{"crc32",  crc32c_crc32, METH_VARARGS, "CRC32C using Intel SSE4.2 instruction."},
	{"crc32_and_consume", crc32c_crc32_and_consume, METH_VARARGS, "CRC32C using Intel SSE4.2 while reading and writing"},
	{NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initcrc32c(void) {
    (void) Py_InitModule("crc32c", CRC32CMethods);
}
