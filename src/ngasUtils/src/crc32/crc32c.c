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
 */

#include "Python.h"

#include <stdio.h>
#include <inttypes.h>
#include <stdlib.h>
#include <sys/types.h>


static uint32_t crc32c_intel_le_hw_byte(uint32_t crc, unsigned char const *data,
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

static uint32_t crc32c_intel(uint32_t crc_init, unsigned char const *data, unsigned long length) {
	unsigned int iquotient = length / sizeof(uint64_t);
	unsigned int iremainder = length % sizeof(uint64_t);
	uint64_t *ptmp = (uint64_t *) data;
	uint32_t crc = crc_init;

	while (iquotient--) {
		__asm__ __volatile__(
			".byte 0xf2, " "0x48, 0xf, 0x38, 0xf1, 0xf1;"
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

static PyObject *
crc32c_crc32(PyObject *self, PyObject *args) {
	Py_buffer pbin;
	unsigned char *bin_data;
	uint32_t crc = 0U;      /* initial value of CRC for getting input */

	if (!PyArg_ParseTuple(args, "s*|I:crc32", &pbin, &crc) )
		return NULL;
	bin_data = pbin.buf;
	uint32_t result = crc32c_intel(crc, bin_data, pbin.len);

	PyBuffer_Release(&pbin);
	//printf("crc in c = %lX\n", result);
	return PyInt_FromLong(result);
}

static PyMethodDef CRC32CMethods[] = {
    {"crc32",  crc32c_crc32, METH_VARARGS,
     "CRC32C using Intel SSE4.2 instruction."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initcrc32c(void) {
    (void) Py_InitModule("crc32c", CRC32CMethods);
}


