/*
 * Intel SSE 4.2 instruction probing implementation
 *
 * ICRAR - International Centre for Radio Astronomy Research
 * (c) UWA - The University of Western Australia, 2014
 * Copyright by UWA (in the framework of the ICRAR)
 * All rights reserved
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 * MA 02111-1307  USA
 *
 * Who       When        What
 * --------  ----------  -------------------------------------------------------
 * mark	    30/June/2014	ported from Intel C code
 * cwu      1/July/2014  	Created python callable library
 */

static inline
void do_cpuid(unsigned int *eax, unsigned int *ebx, unsigned int *ecx,
		     unsigned int *edx) {
	int id = *eax;

	asm("movl %4, %%eax;"
	    "cpuid;"
	    "movl %%eax, %0;"
	    "movl %%ebx, %1;"
	    "movl %%ecx, %2;"
	    "movl %%edx, %3;"
		: "=r" (*eax), "=r" (*ebx), "=r" (*ecx), "=r" (*edx)
		: "r" (id)
		: "eax", "ebx", "ecx", "edx");
}

int _crc32c_intel_probe(void) {
	unsigned int eax, ebx, ecx, edx;
	eax = 1;
	int crc32c_intel_available = 0;
	do_cpuid(&eax, &ebx, &ecx, &edx);
	crc32c_intel_available = (ecx & (1 << 20)) != 0;
	return crc32c_intel_available;

}
