/*
 *   ALMA - Atacama Large Millimiter Array
 *   (c) European Southern Observatory, 2002
 *   Copyright by ESO (in the framework of the ALMA collaboration),
 *   All rights reserved
 *
 *   This library is free software; you can redistribute it and/or
 *   modify it under the terms of the GNU Lesser General Public
 *   License as published by the Free Software Foundation; either
 *   version 2.1 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public
 *   License along with this library; if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *   MA 02111-1307  USA
 *
 */

/******************************************************************************
 *
 * "@(#) $Id: ngamsCrc32.c,v 1.3 2008/08/19 20:51:50 jknudstr Exp $" 
 *
 * who       when      what
 * --------  --------  --------------------------------------------------
 * jknudstr  30/12/03  created
 */

/************************************************************************
 *   NAME
 *   ngamsCrc32 - Command line utility to calculate CRC-32 checksum
 *
 *   SYNOPSIS
 *   Invoke the tool without command parameters to get a help page.
 *
 *   DESCRIPTION
 *
 *   FILES
 *
 *   ENVIRONMENT
 *
 *   COMMANDS
 *
 *   RETURN VALUES
 *
 *   CAUTIONS 
 *
 *   EXAMPLES
 *
 *   SEE ALSO
 *
 *   BUGS   
 * 
 *-----------------------------------------------------------------------
 */

/* static char *rcsId="@(#) $Id: ngamsCrc32.c,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"; */
/* static void *use_rcsId = ((void)&use_rcsId,(void *) &rcsId); */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


void ngamsCorrectUsage()
{
    printf("\n\nngamsCrc32 <Filename>\n\n");
}


/* Main
 */
int main (int argc, char *argv[])
{
    char             buf[131072];
    int              crc = 0, fd, len;

    if (argc != 2)
	{
	ngamsCorrectUsage();
	exit(1);
	}
    if ((fd = open(argv[1], O_RDONLY)) == -1)
	{
	fprintf(stderr, "ERROR: Error opening file: %s", argv[1]);
	exit(1);
	}
    do
	{
	if ((len = read(fd, buf, 131072)) == -1)
	    {
	    fprintf(stderr, "ERROR: Error reading file: %s", argv[1]);
	    close(fd);
	    exit(1);
	    }
	if (len) crc = crc32(crc, buf, len);
	}
    while (len > 0);
    close(fd);
    printf("%d", crc);

    exit(0);
}


/* EOF */
