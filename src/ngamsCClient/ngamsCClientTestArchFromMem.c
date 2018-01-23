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
 * "@(#) $Id: ngamsCClientTestArchFromMem.c,v 1.3 2008/08/19 20:51:50 jknudstr Exp $" 
 *
 * who       when      what
 * --------  --------  --------------------------------------------------
 * jknudstr  03/09/02  created
 */

/************************************************************************
 *   NAME
 *   ngamsCClientTestArchFromMem - Application testing archiving from
 *                                 memory (ngamsArchiveFromMem()).
 *
 *   SYNOPSIS
 *   ngamsCClientTestArchFromMem <port> <host> <file>
 *
 *
 *   <port>:   Port number used by NG/AMS Server with which there is
 *             communicated.
 * 
 *   <host>:   Name of host where NG/AMS Server is running.
 *
 *   <file>:   Name of file to archive.
 *
 *
 *   DESCRIPTION
 *   Application to test the NG/AMS C-API function "ngamsArchiveFromMem()".
 *   It loads the specified file into memory and invokes the function.
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

static char *rcsId="@(#) $Id: ngamsCClientTestArchFromMem.c,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"; 
static void *use_rcsId = ((void)&use_rcsId,(void *) &rcsId);

#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "ngams.h"


void correctUsage()
{
    printf("> ngamsCClientTestArchFromMem <port> <host> <file>\n");
}


/* Main
 */
int main (int argc, char *argv[])
{
    int          portNo, fd = 0;
    char         host[64], filename[256], *tmpBuf = NULL;
    struct stat  statBuf;
    ngamsSTATUS  status;

     if (argc != 4) 
	 {
	 correctUsage();
	 exit(1);
	 }
     portNo = atoi(argv[1]);
     strcpy(host, argv[2]);
     strcpy(filename, argv[3]);

     /* Load the file to archive */
     stat(filename, &statBuf);
     tmpBuf = malloc(statBuf.st_size);
     fd = open(filename, O_RDONLY);
     read(fd, tmpBuf, statBuf.st_size);
     close(fd);

     /* Send the contents of the buffer to the NG/AMS Server */
     if (ngamsArchiveFromMem(host, portNo, ngamsNO_TIME_OUT, filename, tmpBuf,
			     statBuf.st_size, "", 0, 1, 
			     &status) != ngamsSTAT_SUCCESS)
         {
	 free(tmpBuf);
	 ngamsDumpStatStdout(&status);
	 exit(1);
	 }
     free(tmpBuf);

     /* Dump status */
     if (status.replyData[0] != NULL)
         printf("%s\n", status.replyData[0]);

     exit(0);
}


/* EOF */
