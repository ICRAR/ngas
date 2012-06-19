#ifndef ngamsArchiveClient_H
#define ngamsArchiveClient_H

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
 * "@(#) $Id: ngamsArchiveClient.h,v 1.8 2009/03/04 20:29:53 awicenec Exp $"
 *
 * who       when      what
 * --------  --------  ----------------------------------------------
 * jknudstr  30/08/01  created
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "ngams.h"

/* Directories/filenames */
#define ngamsARCH_CLI_NAME           "ngamsArchiveClient"
#define ngamsARCH_CLI_DIR            "NGAMS_ARCHIVE_CLIENT"
#define ngamsARCH_CLI_LOG_DIR        "log"
#define ngamsARCH_CLI_QUE_DIR        "queue"
#define ngamsARCH_CLI_ARC_DIR        "archived"
#define ngamsARCH_CLI_BAD_DIR        "bad"
#define ngamsARCH_CLI_LOG_FILE       "ngamsArchiveClient.log"
#define ngamsARCH_CLI_STAT_EXT       "___STATUS.xml"
#define ngamsMAX_FILES_IN_Q          4096
#define ngamsMAX_ARCHIVE_THREADS     128


typedef struct {
    ino_t          inodeNo;
    ngamsMED_BUF   name;
} ngamsDirInfo;


/* Structure to handle common information in connection with the threads
 * running within the archiev client.
 */
typedef struct {
    /* Command line options */
    ngamsMED_BUF     remoteHost;
    int              remotePort;
    ngamsMED_BUF     servers;
    ngamsMED_BUF     auth;
    int              streams;
    ngamsMED_BUF     rootDir;
    ngamsMED_BUF     checksum;
    ngamsSMALL_BUF   mimeType;
    ngamsPAR_ARRAY   parArray;
    float            archiveQueuePollTime;
    int              cleanUpTimeOut;
    int              immediateCleanUp;
    int              verboseLevel;
    int              logLevel;
    int              logRotate;
    int              logHistory;
    int              archiveLog;
    ngamsMED_BUF     serverCmd;

    /* Archive Queue handling */
    pthread_mutex_t  archiveQueueMutex;
    int              archiveQueueRdIdx;
    int              archiveQueueWrIdx;
    int              archiveQueueCount;
    ngamsMED_BUF     archiveQueue[ngamsMAX_FILES_IN_Q];
    DB*              queueDbmHandle;
    DB*              procDbmHandle;

    /* Thread handling */
    int              threadRunPermission;
    pthread_t        archiveQMonThread;
    pthread_t        archiveThreads[ngamsMAX_ARCHIVE_THREADS];
    pthread_t        cleanUpThread;
} ngamsARCHIVE_CLIENT_REGISTRY;


#ifdef __cplusplus
}
#endif

#endif /* ngamsArchiveClient_H */
