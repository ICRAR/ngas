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
 * "@(#) $Id: ngamsCClientLib.c,v 1.30 2010/05/04 09:44:20 hsommer Exp $"
 *
 * who       when      what
 * --------  --------  --------------------------------------------------
 * jknudstr  30/08/01  created
 */

/**
 NAME
 ngamsCClientLib - NG/AMS C-API library

 SYNOPSIS
 #include "ngams.h"


 Before starting to use the NG/AMS C-API the function ngamsInitApi() must
 be called.


 If HTTP authorization is enabled on the server side, it is necessary to
 call the function ngamsSetAuthorization() with an access code, which is
 provided by the responsibles of the server side before starting to
 communicate with the remote server.


 In general for the NG/AMS interface functions listed below,
 the "host" parameter is the name of the host where the NG/AMS Server
 is running. E.g.: "arcdev1.hq.eso.org". The "port" parameter is the
 socket port, which the NG/AMS Server is waiting on.

 If the parameter "wait" is set to 0, an immediate reply to the
 request will be generated, i.e. before the request has been handled.

 The parameter "status" is a structure containing the following members:

 Data Type       Member     Description
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 ngamsSMALL_BUF  date       Date for handling query.
 ngamsSTAT       errorCode  Error code giving status for the query.
 See #1.
 ngamsSMALL_BUF  hostId     Host ID for host where the NG/AMS Server
 is running.
 ngamsHUGE_BUF   message    Message from the NG/AMS Server.
 ngamsSMALL_BUF  status     Status of query ("OK" | "FAILURE").
 ngamsSMALL_BUF  state      State of the NG/AMS Server.
 ngamsSMALL_BUF  subState   Sub-State of the NG/AMS Server.
 ngamsSMALL_BUF  version    Version of the NG/AMS Server.
 char            replyData  Pointer array of pointers pointing to
 allocated buffers containing the reply data
 from the request.


 #1: The following error codes (internal to the NG/AMS C API)
 are defined (data type: ngamsSTAT):

 Error Macro             Description
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 ngamsSTAT_SUCCESS       Query successfully executed.

 ngamsERR_HOST           No such host.
 ngamsERR_SOCK           Cannot create socket.
 ngamsERR_CON            Cannot connect to host/server.

 ngamsERR_COM            Problem occurred during socket connection.
 ngamsERR_TIMEOUT        Timeout encountered while communication with
 server.

 ngamsERR_WR_HD          Write error on socket while writing header.
 ngamsERR_WR_DATA        Write error on socket while writing data.
 ngamsERR_RD_DATA        Read error while reading data.
 ngamsERR_INV_REPLY      Invalid reply from data server.

 ngamsERR_FILE           Invalid filename specified.
 ngamsERR_ALLOC_MEM      Cannot allocate memory.

 ngamsERR_UNKNOWN_STAT   Unknown status code.
 ngamsERR_UNKNOWN_CMD    Unknown command issued.
 ngamsERR_INV_TARG_FILE  Invalid target filename specified.
 ngamsERR_INV_PARS       Invalid parameters given.

 ngamsSRV_OK             Request successfully handled by server.
 ngamsSRV_REDIRECT       The reply is an HTTP redirection response.

 ngamsSRV_INV_QUERY      Invalid query.


 Apart from that, the errors defined by NG/AMS can be returned.


 All functions return ngamsSTAT_SUCCESS in case of success. In case of
 an error a termination status within the set of status codes
 given above.


 The following macros are defined for referring to NG/AMS commands:

 Command Macros (#2)       Description
 - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 ngamsCMD_ARCHIVE          Archive file.
 ngamsCMD_CLONE            Clone files.
 ngamsCMD_EXIT             Make NG/AMS Server exit.
 ngamsCMD_INIT             Re-initialize the server (same as Offline->Online).
 ngamsCMD_LABEL            Make NG/AMS print out a disk label.
 ngamsCMD_ONLINE           Bring NG/AMS Server Online.
 ngamsCMD_OFFLINE          Bring NG/AMS Server Offline.
 ngamsCMD_REGISTER         Register files on a disk.
 ngamsCMD_REMDISK          Remove a disk from NGAS.
 ngamsCMD_REMFILE          Remove a file from NGAS.
 ngamsCMD_RETRIEVE         Retrieve a file.
 ngamsCMD_STATUS           Query status information from NG/AMS.
 ngamsCMD_SUBSCRIBE        Subscribe to a NG/AMS Server.
 ngamsCMD_UNSUBSCRIBE      Un-subscribe/cancel a previous subscription.

 #2: All command macros exist also as a string, which carries the name
 of the enumerated macro name with a "_STR" appended.


 In the following, the functions provided for interacting with NG/AMS
 are listed. The specific parameters for each function are listed in
 connection with the function. The parameters used by several
 functions are as follows:

 host:         Host name of NG/AMS Server to contact.

 port:         Port number used by NG/AMS Server to contact.

 timeoutSecs:  Timeout in seconds to apply while executing the request.

 wait:         Wait for the NG/AMS Server to finish the request (=1)
 completely, or return an immediate response (=0).

 status:       Pointer to ngamsSTATUS structure containing the
 status of the query.

 parArray:     Instance of the ngamsPAR_ARRAY structure containing the
 parameters of the query. This should be filled with the
 function: ngamsAddParAndVal(). Before usage, the structure
 should be initialized invoking ngamsResetParArray() on it.


 LOGGING/DEBUGGING
 It is possible to control the generation of log output into a log file and/or
 on stdout via the following environment variables:

 - NGAMS_LOG_FILE:      Name of a log file into which there is logged.

 - NGAMS_LOG_LEVEL:     The log level to apply when logging into the
 associated log file.

 - NGAMS_VERBOSE_LEVEL: Log level to apply when logging on stdout.

 This should normally only be used for troubleshooting purpose though.


 EXAMPLES
 To archive a file using the API the following must be called from
 the application:

 #include "ngams.h"

 ngamsSTATUS      status;
 if (ngamsArchive("wfinau", "7171", 10, "/home/data/MyFile.fits", "",
 1, 0, &status) != ngamsSTAT_SUCCESS)
 {
 ngamsDumpStatStdout(&status);
 ... error handling ...
 }


 To retrieve a file into the directory "/home/data/target_dir". The
 name will be the same as the File ID:

 #include "ngams.h"

 ngamsSTATUS      status;
 if (ngamsRetrieve2File("wfinau", "7171", 30,
 "WFI.2001-10-21T23:24:03.925",
 -1, "", "", "/home/data/target_dir",
 &status) != ngamsSTAT_SUCCESS)
 {
 ngamsDumpStatStdout(&status);
 ... error handling ...
 }


 CAUTIONS
 Remember to perform a call to ngamsInitStatus() right after declaring
 an instance of the ngamsSTATUS class, and a call to ngamsFreeStatus()
 after each call to one of the commands used to issue commands to NG/AMS.
 Memory may be allocated dynamically and needs to be freed.

 If data is returned by the NG/AMS Server the member "replyData" will
 have a pointer pointing to each block of data. It is the responsibility
 of the calling application to free these data chunks. The function
 "ngamsFreeStatus()" can be used for this.

 -----------------------------------------------------------------------------
 */

/* static char *rcsId="@(#) $Id: ngamsCClientLib.c,v 1.30 2010/05/04 09:44:20 hsommer Exp $"; */
/* static void *use_rcsId = ((void)&use_rcsId,(void *) &rcsId); */

/* Should be defined in fcntl.h ... */
/*#define O_LARGEFILE	0100000 - JKN/2008-02-18: CHECK */

/* Enable 64-bits definitions */
#define _FILE_OFFSET_BITS 64

#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifndef S_IFMT
#define S_IFMT   __S_IFMT
#define S_IFDIR  __S_IFDIR
#endif

#include "ngams.h"
#include "ngamsVERSION.h"
#include "ngamsCClientGlobals.h"

char* _ngamsLicense(void);
char* _ngamsManPage(void);

/* IMPL: Add entry/exit debug logs in all functions where relevant. */

/**
 *******************************************************************************
 Some global variables - should get rid of them.
 *******************************************************************************
 */

/* Server contact information. */
pthread_mutex_t _genMutex;
ngamsSRV_INFO _srvInfoList[ngamsMAX_SRVS];

void _ngamsLockSrvInfoSem() {
	//ngamsLogDebug("Entering _ngamsLockSrvInfoSem() ...");
	pthread_mutex_lock(&_genMutex);
	//ngamsLogDebug("Leaving _ngamsLockSrvInfoSem()");
}

void _ngamsUnlockSrvInfoSem() {
	//ngamsLogDebug("Entering _ngamsUnlockSrvInfoSem() ...");
	pthread_mutex_unlock(&_genMutex);
	//ngamsLogDebug("Leaving _ngamsUnlockSrvInfoSem()");
}

/* For checking the duplicate socket that might be assigned during multi-thread program*/
int sockList[ngamsMAX_SOCKS];
int registerSock(int sockFd) {
	int n;
	for (n = 0; n < ngamsMAX_SOCKS; n++) {
		if (sockList[n] == 0) {
			sockList[n] = sockFd;
			return ngamsSTAT_SUCCESS;
		}
	}
	return ngamsSTAT_FAILURE;
}
int unregisterSock(int sockFd) {
	ngamsLogDebug("UnregisterSock socket(%d)", sockFd);

	if (sockFd == 0)
		return ngamsSTAT_SUCCESS;
	int n;
	for (n = 0; n < ngamsMAX_SOCKS; n++) {
		if (sockList[n] == sockFd) {
			sockList[n] = 0;
			return ngamsSTAT_SUCCESS;
		}
	}
	return ngamsSTAT_FAILURE;
}
int existedSock(int sockFd) {
	int n;
	for (n = 0; n < ngamsMAX_SOCKS; n++) {
		if (sockList[n] == sockFd) {
			return 1;
		}
	}
	return 0;
}

void ngamsInitApi(void) {
	static int initialized;
	int n;

	ngamsLogDebug("Entering ngamsInitApi() ...");

	if (initialized)
		return;
	pthread_mutex_init(&_genMutex, NULL);
	for (n = 0; n < ngamsMAX_SRVS; n++)
		memset(&(_srvInfoList[n]), 0, sizeof(ngamsSRV_INFO));
	for (n = 0; n < ngamsMAX_SOCKS; n++)
		sockList[n] = 0;
	initialized = 1;

	ngamsLogDebug("Leaving ngamsInitApi()");
}

void _ngamsGetSrvInfoObj(const char* listId, int* foundList,
		ngamsSRV_INFO** srvInfoP) {
	int n;

	ngamsLogDebug("Entering _ngamsGetSrvInfoObj() ...");

	_ngamsLockSrvInfoSem();
	*foundList = 0;
	*srvInfoP = NULL;
	for (n = 0; n < ngamsMAX_SRVS; n++) {
		if (strcmp(listId, _srvInfoList[n].id) == 0) {
			*foundList = 1;
			break;
		} else if (*(_srvInfoList[n].id) == '\0')
			break;
	}
	if (n < ngamsMAX_SRVS)
		*srvInfoP = &_srvInfoList[n];
	_ngamsUnlockSrvInfoSem();

	ngamsLogDebug("Leaving _ngamsGetSrvInfoObj()");
}

ngamsSTAT _ngamsGetNextSrv(int* idx, ngamsSRV_INFO* srvInfoP, char** host,
		int* port) {
	ngamsLogDebug("Entering _ngamsGetNextSrv() ...");

	_ngamsLockSrvInfoSem();
	if (*idx == -1) {
		srvInfoP->srvIdx = ((srvInfoP->srvIdx + 1) % srvInfoP->numberOfSrvs);
		*idx = srvInfoP->srvIdx;
	} else
		*idx = ((*idx + 1) % srvInfoP->numberOfSrvs);
	*host = srvInfoP->hosts[*idx];
	*port = srvInfoP->ports[*idx];
	_ngamsUnlockSrvInfoSem();
	ngamsLogDebug("Next server: %s:%d", *host, *port);

	ngamsLogDebug("Leaving _ngamsGetNextSrv()");

	return ngamsSTAT_SUCCESS;
}

/* Authorization user/password */
char* _authorization = NULL;

/* Log Conditions */
ngamsMED_BUF _logFile;
int _logLevel = 0;
int _logRotate = -1;
int _logHistory = -1;
int _verboseLevel = 0;

/**
 ******************************************************************************
 */

/**
 *  FUNCTIONS
 */

/**
 ******************************************************************************
 * Miscellaneous, internal utility functions.
 ******************************************************************************
 */

char getLastChar(const char* str) {
	return str[strlen(str) - 1];
}

ngamsSTAT safeStrNCp(char* dest, const char* src, const int len,
		const int maxLen) {
	if (len >= maxLen) {
		ngamsLogError("Error copying value: |%s| into string buffer, source "
			"too long for destination buffer (%d/%d)", src, maxLen, len);
		return ngamsSTAT_FAILURE;
	}
	strncpy(dest, src, len);
	*(dest + len) = '\0';

	return ngamsSTAT_SUCCESS;
}

ngamsSTAT safeStrCp(char* dest, const char* src, const int maxLen) {
	return safeStrNCp(dest, src, strlen(src), maxLen);
}

void ngamsSleep(const float sleepTime) {
	struct timespec sleepT, timeSlept;
	sleepT.tv_sec = (time_t) sleepTime;
	sleepT.tv_nsec = (long) (1e9 * (sleepTime - sleepT.tv_sec));
	nanosleep(&sleepT, &timeSlept);
}

/**
 *******************************************************************************
 */

/**
 *******************************************************************************
 Functions to Handle NG/AMS Commands.
 *******************************************************************************
 */

/**
 ngamsSTAT ngamsUnpackStatus(const char*    xmlDoc,
 ngamsSTATUS*   status)
 Unpack an XML Status Document received from the NG/AMS Server.

 xmlDoc:     XML Status Document (string).

 status:     Instance of the ngamsSTATUS structure, which will contain
 the deoded status.

 Returns:    One of the following status codes:

 ngamsSTAT_SUCCESS or ngamsERR_INV_REPLY
 */
ngamsSTAT ngamsUnpackStatus(const char* xmlDoc, ngamsSTATUS* status) {
	char errCode[64], *tmpBuf = NULL;
	int OK = ngamsSTAT_SUCCESS, stat;

	ngamsLogDebug("Entering ngamsUnpackStatus() ...");
	if ((ngamsGetXmlAttr(xmlDoc, "Status", "Date", sizeof(ngamsSMALL_BUF),
			status->date) != OK) || (ngamsGetXmlAttr(xmlDoc, "Status",
			"HostId", sizeof(ngamsSMALL_BUF), status->hostId) != OK)
			|| (ngamsGetXmlAttr(xmlDoc, "Status", "Message",
					sizeof(ngamsHUGE_BUF), status->message) != OK)
			|| (ngamsGetXmlAttr(xmlDoc, "Status", "State",
					sizeof(ngamsSMALL_BUF), status->state) != OK)
			|| (ngamsGetXmlAttr(xmlDoc, "Status", "SubState",
					sizeof(ngamsSMALL_BUF), status->subState) != OK)
			|| (ngamsGetXmlAttr(xmlDoc, "Status", "Status",
					sizeof(ngamsSMALL_BUF), status->status) != OK)
			|| (ngamsGetXmlAttr(xmlDoc, "Status", "Version",
					sizeof(ngamsSMALL_BUF), status->version) != OK)) {
		ngamsStat2Str(ngamsERR_INV_REPLY, status->message);
		status->errorCode = ngamsERR_INV_REPLY;
		stat = ngamsERR_INV_REPLY;
		goto errExit;
	}
	if ((strcmp(status->status, NGAMS_FAILURE) == 0) && ((strstr(
			status->message, ":ERROR:") != NULL) || (strstr(status->message,
			":WARNING:") != NULL) || (strstr(status->message, ":ALERT:")
			!= NULL))) {
		tmpBuf = malloc(strlen(status->message) + 1);
		strcpy(tmpBuf, status->message);
		strtok(tmpBuf, ":");
		memset(errCode, 0, (64 * sizeof(char)));
		strncpy(errCode, strtok(NULL, ":"), 64);
		if (errCode != NULL)
			status->errorCode = atoi(errCode);
		free(tmpBuf);
	} else
		status->errorCode = ngamsSTAT_SUCCESS;

	if ((strcmp(status->status, NGAMS_FAILURE) != 0) && (strcmp(status->status,
			NGAMS_SUCCESS) != 0)) {
		ngamsStat2Str(ngamsERR_INV_REPLY, status->message);
		status->errorCode = ngamsERR_INV_REPLY;
		stat = ngamsERR_INV_REPLY;
		goto errExit;
	}

	/* Make the data reply array refer to the data received */
	status->replyData[0] = (char*) xmlDoc;

	ngamsLogDebug("Leaving ngamsUnpackStatus().");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsLogDebug("Leaving ngamsUnpackStatus()/FAILURE. Status: %d",
			stat);
	return stat;
}

/**
 ngamsSTAT ngamsHandleStatus(int              retCode,
 ngamsHTTP_DATA*  repDataRef,
 ngamsSTATUS*     status)
 Handle the status of an HTTP response. The retCode is return value from
 the ngamsHttpGet() or ngamsHttpPost() functions.

 retCode:       Return code from ngamsHttpGet() or ngamsHttpPost().

 repDataRef:    Instance of the ngamsHTTP_DATA structure containing the
 the reference to the data in the HTTP response.

 status:        Instance of the ngamsSTATUS structure, which will contain
 the deoded status.

 Returns:       ngamsSTAT_SUCCESS.
 */
ngamsSTAT ngamsHandleStatus(int retCode, float timeout, ngamsHTTP_DATA* repDataRef,
		ngamsSTATUS* status) {
	ngamsSMALL_BUF errBuf;

	ngamsLogDebug("Entering ngamsHandleStatus() ...");
	ngamsInitStatus(status);
	if (repDataRef->pdata != NULL)
		ngamsUnpackStatus(repDataRef->pdata, status);
	else {
		ngamsStat2Str(retCode, status->message);
		if ((retCode != ngamsSRV_OK) && ((retCode != ngamsSTAT_SUCCESS)))
			strcpy(status->status, NGAMS_FAILURE);
		else
			strcpy(status->status, NGAMS_SUCCESS);
		status->errorCode = retCode;
	}

	/* If the error reported is ngamsERR_TIMEOUT we add the time-out time
	 * to the message for completenes.
	 */
	if (status->errorCode == ngamsERR_TIMEOUT) {
		if( timeout < 0 ) {
			timeout = ngamsDEFAULT_TIME_OUT;
		}
		sprintf(errBuf, ". Timeout: %.3fs", (float) (timeout));
		strcat(status->message, errBuf);
	}

	ngamsLogDebug("Leaving ngamsHandleStatus()");
	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsGenSendData(const char*            host,
 const int              port,
 const ngamsCMD         cmdCode,
 const float            timeoutSecs,
 const char*            fileUri,
 const char*            mimeType,
 const ngamsPAR_ARRAY*  parArray,
 ngamsSTATUS*           status)

 If the URI referring to the file is a filename, the function will carry
 out an HTTP POST request. Otherwise if the URI is a URL, the function
 will make an HTTP GET request.

 fileUri:        URI referring to file.

 mimeType:       Mime-type of data. If set to '\0' a generic mime-type will
 be contained in the HTTP header 'Content-Type' and the
 NG/AMS Server will determine the mime-type itself.

 parArray:       Instance of the ngamsPAR_ARRAY structure containing the
 parameters of the query.

 Returns:        Returns ngamsSTAT_SUCCESS or a status code returned by
 one of the functions:

 ngamsHttpGet(), ngamsHttpPost()
 */
ngamsSTAT ngamsGenSendData(const char* host, const int port,
		const ngamsCMD cmdCode, const float timeoutSecs, const char* fileUri,
		const char* mimeType, const ngamsPAR_ARRAY* parArray,
		ngamsSTATUS* status) {
	int retCode, i;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF mtBuf, url, tmpBuf, tmpFileUri, tmpEnc, cmd;
	ngamsHUGE_BUF contDisp;

	ngamsLogDebug("Entering ngamsGenSendData() ...");
	ngamsInitStatus(status);
	memset(url, 0, sizeof(ngamsMED_BUF));

	ngamsCmd2Str(cmdCode, cmd);
	ngamsEncodeUrlVal(fileUri, 1, tmpFileUri);

	if ((strstr(fileUri, "file:") != NULL)
			|| (strstr(fileUri, "http:") != NULL) || (strstr(fileUri, "ftp:")
			!= NULL)) {
		/* Data is pulled */
		sprintf(url, "%s?filename=\"%s\"&mime_type=\"%s\"", cmd, tmpFileUri,
				mimeType);
		for (i = 0; i < parArray->idx; i++) {
			ngamsEncodeUrlVal(parArray->valArray[i], 1, tmpEnc);
			sprintf(tmpBuf, "&%s=\"%s\"", parArray->parArray[i], tmpEnc);
			strcat(url, tmpBuf);
		}
		if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, url, 1,
				&repDataRef, &repDataLen, &httpResp, httpHdr))
				!= ngamsSTAT_SUCCESS)
			goto errExit;
	} else {
		/* Data is pushed */
		if (strstr(tmpFileUri, "uid%3A") != NULL)
			sprintf(contDisp, "attachment; filename=\"%s\"",
					strstr(tmpFileUri, "uid%3A"));
		else
			sprintf(contDisp, "attachment; filename=\"%s\"", tmpFileUri);
		for (i = 0; i < parArray->idx; i++) {
			ngamsEncodeUrlVal(parArray->valArray[i], 1, tmpEnc);
			sprintf(tmpBuf, "; %s=\"%s\"", parArray->parArray[i], tmpEnc);
			strcat(contDisp, tmpBuf);
		}
		if (*mimeType == '\0')
			strcpy(mtBuf, ngamsARHIVE_REQ_MT);
		else
			strcpy(mtBuf, mimeType);
		if ((retCode = ngamsHttpPost(host, port, timeoutSecs, ngamsUSER_AGENT, cmd, mtBuf,
				contDisp, fileUri, "", 0, &repDataRef, &repDataLen, &httpResp,
				httpHdr)) != ngamsSTAT_SUCCESS)
			goto errExit;
	}
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsGenSendData()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsGenSendData()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsArchive(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         fileUri,
 const char*         mimeType,
 const int           noVersioning,
 const int           wait,
 ngamsSTATUS*        status)
 Archive a file into the NGAS Archive.

 fileUri:      Reference name for the file to archive.

 mimeType:     The mime-type of the file to archive. In some cases
 it is not possible for NG/AMS to determine the mime-type
 of a data file to be archived, e.g. when the file being is
 archived is RETRIEVEd from another NGAS Host. For efficiency
 it is thus better to indicate the mime-type to enable
 NG/AMS to store the file directly on the target disk.
 If not use this can be put to "".

 noVersioning: If set to 1 no new File Version will be generated for
 the file being archived even though a file with that
 File ID was already existing.

 Returns:      Execution status (ngamsSTAT_SUCCESS|ngamsSTAT_FAILURE).
 */
ngamsSTAT ngamsArchive(const char* host, const int port,
		const float timeoutSecs, const char* fileUri, const char* mimeType,
		const int noVersioning, const int wait, ngamsSTATUS* status) {
	int stat;
	ngamsMED_BUF tmpBuf;
	ngamsPAR_ARRAY parArray;

	ngamsLogDebug("Entering ngamsArchive() ...");
	ngamsResetParArray(&parArray);
	sprintf(tmpBuf, "%d", noVersioning);
	ngamsAddParAndVal(&parArray, "no_versioning", tmpBuf);
	sprintf(tmpBuf, "%d", wait);
	ngamsAddParAndVal(&parArray, "wait", tmpBuf);
	if (timeoutSecs != -1)
		sprintf(tmpBuf, "%d", (int) (timeoutSecs + 0.5));
	else
		sprintf(tmpBuf, "-1");
	ngamsAddParAndVal(&parArray, "time_out", tmpBuf);
	stat = ngamsGenSendData(host, port, ngamsCMD_ARCHIVE, timeoutSecs, fileUri,
			mimeType, &parArray, status);
	ngamsLogDebug("Leaving ngamsArchive()");
	return stat;
}

/**
 ngamsSTAT ngamsQArchive(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         fileUri,
 const char*         mimeType,
 const int           noVersioning,
 const int           wait,
 ngamsSTATUS*        status)
 Quick Archive a file into the NGAS Archive.

 fileUri:      Reference name for the file to archive.

 mimeType:     The mime-type of the file to archive. In some cases
 it is not possible for NG/AMS to determine the mime-type
 of a data file to be archived, e.g. when the file being is
 archived is RETRIEVEd from another NGAS Host. For efficiency
 it is thus better to indicate the mime-type to enable
 NG/AMS to store the file directly on the target disk.
 If not use this can be put to "".

 noVersioning: If set to 1 no new File Version will be generated for
 the file being archived even though a file with that
 File ID was already existing.

 Returns:      Execution status (ngamsSTAT_SUCCESS|ngamsSTAT_FAILURE).
 */
ngamsSTAT ngamsQArchive(const char* host, const int port,
		const float timeoutSecs, const char* fileUri, const char* mimeType,
		const int noVersioning, const int wait, ngamsSTATUS* status) {
	int stat;
	ngamsMED_BUF tmpBuf;
	ngamsPAR_ARRAY parArray;

	ngamsLogDebug("Entering ngamsQArchive() ...");
	ngamsResetParArray(&parArray);
	/**
	 * We decided not to support this non_version business,
	 * which will create inconsistency on the server side if a single server has multiple disk volumes
	 *
	 int versioning = 1;
	 if (noVersioning) {
	 versioning = 0;
	 }
	 */
	sprintf(tmpBuf, "%d", noVersioning);
	//sprintf(tmpBuf, "%d", versioning);
	ngamsAddParAndVal(&parArray, "no_versioning", tmpBuf);
	//ngamsAddParAndVal(&parArray, "versioning", tmpBuf);
	sprintf(tmpBuf, "%d", wait);
	ngamsAddParAndVal(&parArray, "wait", tmpBuf);
	if (timeoutSecs != -1)
		sprintf(tmpBuf, "%d", (int) (timeoutSecs + 0.5));
	else
		sprintf(tmpBuf, "-1");
	ngamsAddParAndVal(&parArray, "time_out", tmpBuf);
	stat = ngamsGenSendData(host, port, ngamsCMD_QARCHIVE, timeoutSecs,
			fileUri, mimeType, &parArray, status);
	ngamsLogDebug("Leaving ngamsQArchive()");
	return stat;
}

/**
 ngamsSTAT ngamsPArchive(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         fileUri,
 const char*         mimeType,
 const int           noVersioning,
 const int           wait,
 ngamsSTATUS*        status)
 Quick Archive a file into the NGAS Archive.

 fileUri:      Reference name for the file to archive.

 mimeType:     The mime-type of the file to archive. In some cases
 it is not possible for NG/AMS to determine the mime-type
 of a data file to be archived, e.g. when the file being is
 archived is RETRIEVEd from another NGAS Host. For efficiency
 it is thus better to indicate the mime-type to enable
 NG/AMS to store the file directly on the target disk.
 If not use this can be put to "".

 noVersioning: If set to 1 no new File Version will be generated for
 the file being archived even though a file with that
 File ID was already existing.

 Returns:      Execution status (ngamsSTAT_SUCCESS|ngamsSTAT_FAILURE).
 */
ngamsSTAT ngamsPArchive(const char* host, const int port,
		const float timeoutSecs, const char* fileUri, const char* mimeType,
		const int noVersioning, const int wait, const char* nexturl,
		ngamsSTATUS* status) {
	int stat;
	ngamsMED_BUF tmpBuf;
	ngamsPAR_ARRAY parArray;

	ngamsLogDebug("Entering ngamsQArchive() ...");
	ngamsResetParArray(&parArray);
	sprintf(tmpBuf, "%d", noVersioning);
	ngamsAddParAndVal(&parArray, "no_versioning", tmpBuf);
	sprintf(tmpBuf, "%d", wait);
	ngamsAddParAndVal(&parArray, "wait", tmpBuf);
	if (timeoutSecs != -1)
		sprintf(tmpBuf, "%d", (int) (timeoutSecs + 0.5));
	else
		sprintf(tmpBuf, "-1");
	ngamsAddParAndVal(&parArray, "time_out", tmpBuf);
	ngamsAddParAndVal(&parArray, "nexturl", nexturl);
	stat = ngamsGenSendData(host, port, ngamsCMD_PARCHIVE, timeoutSecs,
			fileUri, mimeType, &parArray, status);
	ngamsLogDebug("Leaving ngamsQArchive()");
	return stat;
}

/**
 ngamsSTAT ngamsArchiveFromMem(const char*       host,
 const int         port,
 const float       timeoutSecs,
 const char*       fileUri,
 const char*       bufPtr,
 const int         size,
 const char*       mimeType,
 const int         noVersioning,
 const int         wait,
 ngamsSTATUS*      status)
 Archive a file which contents have been loaded into a buffer in memory.

 fileUri,
 mimeType,
 noVersioning: See ngamsArchive().

 bufPtr:       Pointer to buffer containing the contents of the file.

 size:         Size in bytes of the data loaded into memory.

 Returns:      See ngamsArchive().
 */
ngamsSTAT ngamsArchiveFromMem(const char* host, const int port,
		const float timeoutSecs, const char* fileUri, const char* bufPtr,
		const int size, const char* mimeType, const int noVersioning,
		const int wait, ngamsSTATUS* status) {
	int retCode, locTimeout;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, contDisp, mtBuf, tmpFileUri;

	ngamsLogDebug("Entering ngamsArchiveFromMem() ...");
	ngamsInitStatus(status);
	if (*mimeType != '\0')
		sprintf(mtBuf, "&mime_type=\"%s\"", mimeType);

	ngamsEncodeUrlVal(fileUri, 1, tmpFileUri);

	/* Perform an Archive Push */
	if (timeoutSecs == -1)
		locTimeout = -1;
	else
		locTimeout = (int) (timeoutSecs + 0.5);
	sprintf(contDisp, "attachment; filename=\"%s\"; wait=\"%d\"; "
		"no_versioning=\"%d\"; time_out\"%d\"", tmpFileUri, wait, noVersioning,
			locTimeout);
	if (*mimeType != '\0')
		strcat(tmpUrl, mtBuf);
	if ((retCode = ngamsHttpPost(host, port, timeoutSecs, ngamsUSER_AGENT, "ARCHIVE",
			ngamsARHIVE_REQ_MT, contDisp, "", bufPtr, size, &repDataRef,
			&repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsArchiveFromMem()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsArchiveFromMem()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsClone(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         fileId,
 const int           fileVersion,
 const char*         diskId,
 const char*         targetDiskId,
 const int           wait,
 ngamsSTATUS*        status)
 Execute a CLONE command. For the exact interpretation of various
 combinations of fileId, fileVersion and diskId, consult the man-page
 for the NG/AMS Python module "ngamsCloneCmd", function: "clone()".

 fileId:          ID of file to clone.

 fileVersion:     Version of files to be taken into account for the
 cloning.

 diskId:          Disk ID for the files to be taken into account.

 targetDiskId:    ID of disk where to stored the cloned files.

 Returns:         ngamsSTAT_SUCCESS or one of the error codes returned by
 the function ngamsHttpGet()
 */
ngamsSTAT ngamsClone(const char* host, const int port, const float timeoutSecs,
		const char* fileId, const int fileVersion, const char* diskId,
		const char* targetDiskId, const int wait, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, tmpBuf;

	ngamsLogDebug("Entering ngamsClone() ...");
	ngamsInitStatus(status);
	sprintf(tmpUrl, "%s?wait=\"%d\"", ngamsCMD_CLONE_STR, wait);
	if (*fileId != '\0') {
		sprintf(tmpBuf, "&file_id=\"%s\"", fileId);
		strcat(tmpUrl, tmpBuf);
	}
	if (fileVersion != -1) {
		sprintf(tmpBuf, "&file_version=%d", fileVersion);
		strcat(tmpUrl, tmpBuf);
	}
	if (*diskId != '\0') {
		sprintf(tmpBuf, "&disk_id=\"%s\"", diskId);
		strcat(tmpUrl, tmpBuf);
	}
	if (*targetDiskId != '\0') {
		sprintf(tmpBuf, "&target_disk_id=\"%s\"", targetDiskId);
		strcat(tmpUrl, tmpBuf);
	}
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsClone()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsClone()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsExit(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const int           wait,
 ngamsSTATUS*        status)
 Send an EXIT command to the NG/AMS Server to make it clean up and
 terminate execution.

 Returns:         ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsExit(const char* host, const int port, const float timeoutSecs,
		const int wait, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl;

	ngamsLogDebug("Entering ngamsExit() ...");
	ngamsInitStatus(status);
	sprintf(tmpUrl, "%s?wait=\"%d\"", ngamsCMD_EXIT_STR, wait);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsExit()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsExit()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsLabel(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         slotId,
 ngamsSTATUS*        status)
 Send a LABEL command to the NG/AMS Server.

 slotId:    ID of slot hosting the disk for which to generate the label.

 Returns:   ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsLabel(const char* host, const int port, const float timeoutSecs,
		const char* slotId, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, tmpEnc;

	ngamsLogDebug("Entering ngamsLabel() ...");
	ngamsInitStatus(status);
	ngamsEncodeUrlVal(slotId, 1, tmpEnc);
	sprintf(tmpUrl, "%s?slot_id=\"%s\"", ngamsCMD_LABEL_STR, tmpEnc);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsLabel()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsLabel()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsOnline(const char*          host,
 const int            port,
 const float          timeoutSecs,
 const int            wait,
 ngamsSTATUS*         status)
 Send an ONLINE command to the NG/AMS Server to bring it to Online State.

 Returns:         ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsOnline(const char* host, const int port,
		const float timeoutSecs, const int wait, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl;

	ngamsLogDebug("Entering ngamsOnline() ...");
	ngamsInitStatus(status);
	sprintf(tmpUrl, "%s?wait=\"%d\"", ngamsCMD_ONLINE_STR, wait);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsOnline()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsOnline()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsOffline(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const int           force,
 const int           wait,
 ngamsSTATUS*        status)

 Send an OFFLINE command to the NG/AMS Server to bring it to Offline State.

 force:     Force the server to go Offline immediately, even though it is busy.

 Returns:   ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsOffline(const char* host, const int port,
		const float timeoutSecs, const int force, const int wait,
		ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl;

	ngamsLogDebug("Entering ngamsOffline() ...");
	ngamsInitStatus(status);
	sprintf(tmpUrl, "%s?wait=\"%d\"", ngamsCMD_OFFLINE_STR, wait);
	if (force)
		strcat(tmpUrl, "&force=1");
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsOffline()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsOffline()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsParseSrvListId(const char*   listId,
 const char*   servers)
 Parse a comma separated list of server nodes and port numbers of the form:

 "<Host 1>:<Port 1>,<Host 2>:<Port 2>,..."

 The server/port pairs are kept in an internal registry to be used when
 communicating with the remote server(s).

 listId:    ID for the list for future reference.

 servers:   List of servers/ports.

 Returns:   ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsParseSrvListId(const char* listId, const char* servers) {
	char* nextTok;
	char* strPtr;
	int port, foundList;
	ngamsHUGE_BUF srvsLoc;
	ngamsMED_BUF host;
	ngamsSRV_INFO* srvInfoP;

	ngamsLogDebug("Entering ngamsParseSrvListId() ...");

	ngamsLogDebug("ngamsParseSrvListId(): listId=%s, servers=%s", listId,
			servers);
	_ngamsGetSrvInfoObj(listId, &foundList, &srvInfoP);
	if (srvInfoP == NULL) {
		ngamsLogError("Illegal Server List ID or no more free slots");
		return ngamsSTAT_FAILURE;
	}

	_ngamsLockSrvInfoSem();
	memset(srvInfoP, 0, sizeof(ngamsSRV_INFO));
	strcpy(srvInfoP->id, listId);
	srvInfoP->srvIdx = -1;
	strcpy(srvsLoc, servers);
	nextTok = strtok_r(srvsLoc, ",", &strPtr);
	while (nextTok != NULL) {
		if (ngamsSplitSrvAddr(nextTok, host, &port) != ngamsSTAT_SUCCESS) {
			_ngamsUnlockSrvInfoSem();
			ngamsLogDebug("Leaving ngamsParseSrvList()/FAILURE");
			return ngamsSTAT_FAILURE;
		}
		strcpy(srvInfoP->hosts[srvInfoP->numberOfSrvs], host);
		srvInfoP->ports[srvInfoP->numberOfSrvs] = port;
		srvInfoP->numberOfSrvs++;
		nextTok = strtok_r(NULL, ",", &strPtr);
	}
	_ngamsUnlockSrvInfoSem();

	ngamsLogDebug("Leaving ngamsParseSrvListId()");

	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsParseSrvList(const char* servers)
 Parse a comma separated list of server nodes and port numbers of the form:

 "<Host 1>:<Port 1>,<Host 2>:<Port 2>,..."

 The server/port pairs are kept in an internal registry to be used when
 communicating with the remote server(s).

 servers:   List of servers/ports.

 Returns:   ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsParseSrvList(const char* servers) {
	ngamsLogDebug("Entering ngamsParseSrvList() ...");

	if (ngamsParseSrvListId("", servers) != ngamsSTAT_SUCCESS)
		return ngamsSTAT_FAILURE;
	ngamsLogDebug("Leaving ngamsParseSrvList()");

	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsRegister(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         path,
 const int           wait,
 ngamsSTATUS*        status)
 Send an REGISTER command to the NG/AMS Server to make it register
 a file or set of files.

 path:      Path starting point from which the scanning for files to
 register will be initiated. Only files with a known mime-type
 is taken into account.

 Returns:   ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsRegister(const char* host, const int port,
		const float timeoutSecs, const char* path, const int wait,
		ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl;

	ngamsLogDebug("Entering ngamsRegister() ...");
	ngamsInitStatus(status);
	sprintf(tmpUrl, "%s?wait=\"%d\"&path=\"%s\"", ngamsCMD_REGISTER_STR, wait,
			path);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsRegister()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsRegister()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsRemDisk(const char*       host,
 const int         port,
 const float       timeoutSecs,
 const char*       diskId,
 const int         execute,
 ngamsSTATUS*      status)
 Send a REMDISK command to the NG/AMS Server. If execute is 0 the
 disk information will not be deleted from the NGAS DB and from the
 the disk itself. Otherwise, if 1 is specified, this information will
 will be deleted.

 diskId:      ID of disk to remove.

 execute:     If set to 1 the command will be executed and the disk
 removed from the system (if possible). Otherwise a report
 will be send back indicating if it is possible to
 remove the disk referenced.

 Returns:     ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsRemDisk(const char* host, const int port,
		const float timeoutSecs, const char* diskId, const int execute,
		ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, tmpEnc;

	ngamsLogDebug("Entering ngamsRemDisk() ...");
	ngamsInitStatus(status);
	ngamsEncodeUrlVal(diskId, 1, tmpEnc);
	sprintf(tmpUrl, "%s?disk_id=\"%s\"&execute=%d", ngamsCMD_REMDISK_STR,
			tmpEnc, execute);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsRemDisk()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsRemDisk()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsRemFile(const char*       host,
 const int         port,
 const float       timeoutSecs,
 const char*       diskId,
 const char*       fileId,
 const int         fileVersion,
 const int         execute,
 ngamsSTATUS*       status)
 Send a REMFILE command to the NG/AMS Server. If execute is 0 the
 disk information will not be deleted from the NGAS DB and from the
 the disk itself. Otherwise, if 1 is specified, this information will
 will be deleted. For the interpretation of various combinations of
 the parameters diskId, fileId, fileVersion and execute consult the
 man-page of the Python module "ngamsRemoveCmd", function remFile().

 diskId:        ID of disk hosting the file(s) to be removed.

 fileId:        ID of file(s) to be removed.

 fileVersion:   Version of file(s) to be removed.

 execute:       If set to 1 the files will be removed (if possible),
 otherwise a report will be send back indicating what
 would be removed if the command is executed.

 Returns:       ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsRemFile(const char* host, const int port,
		const float timeoutSecs, const char* diskId, const char* fileId,
		const int fileVersion, const int execute, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, encFileId, encDiskId;

	ngamsLogDebug("Entering ngamsRemFile() ...");
	ngamsInitStatus(status);
	ngamsEncodeUrlVal(fileId, 1, encFileId);
	ngamsEncodeUrlVal(diskId, 1, encDiskId);
	sprintf(tmpUrl, "%s?disk_id=\"%s\"&file_id=\"%s\"&file_version=%d"
		"&execute=%d", ngamsCMD_REMFILE_STR, encDiskId, encFileId, fileVersion,
			execute);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsRemFile()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsRemFile()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsRetrieve2Mem(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         fileId,
 const int           fileVersion,
 const char*         processing,
 const char*         processingPars,
 const int           internal,
 const char*         hostId,
 ngamsHTTP_DATA*     repDataRef,
 ngamsDATA_LENt*     repDataLen,
 ngamsSTATUS*        status)

 Send a RETRIEVE command to the NG/AMS Server to retrieve a data file, and
 store this in memory.

 fileId:           ID of the file to retrieve.

 fileVersion:      Specific version of file to retrieve. If set to -1 the
 latest version will be retrieved.

 processing:       Name of DPPI to be invoked by NG/AMS when data is
 retrieved.

 processingPars:   Optional list of parameters to hand over to the DPPI.

 internal:         Retrieve an internal file.

 hostId:           ID of host from where to retrieve the file (if needed to
 make this explicit).

 repDataRef:       Structure containing the reference to the data.

 repDataLen:       Length of the data received.

 Returns:          ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsRetrieve2Mem(const char* host, const int port,
		const float timeoutSecs, const char* fileId, const int fileVersion,
		const char* processing, const char* processingPars, const int internal,
		const char* hostId, ngamsHTTP_DATA* repDataRef,
		ngamsDATA_LEN* repDataLen, ngamsSTATUS* status) {
	char tmpBuf[10000];
	int retCode;
	int bytesRd;
	ngamsDATA_LEN bytesRead = 0;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsBIG_BUF tmpUrl;

	/* Perform the query */
	if (strcmp(fileId, ngamsNG_LOG_REF) == 0)
		sprintf(tmpUrl, "%s?ng_log", ngamsCMD_RETRIEVE_STR);
	else if (strcmp(fileId, ngamsCFG_REF) == 0)
		sprintf(tmpUrl, "%s?cfg", ngamsCMD_RETRIEVE_STR);
	else if (internal) {
		ngamsEncodeUrlVal(fileId, 1, tmpBuf);
		sprintf(tmpUrl, "%s?internal=\"%s\"", ngamsCMD_RETRIEVE_STR, tmpBuf);
	} else {
		ngamsEncodeUrlVal(fileId, 1, tmpBuf);
		sprintf(tmpUrl, "%s?file_id=\"%s\"", ngamsCMD_RETRIEVE_STR, tmpBuf);
	}
	if ((processing != NULL) && (*processing != '\0')) {
		ngamsEncodeUrlVal(processing, 1, tmpBuf);
		sprintf(tmpBuf, "&processing=\"%s\"", tmpBuf);
		strcat(tmpUrl, tmpBuf);
		if ((processingPars != NULL) && (*processingPars != '\0')) {
			ngamsEncodeUrlVal(processingPars, 1, tmpBuf);
			sprintf(tmpBuf, "&processing_pars=\"%s\"", tmpBuf);
			strcat(tmpUrl, tmpBuf);
		}
	}

	if (fileVersion != -1) {
		sprintf(tmpBuf, "&file_version=\"%d\"", fileVersion);
		strcat(tmpUrl, tmpBuf);
	}

	if (*hostId != '\0') {
		sprintf(tmpBuf, "&host_id=%s", hostId);
		strcat(tmpUrl, tmpBuf);
	}

	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 0,
			repDataRef, repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;

	/* Query went OK - for the moment no XML status document from RETRIEVE */
	strcpy(status->message, "Successfully handled RETRIEVE command");
	strcpy(status->status, "OK");
	status->errorCode = ngamsSTAT_SUCCESS;

	return ngamsSTAT_SUCCESS;

	errExit:

	if (repDataRef->pdata)
		ngamsUnpackStatus(repDataRef->pdata, status);
	else {
		ngamsStat2Str(retCode, status->message);
		strcpy(status->status, "FAILURE");
		status->errorCode = retCode;
	}

	return retCode;
}

ngamsSTAT _ngamsRetrieve2File(const char* host, const int port,
		const float timeoutSecs, const char* fileRef, const int fileVersion,
		const char* processing, const char* processingPars,
		const char* targetFile, ngamsMED_BUF finalTargetFile,
		ngamsSTATUS* status, const int internal, const char* hostId) {
	char tmpBuf[10000];
	char* tmpP;
	int retCode;
	int fd = 0, bytesRd;
	time_t timeLastRec;
	ssize_t bytes_written;
	ngamsDATA_LEN bytesRead = 0;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsBIG_BUF tmpUrl;
	ngamsMED_BUF tmpTargetFilename, tmpEnc;

	ngamsLogDebug("Entering _ngamsRetrieve2File() ...");
	ngamsInitStatus(status);
	memset(&repDataRef, 0, sizeof(repDataRef));

	/* Perform the query */
	if (strcmp(fileRef, ngamsNG_LOG_REF) == 0)
		sprintf(tmpUrl, "%s?ng_log", ngamsCMD_RETRIEVE_STR);
	else if (strcmp(fileRef, ngamsCFG_REF) == 0)
		sprintf(tmpUrl, "%s?cfg", ngamsCMD_RETRIEVE_STR);
	else if (internal) {
		ngamsEncodeUrlVal(fileRef, 1, tmpEnc);
		sprintf(tmpUrl, "%s?internal=%s", ngamsCMD_RETRIEVE_STR, tmpEnc);
	} else {
		ngamsEncodeUrlVal(fileRef, 1, tmpEnc);
		sprintf(tmpUrl, "%s?file_id=%s", ngamsCMD_RETRIEVE_STR, tmpEnc);
	}
	if ((processing != NULL) && (*processing != '\0')) {
		ngamsEncodeUrlVal(processing, 1, tmpEnc);
		sprintf(tmpBuf, "&processing=%s", tmpEnc);
		strcat(tmpUrl, tmpBuf);
		if ((processingPars != NULL) && (*processingPars != '\0')) {
			ngamsEncodeUrlVal(processingPars, 1, tmpEnc);
			sprintf(tmpBuf, "&processing_pars=%s", tmpEnc);
			strcat(tmpUrl, tmpBuf);
		}
	}

	if (fileVersion != -1) {
		sprintf(tmpBuf, "&file_version=%d", fileVersion);
		strcat(tmpUrl, tmpBuf);
	}

	if (*hostId != '\0') {
		sprintf(tmpBuf, "&host_id=%s", hostId);
		strcat(tmpUrl, tmpBuf);
	}

	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 0,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS) {
		ngamsLogDebug("Error invoking ngamsHttpGet(). Host:port/URL: "
			"%s:%d/%s", host, port, tmpUrl);
		goto errExit;
	}

	/* If the specified filename is a file, store the data into a file
	 * with that name. Othwerwise if a directory is specified, receive
	 * the data into a file in that directory, with the name indicated
	 * in the HTTP header.
	 */
	if (ngamsIsDir(targetFile)) {
		if ((retCode = ngamsGetHttpHdrEntry(httpHdr, "content-disposition",
				"filename", tmpTargetFilename)) != ngamsSTAT_SUCCESS)
			goto errExit;
		strcpy(finalTargetFile, targetFile);
		if (*(finalTargetFile + strlen(finalTargetFile) - 1) != '/')
			strcat(finalTargetFile, "/");
		strcat(finalTargetFile, tmpTargetFilename);
	} else if (*targetFile != '\0')
		strcpy(finalTargetFile, targetFile);
	else {
		if ((retCode = ngamsGetHttpHdrEntry(httpHdr, "content-disposition",
				"filename", tmpTargetFilename)) != ngamsSTAT_SUCCESS)
			goto errExit;
		if ((tmpP = getenv("PWD")) != NULL)
			sprintf(finalTargetFile, "%s/", tmpP);
		else
			*finalTargetFile = '\0';
		strcat(finalTargetFile, tmpTargetFilename);
	}

	/* Receive the data */
	/* -rw-rw-r-- */
	/* int    oflag = (O_CREAT | O_RDWR | O_TRUNC | O_LARGEFILE);*/
	int oflag = (O_CREAT | O_RDWR | O_TRUNC);
	mode_t mode = (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
	if ((fd = open(finalTargetFile, oflag, mode)) == -1) {
		ngamsLogDebug("Error creating target file: %d/%s", errno,
				strerror(errno));
		retCode = ngamsERR_INV_TARG_FILE;
		goto errExit;
	}

	timeLastRec = time(NULL);
	time_t startTime = time(NULL);
	int count = 0;
	while (bytesRead < repDataLen) {

		bytesRd = read(repDataRef.fd, tmpBuf, 10000);
		ngamsLogDebug("Read data block of size: %d. Bytes read: %.9E bytes",
				bytesRd, (double) bytesRead);
		if (bytesRd > 0) {
			bytesRead += bytesRd;
			if (((count % 1000) == 0) && ((_logLevel >= LEV5) || (_verboseLevel
					>= LEV5))) {
				/* Calculate throughput */
				float throughPut = (((bytesRead) / 1048576.) / (time(NULL)
						- startTime));
				ngamsLogDebug("Data received so far: %lu bytes (%.6f MB), "
					"Throughput: %.6f MB/s", bytesRead, (bytesRead / 1048576.),
						throughPut);
			}
			bytes_written = write(fd, tmpBuf, bytesRd);
			if (bytes_written == -1) {
				ngamsLogError("Error while writing data to target file %s: %s", finalTargetFile, strerror(errno));
				retCode = ngamsERR_WR_DATA;
				goto errExit;
			}
			timeLastRec = time(NULL);
			count++;
		} else if( bytesRd == 0 ) {
			retCode = ngamsERR_TIMEOUT;
			goto errExit;
		} else if ( bytesRd == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
			retCode = ngamsERR_TIMEOUT;
			goto errExit;
		} else { /* bytesRd < 0 */
			retCode = ngamsERR_COM;
			goto errExit;
		}
	}
	close(fd);

	/* Query went OK - for the moment no XML status document from RETRIEVE */
	strcpy(status->message, "Successfully handled RETRIEVE command");
	strcpy(status->status, "OK");
	status->errorCode = ngamsSTAT_SUCCESS;

	ngamsLogDebug("Leaving _ngamsRetrieve2File()");
	return ngamsSTAT_SUCCESS;

	errExit:
	if (repDataRef.fd > 0)
		close(repDataRef.fd);


	if (repDataRef.pdata)
		ngamsUnpackStatus(repDataRef.pdata, status);
	else {
		ngamsStat2Str(retCode, status->message);
		strcpy(status->status, "FAILURE");
		status->errorCode = retCode;
	}
	ngamsLogDebug("Leaving _ngamsRetrieve2File()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsRetrieve2File(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         fileId,
 const int           fileVersion,
 const char*         processing,
 const char*         processingPars,
 const char*         targetFile,
 ngamsMED_BUF        finalTargetFile,
 ngamsSTATUS*        status);

 Send a RETRIEVE command to the NG/AMS Server to retrieve a
 data file, and store this in a file on the local disk.

 fileId:           ID of the file to retrieve.

 fileVersion:      Specific version of file to retrieve. If set to -1 the
 latest version will be retrieved.

 processing:       Name of DPPI to be invoked by NG/AMS when data is
 retrieved.

 processingPars:   Optional list of parameters to hand over to the DPPI.

 targetFile:       If a valid filename is specified the data retrieved
 will be stored in a file with that name. If a directory
 is given, the data file retrieved will be stored in that
 directory with the name under which it is stored in
 NGAS. If this parameter is an empty string, it will be
 tried to stored the file retrieved under the NGAS
 archive name in the current working directory.

 finalTargetFile:  The final target filename under which the file retrieved
 was stored.

 Returns:          ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsRetrieve2File(const char* host, const int port,
		const float timeoutSecs, const char* fileId, const int fileVersion,
		const char* processing, const char* processingPars,
		const char* targetFile, ngamsMED_BUF finalTargetFile,
		ngamsSTATUS* status) {
	int stat;

	ngamsLogDebug("Entering ngamsRetrieve2File() ...");
	stat = _ngamsRetrieve2File(host, port, timeoutSecs, fileId, fileVersion,
			processing, processingPars, targetFile, finalTargetFile, status, 0,
			"");
	ngamsLogDebug("Leaving ngamsRetrieve2File()");
	return stat;
}

/**
 ngamsSTAT ngamsGenRetrieve2File(const char*           host,
 const int             port,
 const float           timeoutSecs,
 const ngamsCMD        cmdCode,
 const ngamsPAR_ARRAY* parArray,
 const char*           targetFile,
 ngamsMED_BUF          finalTargetFile,
 ngamsSTATUS*          status)
 See ngamsRetrieve2File().
 */
ngamsSTAT ngamsGenRetrieve2File(const char* host, const int port,
		const float timeoutSecs, const ngamsCMD cmdCode,
		const ngamsPAR_ARRAY* parArray, const char* targetFile,
		ngamsMED_BUF finalTargetFile, ngamsSTATUS* status) {
	char tmpBuf[10000];
	char* tmpP;
	int retCode, i;
	int fd = 0, bytesRd;
	time_t timeLastRec;
	ssize_t bytes_written;
	ngamsDATA_LEN bytesRead = 0;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsBIG_BUF url;
	ngamsMED_BUF tmpTargetFilename, cmd, tmpEnc;

	ngamsLogDebug("Entering ngamsGenRetrieve2File() ...");
	ngamsInitStatus(status);
	memset(&repDataRef, 0, sizeof(repDataRef));
	ngamsCmd2Str(cmdCode, cmd);
	strcpy(url, cmd);
	for (i = 0; i < parArray->idx; i++) {
		if (i == 0) {
			ngamsEncodeUrlVal(parArray->valArray[i], 1, tmpEnc);
			sprintf(tmpBuf, "?%s=\"%s\"", parArray->parArray[i], tmpEnc);
		} else {
			ngamsEncodeUrlVal(parArray->valArray[i], 1, tmpEnc);
			sprintf(tmpBuf, "&%s=\"%s\"", parArray->parArray[i], tmpEnc);
		}
		strcat(url, tmpBuf);
	}
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, url, 0,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;

	/* If the specified filename is a file, store the data into a file with
	 * that name. Othwerwise if a directory is specified, receive the data into
	 * a file in that directory, with the name indicated in the HTTP header.
	 */
	if (ngamsIsDir(targetFile)) {
		if ((retCode = ngamsGetHttpHdrEntry(httpHdr, "content-disposition",
				"filename", tmpTargetFilename)) != ngamsSTAT_SUCCESS)
			goto errExit;
		strcpy(finalTargetFile, targetFile);
		if (*(finalTargetFile + strlen(finalTargetFile) - 1) != '/')
			strcat(finalTargetFile, "/");
		strcat(finalTargetFile, tmpTargetFilename);
	} else if (*targetFile != '\0')
		strcpy(finalTargetFile, targetFile);
	else {
		if ((retCode = ngamsGetHttpHdrEntry(httpHdr, "content-disposition",
				"filename", tmpTargetFilename)) != ngamsSTAT_SUCCESS)
			goto errExit;
		if ((tmpP = getenv("PWD")) != NULL)
			sprintf(finalTargetFile, "%s/", tmpP);
		else
			*finalTargetFile = '\0';
		strcat(finalTargetFile, tmpTargetFilename);
	}

	/* Receive the data */
	if ((fd = creat(finalTargetFile, 0664)) == -1) {
		retCode = ngamsERR_INV_TARG_FILE;
		goto errExit;
	}

	timeLastRec = time(NULL);
	while (bytesRead < repDataLen) {

		bytesRd = read(repDataRef.fd, tmpBuf, 10000);
		if (bytesRd > 0) {
			bytes_written = write(fd, tmpBuf, bytesRd);
			if (bytes_written == -1) {
				ngamsLogError("Error while writing data to target file %s: %s", finalTargetFile, strerror(errno));
				retCode = ngamsERR_WR_DATA;
				goto errExit;
			}
			bytesRead += bytesRd;
			timeLastRec = time(NULL);
		} else if( bytesRd == 0 ) {
			retCode = ngamsERR_TIMEOUT;
			goto errExit;
		} else if ( bytesRd == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
			retCode = ngamsERR_TIMEOUT;
			goto errExit;
		} else { /* bytesRd < 0 */
			retCode = ngamsERR_COM;
			goto errExit;
		}
	}
	close(fd);

	/* Query went OK - for the moment no XML status document from RETRIEVE */
	strcpy(status->message, "Successfully handled RETRIEVE command");
	strcpy(status->status, "OK");
	status->errorCode = ngamsSTAT_SUCCESS;

	if (repDataRef.fd > 0)
		close(repDataRef.fd);

	ngamsLogDebug("Leaving ngamsGenRetrieve2File()");
	return ngamsSTAT_SUCCESS;

	errExit:
	if (repDataRef.pdata)
		ngamsUnpackStatus(repDataRef.pdata, status);
	else {
		ngamsStat2Str(retCode, status->message);
		strcpy(status->status, "FAILURE");
		status->errorCode = retCode;
	}
	ngamsLogDebug("Leaving ngamsGenRetrieve2File()/FAILURE. Status: %d",
			retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsStatus(const char*         host,
 const int           port,
 const float         timeoutSecs,
 ngamsSTATUS*        status)
 Send a STATUS command to NG/AMS to query the current status of the
 system. No parameters are defined at present.

 Returns:      ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsStatus(const char* host, const int port,
		const float timeoutSecs, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl;

	ngamsLogDebug("Entering ngamsStatus() ...");
	ngamsInitStatus(status);
	sprintf(tmpUrl, "%s", ngamsCMD_STATUS_STR);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, tmpUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsStatus()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsStatus()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 ngamsSTAT ngamsSubscribe(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         url,
 const int           priority,
 const char*         startDate,
 const char*         filterPlugIn,
 const char*         filterPlugInPars,
 ngamsSTATUS*        status)
 Send a SUBSCRIBE to NG/AMS to subscribe for data or a specific type of data.

 url:                 Subscriber URL to where data is pushed.

 priority:            Priority of the Subscriber (low number = high
 priority). Default value 10.

 startDate:           Start date defining which data the subscription
 should take into account.

 filterPlugIn:        Optional Filter Plug-In to apply when selecting
 which data files to deliver to the clients.

 filterPlugInPars:    Optional Filter Plug-In Parameters to transfer
 to the Filter Plug-In.

 Returns:             ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsSubscribe(const char* host, const int port,
		const float timeoutSecs, const char* url, const int priority,
		const char* startDate, const char* filterPlugIn,
		const char* filterPlugInPars, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, reqUrl, tmpEnc;

	ngamsLogDebug("Entering ngamsSubscribe() ...");
	ngamsInitStatus(status);
	ngamsEncodeUrlVal(url, 1, tmpEnc);
	sprintf(reqUrl, "%s?url=\"%s\"&priority=%d", ngamsCMD_SUBSCRIBE_STR,
			tmpEnc, priority);
	if ((startDate != NULL) && (*startDate != '\0')) {
		ngamsEncodeUrlVal(startDate, 1, tmpEnc);
		sprintf(tmpUrl, "&start_date=\"%s\"", tmpEnc);
		strcpy(reqUrl, tmpUrl);
	}
	if ((filterPlugIn != NULL) && (*filterPlugIn != '\0')) {
		ngamsEncodeUrlVal(filterPlugIn, 1, tmpEnc);
		sprintf(tmpUrl, "&filter_plug_in=\"%s\"", tmpEnc);
		strcpy(reqUrl, tmpUrl);
	}
	if ((filterPlugInPars != NULL) && (*filterPlugInPars != '\0')) {
		ngamsEncodeUrlVal(filterPlugInPars, 1, tmpEnc);
		sprintf(tmpUrl, "&plug_in_pars=\"%s\"", tmpEnc);
		strcpy(reqUrl, tmpUrl);
	}
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, reqUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsSubscribe()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsSubscribe()/FAILURE. Status: %d", retCode);
	return retCode;

}

/**
 ngamsSTAT ngamsUnsubscribe(const char*         host,
 const int           port,
 const float         timeoutSecs,
 const char*         url,
 ngamsSTATUS*        status)
 Send an UNSUBSCRIBE Command to the given NG/AMS Server.

 Returns:    ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsUnsubscribe(const char* host, const int port,
		const float timeoutSecs, const char* url, ngamsSTATUS* status) {
	int retCode;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF reqUrl, tmpEnc;

	ngamsLogDebug("Entering ngamsUnsubscribe() ...");
	ngamsInitStatus(status);
	ngamsEncodeUrlVal(url, 1, tmpEnc);
	sprintf(reqUrl, "%s?url=\"%s\"", ngamsCMD_UNSUBSCRIBE_STR, tmpEnc);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, reqUrl, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsUnsubscribe()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsUnsubscribe()/FAILURE. Status: %d", retCode);
	return retCode;

}

/**
 *******************************************************************************
 */

/**
 *******************************************************************************
 The following functions are used to handle the HTTP communication.
 These could be moved to a separate module for reusage.
 *******************************************************************************
 */

#define	ngamsMAXLINE    16384
#define	ngamsBUFSIZE    65536

/**
 ngamsSTAT ngamsGetHttpHdrEntry(ngamsHTTP_HDR   httpHdr,
 const char*     hdrName,
 const char*     fieldName,
 ngamsMED_BUF    value)
 Get entry from HTTP headers.

 httpHdr:      Instance of ngamsHTTP_HDR structure containing the HTTP
 headers.

 hdrName:      Name of the HTTP header entry to consider.

 fieldName:    Fieldname within the HTTP header. If this is not relevant,
 this should be specified as '\0'.

 value:        Value of the HTTP header or field.

 Returns:      Execution status (ngamsSTAT_SUCCESS|ngamsSTAT_FAILURE).
 */
ngamsSTAT ngamsGetHttpHdrEntry(ngamsHTTP_HDR httpHdr, const char* hdrName,
		const char* fieldName, ngamsMED_BUF value) {
	char* fieldPtr;
	char* valPtr;
	int found = 0, idx = 0, stat;
	ngamsMED_BUF fieldLoc;

	ngamsLogDebug("Entering ngamsGetHttpHdrEntry() ...");
	sprintf(fieldLoc, "%s=", fieldName);

	/* An HTTP header could be something like:
	 * Content-Disposition: attachment;
	 * filename="ISAAC.1999-04-11T05:03:21.035.fits.Z"
	 */

	/* Find the right HTTP header */
	while (!found && (idx < ngamsHTTP_MAX_HDRS)) {
		if (strstr(httpHdr[idx], hdrName) != NULL)
			found = 1;
		else
			idx++;
	}

	/* Find the field - if fieldName was set to "" we just take
	 * the whole value of the header.
	 */
	if (*fieldName != '\0') {
		if ((fieldPtr = strstr(httpHdr[idx], fieldLoc)) == NULL) {
			stat = ngamsERR_INV_REPLY;
			goto errExit;
		}
		valPtr = (fieldPtr + strlen(fieldLoc));
	} else {
		valPtr = strchr(httpHdr[idx], ':');
		valPtr++;
		while (*valPtr == ' ')
			valPtr++;
	}

	/* Get the value */
	memset(value, 0, sizeof(ngamsMED_BUF));
	if (*valPtr == '"')
		valPtr++;
	while ((*valPtr != '"') && (*valPtr != '\0') && (*valPtr != '\r')
			&& (*valPtr != '\n'))
		strncat(value, valPtr++, sizeof(char));

	ngamsLogDebug("Leaving ngamsGetHttpHdrEntry()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsLogDebug(
			"Leaving ngamsGetHttpHdrEntry()/FAILURE. Status: %d", stat);
	return stat;
}

/**
 int ngamsReadLine(int*	fd,
 char*	ptr,
 int	maxlen)
 Read a line from a HTTP (socket) connection

 fd:        File descriptor for HTTP socket connection.

 ptr:       Pointer to string buffer in which the line will be written.

 maxlen:    Maximum number of bytes to read.

 Returns:   Bytes read.
 */
int ngamsReadLine(int* fd, char* ptr, int maxlen) {
	char c;
	int n, rc;

	for (n = 0; n < maxlen; n++) {
		/* IMPL: If the remote server writes a line, which does not contain
		 *       a newline character, read() blocks. Could maybe be improved
		 *       by probing (using select with timeout defined) before
		 *       trying to read from the socket.
		 */
		if ((rc = read(*fd, &c, 1)) == 1) {
			n++;
			*ptr++ = c;
			if (c == '\n')
				break;
		} else if (rc == 0) {
			break;
		} else
			return rc; /* error */
	}

	*ptr = 0;
	return n;
}

/**
 int ngamsRecvData(char**             data,
 ngamsDATA_LEN      dataLen,
 int*               sockFd)
 Receive data of a given size from a socket connection.

 data:      Pointer pointer to string buffer, which will be allocated by
 this function and which will contain the data read.

 dataLen:   Length of data read.

 sockFd:    Socket file descriptor to read from.

 Returns:   Execution status (ngamsSTAT_SUCCESS|ngamsERR_RD_DATA|
 ngamsERR_ALLOC_MEM).
 */
int ngamsRecvData(char** data, ngamsDATA_LEN dataLen, int* sockFd) {
	int stat = ngamsSTAT_SUCCESS;
	ngamsDATA_LEN dataRem = dataLen;
	ngamsDATA_LEN totalDataRead = 0, dataRead;

	ngamsLogDebug("Entering ngamsRecvData() ...");
	if (!(*data = malloc(dataLen + 1))) {
		stat = ngamsERR_ALLOC_MEM;
		goto errExit;
	}
	while (dataRem > 0) {

		dataRead = read(*sockFd, *data + totalDataRead, dataRem);
		if ( dataRead == 0 ) {
			stat = ngamsERR_RD_DATA;
			goto errExit;
		}
		else if ( dataRead == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
			stat = ngamsERR_TIMEOUT;
			goto errExit;
		}
		else if ( dataRead == -1 ) {
			stat = ngamsERR_CON;
			goto errExit;
		}

		totalDataRead += dataRead;
		dataRem -= dataRead;

	}
	*(*data + totalDataRead) = '\0';
	close(*sockFd);
	if (unregisterSock(*sockFd))
		ngamsLogError("fail to unregister socket(%d) in the list", *sockFd);
	ngamsLogDebug("Socket(%d) closed.", *sockFd);
	*sockFd = 0;

	ngamsLogDebug("Leaving ngamsRecvData(). Status: %d", stat);
	return stat;

	errExit: ngamsLogDebug("Leaving ngamsRecvData()/FAILURE. Status: %d", stat);
	return stat;
}

/* The _ngamsConnect() function (and _connectThread() + _ngamsTHREAD_INFO)
 * are used as a work-around for a problem encountered whereby the connect()
 * socket system calls blocks an arbitrarily long period of time if the
 * specified target host is defined in the DNS domain but is not accessible.
 *
 * The actual connect() is executed in a sub-thread, monitored by the calling
 * thread, which does not block and which can take the necessary actions if
 * connect() blocks.
 */
typedef struct {
	int handleNumber;
	struct sockaddr* servAddr;
	int sockFd;
	int syncPipe[2];
	long unsigned parentThreadId;
	struct timeval reservedTime;
	pthread_mutex_t syncMutex;
	int connectFctLock;
	int connectThrLock;
} _ngamsTHREAD_INFO;

#define MAX_CON_THR_HANDLES 65536
#define THR_HANDLE_TIMEOUT  30
ngamsSTAT _getThreadInfoObj(_ngamsTHREAD_INFO** thrInfo) {
	static _ngamsTHREAD_INFO threadInfoHandles[MAX_CON_THR_HANDLES];
	static int initialized;
	/* Use to control that the clean up is not done at every invocation. */
	static int count;

	/* Begin critial section. */
	_ngamsLockSrvInfoSem();

	if (!initialized) {
		memset(threadInfoHandles, 0,
				(MAX_CON_THR_HANDLES * sizeof(_ngamsTHREAD_INFO)));
		int i;
		for (i = 0; i < MAX_CON_THR_HANDLES; i++) {
			threadInfoHandles[i].handleNumber = (i + 1);
			gettimeofday(&(threadInfoHandles[i].reservedTime), NULL);
		}
		count = 0;
		initialized = 1;
	}

	/* Clean up list, release entries if:
	 *
	 * - connectFctLock and connectThrLock are both 0.
	 * - the object exists for more than the specified timeout.
	 */
	count++;
	if (count == 100) {
		int i;
		for (i = 0; i < MAX_CON_THR_HANDLES; i++) {
			_ngamsTHREAD_INFO* thrHandleP = &(threadInfoHandles[i]);
			struct timeval timeNow;
			gettimeofday(&timeNow, NULL);
			/* Make a forced release of locked handles after a certain
			 timeout. */
			if (((timeNow.tv_sec - thrHandleP->reservedTime.tv_sec)
					> THR_HANDLE_TIMEOUT) && (thrHandleP->connectFctLock
					|| thrHandleP->connectThrLock)) {
				/* Release this entry */
				ngamsLogInfo(1, ">>>>> releasing handle# %d",
						thrHandleP->handleNumber);
				int handleNumber = thrHandleP->handleNumber;
				memset(thrHandleP, 0, sizeof(_ngamsTHREAD_INFO));
				thrHandleP->handleNumber = handleNumber;
				gettimeofday(&(thrHandleP->reservedTime), NULL);
			}
		} /* End: for (i = 0; i < MAX_CON_THR_HANDLES; i++) */
		count = 0;
	} /* End: if (count == 100) */

	/* Find and reserve new handle. */
	int foundThr = 0;
	int i;
	for (i = 0; i < MAX_CON_THR_HANDLES; i++) {
		_ngamsTHREAD_INFO* thrHandleP = &(threadInfoHandles[i]);
		if (!(thrHandleP->connectFctLock) && !(thrHandleP->connectThrLock)) {
			gettimeofday(&(thrHandleP->reservedTime), NULL);
			pthread_mutex_init(&(thrHandleP->syncMutex), NULL);
			thrHandleP->connectFctLock = 1;
			thrHandleP->connectThrLock = 1;
			*thrInfo = thrHandleP;
			foundThr = 1;
			break;
		}
	} /* End: for (i = 0; i < MAX_CON_THR_HANDLES; i++) */

	/* End critial section. */
	_ngamsUnlockSrvInfoSem();

	/* If no free handle found, return error */
	if (foundThr)
		return ngamsSTAT_SUCCESS;
	else
		return ngamsSTAT_FAILURE;
}

void* _connectThread(void* ptr) {
	int stat, connected = 0;
	ssize_t bytes_written;
	_ngamsTHREAD_INFO* thrInfo;

	ngamsLogDebug("Entering _connectThread() ...");
	thrInfo = (_ngamsTHREAD_INFO*) ptr;
	ngamsLogInfo(1,
			">>>>> par thr id: %lu, thr id: %lu: entering _connectThread()",
			thrInfo->parentThreadId, pthread_self());
	int n;
	for (n = 1; n <= 5; n++) {
		ngamsLogInfo(1,
				"_connectThread(): connecting socket %d to server (try %d)",
				thrInfo->sockFd, n);
		stat = connect(thrInfo->sockFd, thrInfo->servAddr,
				sizeof(struct sockaddr_in));
		if (stat >= 0) {
			connected = 1;
			break;
		} else {
			ngamsLogDebug("_connectThread(): Error calling connect(). Parent "
				"Thread ID: %lu Errno(%d):%s", thrInfo->parentThreadId, errno,
					strerror(errno));
			usleep(500);
			continue;
		}
	}
	ngamsLogInfo(
			1,
			">>>>> par thr id: %lu, thr id: %lu: _connectThread(): connected=%d",
			thrInfo->parentThreadId, pthread_self(), connected);
	bytes_written = write(thrInfo->syncPipe[1], &connected, sizeof(int));
	if( bytes_written == -1 ) {
		ngamsLogError("Error while writing to thrInfo->syncPipe[1]: %s", strerror(errno));
		/* We can't do much else, I don't actually know what's exactly going on here... */
	}

	ngamsLogDebug("Leaving _connectThread()");
	//ngamsLogInfo(1, ">>>>> par thr id: %lu, thr id: %lu: _connectThread(): before pthread_mutex_lock()", thrInfo->parentThreadId, pthread_self());
	pthread_mutex_lock(&(thrInfo->syncMutex));
	thrInfo->connectThrLock = 0;
	pthread_mutex_unlock(&(thrInfo->syncMutex));
	//ngamsLogInfo(1, ">>>>> par thr id: %lu, thr id: %lu: _connectThread(): after pthread_mutex_unlock()", thrInfo->parentThreadId, pthread_self());
	pthread_exit(NULL);
	return NULL;
}

ngamsSTAT _ngamsConnect(const int sockFd, const struct sockaddr_in* servAddr) {
	/*
	 int stat, creStat = -1, retStat, connected = 0;
	 pthread_t thrHandle;
	 pthread_attr_t thrAttr;
	 _ngamsTHREAD_INFO* thrInfo;

	 ngamsLogDebug("Entering _ngamsConnect() ...");
	 //	if (_getThreadInfoObj(&thrInfo) == ngamsSTAT_FAILURE) {
	 //		ngamsLogError("Error obtaining connect thread handle");
	 //		retStat = ngamsSTAT_FAILURE;
	 //		goto fctExit;
	 //	}
	 //	ngamsLogInfo(1, ">>>>> got handle# %d", thrInfo->handleNumber);
	 thrInfo->servAddr = (struct sockaddr*)servAddr;
	 thrInfo->sockFd = sockFd;
	 //	if (pipe(thrInfo->syncPipe) != 0) {
	 //		ngamsLogError("Error creating pipe");
	 //		retStat = ngamsSTAT_FAILURE;
	 //		goto fctExit;
	 //	}
	 thrInfo->parentThreadId = pthread_self();
	 pthread_attr_init(&thrAttr);
	 pthread_attr_setdetachstate(&thrAttr, PTHREAD_CREATE_DETACHED);
	 creStat = pthread_create(&thrHandle, &thrAttr, _connectThread, thrInfo);
	 if (creStat != 0) {
	 ngamsLogError("Error creating connect thread");
	 retStat = ngamsSTAT_FAILURE;
	 goto fctExit;
	 }
	 stat = _pollFd(thrInfo->syncPipe[0], -1);
	 if (stat == ngamsSTAT_SUCCESS)
	 read(thrInfo->syncPipe[0], &connected, sizeof(int));
	 if (connected)
	 retStat = ngamsSTAT_SUCCESS;
	 else
	 retStat = ngamsSTAT_FAILURE;
	 goto fctExit;

	 fctExit: if (thrInfo->syncPipe[0]) {
	 close(thrInfo->syncPipe[0]);
	 close(thrInfo->syncPipe[1]);
	 }
	 */
	/*
	 //if (!connected && (creStat == 0)) pthread_cancel(thrHandle);
	 //pthread_mutex_lock(&(thrInfo->syncMutex));
	 ngamsLogInfo(1, ">>>>> _ngamsConnect(): thrInfo->connectThrLock=%d", thrInfo->connectThrLock);
	 if (thrInfo->connectThrLock) {
	 //ngamsLogInfo(1, ">>>>> _ngamsConnect(): before pthread_cancel(thrHandle);");
	 //pthread_cancel(thrHandle);
	 //ngamsLogInfo(1, ">>>>> _ngamsConnect(): after pthread_cancel(thrHandle);");
	 thrInfo->connectThrLock = 0;
	 }
	 //ngamsLogInfo(1, ">>>>> _ngamsConnect(): before pthread_mutex_unlock(&(thrInfo->syncMutex));");
	 pthread_mutex_unlock(&(thrInfo->syncMutex));
	 //ngamsLogInfo(1, ">>>>> _ngamsConnect(): before thread_attr_destroy(&thrAttr);");
	 */

	/*
	 ngamsLogInfo(1, "_ngamsConnect(): connecting socket %d to server (try %d)", sockFd, n);
	 if( connect(sockFd, (struct sockaddr*)servAddr, sizeof(struct sockaddr_in)) >= 0)
	 return ngamsSTAT_SUCCESS;
	 ngamsLogDebug("_ngamsConnect(): Error calling connect(). errno(%d):%s", errno, strerror(errno));
	 return ngamsSTAT_FAILURE;
	 */
	/*
	 pthread_attr_destroy(&thrAttr);
	 thrInfo->connectFctLock = 0;
	 if (retStat == ngamsSTAT_SUCCESS)
	 ngamsLogDebug("Leaving _ngamsConnect()");
	 else
	 ngamsLogDebug("Leaving _ngamsConnect()/FAILURE. Status: %d", retStat);
	 ngamsLogInfo(1, ">>>>> _ngamsConnect(): before return retStat;");
	 return retStat;
	 */
}

/* Mutex for create a socket and connect it to NGAS. */
pthread_mutex_t _socketMutex;
void _ngamsLockSocketGEN() {
	pthread_mutex_lock(&_socketMutex);
}
void _ngamsUnlockSocketGEN() {
	pthread_mutex_unlock(&_socketMutex);
}

/**
 int ngamsPrepSock(const char*     host,
 const int       port)
 Prepare a socket connection.

 host:     Host to which to build up socket connection.

 port:     Port number of listen socket.

 Returns:  Socket file descriptor in case a socket connection was
 successfully created. Otherwise one of the following errors
 might be returned:

 ngamsERR_HOST, ngamsERR_SOCK, ngamsERR_CON
 */
int ngamsPrepSock(const char* host, const int port, float timeout) {
	int sockFd = 0, stat;
	struct hostent* hostRef;
	struct sockaddr_in servAddr;

	ngamsLogDebug("Entering ngamsPrepSock() ...");
	_ngamsLockSocketGEN();

	/* gethostbyname() points to static memory, i.e., is not thread safe */
	h_errno = 0;
	if ((hostRef = gethostbyname(host)) == NULL) {
		ngamsLogDebug("gethostbyname fails. h_errno(%d)", h_errno);
		stat = ngamsERR_HOST;
		goto errExit;
	}
	/* Fill in the structure "servAddr" with the address of the
	 * server that we want to connect with.
	 */
	memset((char *) &servAddr, '\0', sizeof(servAddr));
	memcpy(&servAddr.sin_addr, hostRef->h_addr_list[0], hostRef->h_length);
	servAddr.sin_family = AF_INET;
	servAddr.sin_port = htons(port);

	/* Open a TCP socket (an Internet stream socket).
	 */
	/* JKN: I had to make this loop to make the socket system call return
	 * a socket descriptor >0 in case the hostname given is an IP address?
	 * No idea how this could influence socket(), but retrying to obtain
	 * a socket file descriptor >0 succeeds on the seconds attempt.
	 */
	int count = 1;
	while (1) {
		if ((sockFd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
			stat = ngamsERR_SOCK;
			goto errExit;
		}
		if (sockFd > 0) {
			//sometimes different threads may get the same socket number and cause unexpected result
			//so we need to check if the socket appears before
			if (existedSock(sockFd)) {
				ngamsLogInfo(1, "ngamsPrepSock gets the same socket(%d)",
						sockFd);
				continue;
			}

			/* Set a receive timeout on the socket */
			if( timeout < 0 ) {
				timeout = ngamsDEFAULT_TIME_OUT;
			}
			struct timeval timeout_tv;
			timeout_tv.tv_sec = (time_t)floorf(timeout);
			timeout_tv.tv_usec = (long)((timeout_tv.tv_sec - timeout) * 1000);
			if( setsockopt(sockFd, SOL_SOCKET, SO_RCVTIMEO, (void *)&timeout_tv, sizeof(timeout_tv)) ) {
				perror("Error while setting receiving timeout: ");
				stat = ngamsERR_SOCK;
				goto errExit;
			}

			if (registerSock(sockFd))
				ngamsLogError("fail to register socket(%d) in the list", sockFd);
			break;
		}
		if (++count > 5) {
			stat = ngamsERR_SOCK;
			goto errExit;
		}
	}
	ngamsLogDebug("Socket(%d) opened.", sockFd);

	ngamsLogInfo(1, "connecting socket(%d) to NGAS(%s:%d).", sockFd, host, port);
	if (connect(sockFd, (struct sockaddr*) &servAddr,
			sizeof(struct sockaddr_in)) < 0) {
		ngamsLogDebug(
				"fail to connect socket(%d) to NGAS(%s:%d). errno(%d):%s",
				sockFd, host, port, errno, strerror(errno));
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
		stat = ngamsERR_CON;
		goto errExit;
	}

	ngamsLogDebug("Leaving ngamsPrepSock()");
	_ngamsUnlockSocketGEN();
	return sockFd;

	errExit: ngamsLogDebug("Leaving ngamsPrepSock()/FAILURE. Status: %d", stat);
	_ngamsUnlockSocketGEN();
	return stat;
}

/**
 int ngamsRecvHttpHdr(int*               sockFd,
 ngamsHTTP_HDR      httpHdr,
 ngamsHTTP_RESP*    httpResp,
 ngamsHTTP_DATA*    repDataRef,
 ngamsDATA_LEN*     dataLen)
 Receive HTTP headers from the given HTTP socket file descriptor and
 store them in the instance of the ngamsHTTP_HDR structure.

 sockFd:        Socket file descriptor.

 httpHdr:       Instance of ngamsHTTP_HDR structure.

 httpResp:      Contains the decoded HTTP response, which is of the form:

 "HTTP/1.0 200 OK"

 repDataRef:    In case of an error this is used to retrieve the
 XML status document from the NG/AMS Server. This is
 possibly a minor violation of the HTTP protocol, but
 practicle for displaying a better error message for the
 user. However, this should not make the HTTP protocol
 implemented incompatible with the definition of the HTTP
 protocol.

 dataLen:       Length of the data (XML Status Document) possibly received.

 Returns:       Execution status (ngamsSTAT_SUCCESS | ngamsERR_TIMEOUT |
 ngamsERR_INV_REPLY | ngamsSRV_INV_QUERY).
 */
int ngamsRecvHttpHdr(int* sockFd, ngamsHTTP_HDR httpHdr,
		ngamsHTTP_RESP* httpResp, ngamsHTTP_DATA* repDataRef,
		ngamsDATA_LEN* dataLen) {
	char recvLine[ngamsMAXLINE + 1];
	char* strP1;
	char* strP2;
	char* tmpData;
	char* chr;
	int bytesRead, hdrIdx = 0;
	int retCode = 0;

	ngamsLogDebug("Entering ngamsRecvHttpHdr() ...socket(%d)", *sockFd);

	tmpData = NULL;
	memset(httpResp, 0, sizeof(ngamsHTTP_RESP));
	memset(recvLine, 0, sizeof(ngamsMAXLINE + 1));

	/* Read the HTTP header: ends with a blank line. */
	ngamsLogDebug("ReadLine from socket(%d)", *sockFd);
	while ((bytesRead = ngamsReadLine(sockFd, recvLine, ngamsMAXLINE)) > 0) {
		if (bytesRead > ngamsMAXLINE - 10) {
			ngamsLogDebug("HTTP header is too big(%d).", bytesRead);
			retCode = ngamsERR_COM;
			goto errExit;
		}
		ngamsTrimString(recvLine, "\015\012");
		ngamsLogDebug("Parsing HTTP header(%d): |%s|", strlen(recvLine),
				recvLine);
		if ((bytesRead == 1) || (strstr(recvLine, "\015\012") == recvLine))
			break;
		for ((chr = recvLine); (*chr && (*chr != ':')); chr++)
			*chr = (char) tolower((int) *chr);
		strcpy(httpHdr[hdrIdx], recvLine);
		hdrIdx++;
		if (strstr(recvLine, "content-length:")) {
			long long int tmpLen = atol(&recvLine[15]); /* >>>> */
			*dataLen = tmpLen;
			/* *dataLen = atof(&recvLine[15]); */
			ngamsLogDebug("Extracted Content-Length: %llu bytes", *dataLen);
		}
		/* atoi(&recvLine[15]); */
	}

	/* In case -1 is returned by ngamsReadLine() a problem occurred
	 *  with the communication. */
	if ( bytesRead == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) ) {
		retCode = ngamsERR_TIMEOUT;
		goto errExit;
	} else if ( bytesRead <= 0 ) {
		retCode = ngamsERR_COM;
		goto errExit;
	}

	/* Extract the info from the first line of the HTTP response.
	 * Is something like: "HTTP/1.0 200 OK". */
	strP1 = httpHdr[0];
	strP2 = strchr(strP1, ' ');
	if ((strP2 - strP1) >= sizeof(ngamsSMALL_BUF)) {
		retCode = ngamsERR_INV_REPLY;
		goto errExit;
	}
	strncpy(httpResp->version, strP1, (strP2 - strP1));
	strP1 = (strP2 + 1);
	httpResp->status = atoi(strP1);
	strP2 = strchr(strP1, ' ');
	if (strP2 != NULL) {
		if (*(strP2 + 1) != '\0')
			strcpy(httpResp->message, (strP2 + 1));
	} else
		*(httpResp->message) = '\0';
	ngamsTrimString(httpResp->message, "\015\012");

	/* If the request was invalid, we try to receive possible data
	 * contained in the HTTP reply. */
	if (httpResp->status == ngamsSRV_INV_QUERY) {
		if (ngamsRecvData(&tmpData, *dataLen, sockFd) == ngamsSTAT_SUCCESS) {
			repDataRef->pdata = calloc((*dataLen + 1), sizeof(char));
			memcpy(repDataRef->pdata, tmpData, *dataLen + 1);
			free(tmpData);
			tmpData = NULL;
			ngamsLogDebug("status == ngamsSRV_INV_QUERY");
		}
		retCode = httpResp->status;
		goto errExit;
	} else if (httpResp->status == ngamsSRV_REDIRECT) {
		if (ngamsRecvData(&tmpData, *dataLen, sockFd) == ngamsSTAT_SUCCESS)
			free(tmpData);
		ngamsLogDebug("status == ngamsSRV_REDIRECT");
	}

	ngamsLogDebug("Leaving ngamsRecvHttpHdr()");
	return ngamsSTAT_SUCCESS;

	errExit: if (tmpData)
		free(tmpData);
	ngamsLogDebug("Leaving ngamsRecvHttpHdr()/FAILURE. Status: %d", retCode);
	sleep(5); //server might be too busy. slow down a bit
	return retCode;
}

/**
 int _ngamsHttpGet(const char*         host,
 const int           port,
 const char*         userAgent,
 const char*         path,
 const int           receiveData,
 ngamsHTTP_DATA*     repDataRef,
 ngamsDATA_LEN*      dataLen,
 ngamsHTTP_RESP*     httpResp,
 ngamsHTTP_HDR       httpHdr)
 Execute an HTTP GET.

 See ngamsHttpGet() for parameters and return value.
 */
int _ngamsHttpGet(const char* host, const int port, const float timeout, const char* userAgent,
		const char* path, const int receiveData, ngamsHTTP_DATA* repDataRef,
		ngamsDATA_LEN* dataLen, ngamsHTTP_RESP* httpResp, ngamsHTTP_HDR httpHdr) {
	char* strP;
	int sockFd = 0, n;
	int retCode = 0;
	ngamsMED_BUF redirectUrl, altHost, altPort;
	ngamsHUGE_BUF sendLine, authHdr;

	ngamsLogDebug("Entering _ngamsHttpGet() ...");

	ngamsLogDebug("Submitting request with URL: http://%s:%d/%s", host, port,
			path);

	*dataLen = 0;
	memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
	memset(httpResp, 0, sizeof(ngamsHTTP_RESP));

	if ((sockFd = ngamsPrepSock(host, port, timeout)) < 0) {
		ngamsLogDebug("Error calling ngamsPrepSock(). URL: %s:%d/%s", host,
				port, path);
		retCode = sockFd;
		goto errExit;
	}

	/* Send the GET method */
	if (ngamsGetAuthorization()) {
		memset(authHdr, 0, sizeof(ngamsHUGE_BUF));
		sprintf(authHdr, "\015\012Authorization: Basic%%20%s",
				ngamsGetAuthorization());
	} else
		*authHdr = '\0';
	sprintf(sendLine, "GET %s HTTP/1.0\nUser-Agent: %s%s\015\012\012", path,
			userAgent, authHdr);
	ngamsLogDebug("Submitting HTTP header: %s to host/port: %s/%d", sendLine,
			host, port);
	n = strlen(sendLine);

	// jagonzal: We are using SOCK_STREAM (non-blocking sending mode) for the socket so the write
	//           function (UNIX System API) returns only the actual number of bytes written in the
	//           socket at the return time that are not necessarily the number of bytes in the buffer,
	//           so we have to disable this check.
	// rtobar: The coment above is misleading, since the SOCK_STREAM (a socket type) has nothing to do
	//         with socket I/O operations blocking or not (which is controlled by the O_NONBLOCK flag
	//         set via fcntl(), or the SOCK_NONBLOCK option given at socket() creation time). Thus,
	//         I'm re-enabling the check on the write() call, which is the correct thing to do.
	if (write(sockFd, sendLine, n) != n) {
	      ngamsLogDebug("Error writing on socket. URL: %s:%d/%s", host, port, path);
	      retCode = ngamsERR_WR_HD;
	      goto errExit;
	}

	/* Receive the reply for the request */
	if ((retCode = ngamsRecvHttpHdr(&sockFd, httpHdr, httpResp, repDataRef,
			dataLen)) != ngamsSTAT_SUCCESS) {
		ngamsLogDebug("Error calling ngamsRecvHttpHdr(). URL: %s:%d/%s", host,
				port, path);
		goto errExit;
	}

	/* If an HTTP Redirection Response has been returned, resend the
	 * query to the alternative URL.
	 */
	if (httpResp->status == ngamsSRV_REDIRECT) {
		/* Find the "Location" HTTP header and get the alternative URL.
		 */
		if ((retCode = ngamsGetHttpHdrEntry(httpHdr, "location", "",
				redirectUrl)) != ngamsSTAT_SUCCESS) {
			ngamsLogDebug("Error calling ngamsGetHttpHdrEntry(). "
				"URL: %s:%d/%s", host, port, path);
			goto errExit;
		}

		/* Get out the host + port from the URL, e.g.: http://host:1234/...
		 */
		memset(altHost, 0, sizeof(ngamsMED_BUF));
		memset(altPort, 0, sizeof(ngamsMED_BUF));
		strP = strstr(redirectUrl, "//");
		strP += 2;
		while (*strP != ':')
			strncat(altHost, strP++, 1);
		strP++;
		while (*strP != '/')
			strncat(altPort, strP++, 1);

		/* This function again (recursively).
		 */
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
		sockFd = 0;
		if (repDataRef->pdata)
			free(repDataRef->pdata);
		memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
		return ngamsHttpGet(altHost, atoi(altPort), timeout, userAgent, path,
				receiveData, repDataRef, dataLen, httpResp, httpHdr);
	}

	/* If specified, read the data from the socket and store it into
	 * a buffer of memory allocated.
	 */
	if (receiveData && *dataLen) {
		if ((retCode = ngamsRecvData(&(repDataRef->pdata), *dataLen, &sockFd))
				!= ngamsSTAT_SUCCESS) {
			ngamsLogDebug("Error calling ngamsRecvData(). "
				"URL: %s:%d/%s", host, port, path);
			if (repDataRef->pdata)
				free(repDataRef->pdata);
			memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
			goto errExit;
		}
	} else
		repDataRef->fd = sockFd;

	ngamsLogDebug("Leaving _ngamsHttpGet()");
	return ngamsSTAT_SUCCESS;

	errExit: if (sockFd > 0) {
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
	}
	ngamsLogDebug("Leaving _ngamsHttpGet()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 int ngamsHttpGet(const char*         host,
 const int           port,
 const char*         userAgent,
 const char*         path,
 const int           receiveData,
 ngamsHTTP_DATA*     repDataRef,
 ngamsDATA_LEN*      dataLen,
 ngamsHTTP_RESP*     httpResp,
 ngamsHTTP_HDR       httpHdr)
 Execute an HTTP GET.

 If server multiplexing is used, remember to set the value of the host input
 parameter to '\0'.

 userAgent:     User agent to put in the HTTP query string.

 path:          Path (=URL) for the HTTP GET.

 receiveData:   Indicates if possibly data contained in the HTTP
 response should be received automatically by the
 function (0|1).

 repDataRef:    Pointer to instance of ngamsHTTP_DATA structure containing
 possible response data received.

 dataLen:       Length of possible response data received.

 httpResp:      Instance of ngamsHTTP_RESP structure containing information
 about the HTTP response.

 httpHdr:       Instance of the ngamsHTTP_HDR structure containing the
 HTTP headers received in the HTTP response.

 Returns:       ngamsSTAT_SUCCESS or error code returned by the functions:

 ngamsPrepSock(), ngamsRecvHttpHdr(), ngamsGetHttpHdrEntry(),
 ngamsHttpGet(), ngamsRecvData()

 - or the following error coded: ngamsERR_WR_HD.
 */
int ngamsHttpGet(const char* host, const int port, const float timeout, const char* userAgent,
		const char* path, const int receiveData, ngamsHTTP_DATA* repDataRef,
		ngamsDATA_LEN* dataLen, ngamsHTTP_RESP* httpResp, ngamsHTTP_HDR httpHdr) {
	char* locHost;
	int locPort, maxTries, tryCount = 0, foundList;
	int stat;
	ngamsSRV_INFO* srvInfoP;

	ngamsLogDebug("Entering ngamsHttpGet() ...");

	_ngamsGetSrvInfoObj(host, &foundList, &srvInfoP);
	if (!foundList) {
		stat = _ngamsHttpGet(host, port, timeout, userAgent, path, receiveData,
				repDataRef, dataLen, httpResp, httpHdr);
		ngamsLogDebug("Leaving ngamsHttpGet(). Status: %d", stat);
		return stat;
	} else {
		/* Simple implementation: Just try the next server if the present
		 * one failed. Could be refined.
		 */
		maxTries = srvInfoP->numberOfSrvs;
		int idx = -1;
		while (tryCount < maxTries) {
			_ngamsGetNextSrv(&idx, srvInfoP, &locHost, &locPort);
			stat = _ngamsHttpGet(locHost, locPort, timeout, userAgent, path,
					receiveData, repDataRef, dataLen, httpResp, httpHdr);
			if (stat == ngamsSTAT_SUCCESS)
				break;
			tryCount += 1;
		}
		if (tryCount == maxTries) {
			/* IMPL: Should probably make a summarizing response somehow
			 containing info from all attempts . */
			ngamsLogDebug("Leaving ngamsHttpGet()/FAILURE");
			return ngamsSTAT_FAILURE;
		}
	}

	ngamsLogDebug("Leaving ngamsHttpGet()");
	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsGenSendCmd(const char*            host,
 const int              port,
 const float            timeoutSecs,
 const char*            cmd,
 const ngamsPAR_ARRAY*  parArray,
 ngamsSTATUS*           status)
 Generic function to send a command to the NG/AMS Server specified
 by issuing an HTTP GET request.

 Returns:       ngamsSTAT_SUCCESS or one of the error codes returned
 by the function ngamsHttpGet().
 */
ngamsSTAT ngamsGenSendCmd(const char* host, const int port,
		const float timeoutSecs, const char* cmd,
		const ngamsPAR_ARRAY* parArray, ngamsSTATUS* status) {
	int retCode, i;
	ngamsDATA_LEN repDataLen;
	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;
	ngamsMED_BUF tmpUrl, tmpPar, tmpVal;
	ngamsHUGE_BUF url, urlRaw;

	ngamsLogDebug("Entering ngamsGenSendCmd() ...");

	ngamsInitStatus(status);

	/* Prepare the URL */
	strcpy(url, cmd);
	strcpy(urlRaw, cmd);

	for (i = 0; i < parArray->idx; i++) {
		ngamsEncodeUrlVal(parArray->parArray[i], 1, tmpPar);
		ngamsEncodeUrlVal(parArray->valArray[i], 1, tmpVal);
		sprintf(tmpUrl, "%s=%s", tmpPar, tmpVal);

		if (i)
			strcat(url, "&");
		else
			strcat(url, "?");
		strcat(url, tmpUrl);
	}
	ngamsLogInfo(LEV4, "Issuing command with URL: %s ...", urlRaw);
	if ((retCode = ngamsHttpGet(host, port, timeoutSecs, ngamsUSER_AGENT, url, 1,
			&repDataRef, &repDataLen, &httpResp, httpHdr)) != ngamsSTAT_SUCCESS)
		goto errExit;
	ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);

	ngamsLogDebug("Leaving ngamsGenSendCmd()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsHandleStatus(retCode, timeoutSecs, &repDataRef, status);
	ngamsLogDebug("Leaving ngamsGenSendCmd()/FAILURE. Status: %d", retCode);
	return retCode;
}

/* JKN/2008-02-18: CHECK */
/* Small function to get the size of the file. stat() cannot deal with
 * GB sized files, an EOVERFLOW error is returned.
 * -- a dirty workaround, but maybe one day stat() will support file sizes
 * supported by the kernel ...
 */
/*
 long int _ngamsGetFileSize(const char*  filename)
 {
 FILE*              fo = NULL;
 ngamsDATA_LEN      size;
 ngamsBIG_BUF       cmd;

 sprintf(cmd, "ls -l %s", filename);
 if ((fo = popen(cmd, "r")) == NULL)
 goto errExit;
 ngamsHUGE_BUF result;
 memset(result, 0, sizeof(ngamsHUGE_BUF));
 int resultSize;
 if ((resultSize = fread(result, sizeof(ngamsHUGE_BUF), 1, fo)) == -1)
 goto errExit;
 ngamsMED_BUF  subStr[32];
 int           noOfSubStr;
 if (ngamsSplitString(result, " ", 32, subStr, &noOfSubStr) == -1)
 goto errExit;
 size = (ngamsDATA_LEN)(atof(subStr[4]));
 fclose(fo);

 return size;

 errExit:
 if (fo) fclose(fo);
 return -1;
 }
 */

/**
 int _ngamsHttpPost(const char*              host,
 const int                port,
 const char*              userAgent,
 const char*              path,
 const char*              mimeType,
 const char*              contentDisp,
 const char*              srcFilename,
 const char*              data,
 const ngamsDATA_LEN      dataLen,
 ngamsHTTP_DATA*          repDataRef,
 ngamsDATA_LEN*           repDataLen,
 ngamsHTTP_RESP*          httpResp,
 ngamsHTTP_HDR            httpHdr)
 Execute an HTTP POST and send possible data referred to in the body
 of the HTTP request.

 See man-page for ngamsHttpPost() for a description of the parameters.
 */
int _ngamsHttpPost(const char* host, const int port, const float timeout, const char* userAgent,
		const char* path, const char* mimeType, const char* contentDisp,
		const char* srcFilename, const char* data, const int dataLen,
		ngamsHTTP_DATA* repDataRef, ngamsDATA_LEN* repDataLen,
		ngamsHTTP_RESP* httpResp, ngamsHTTP_HDR httpHdr) {


	char header[ngamsBUFSIZE];//, inBuf[io_size];
	int sockFd = 0, fileFd = 0;
	int retCode = 0;
	int bytesRead, hdrLen;
	struct stat statBuf;
	ngamsBIG_BUF authHdr;
	ngamsDATA_LEN contLen = 0;

	ngamsLogDebug("Entering _ngamsHttpPost() ...");

	memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
	*repDataLen = 0;

	/* Figure out the length of the data to send */
	if (*srcFilename != '\0') {
		if (stat(srcFilename, &statBuf) == -1) {
			ngamsLogDebug("Error calling stat() on file: %s. Error: %s",
					srcFilename, strerror(errno));
		} else {
			contLen = statBuf.st_size;
			ngamsLogDebug("Size of file to send: %s, is: %llu bytes",
					srcFilename, contLen);
		}
	} else if (dataLen >= 0)
		contLen = dataLen;

	/* Open the socket connection to the server */
	if ((sockFd = ngamsPrepSock(host, port, timeout)) < 0) {
		retCode = sockFd;
		goto errExit;
	}

	int def_sndbuf = 10240;
	socklen_t optlen = sizeof def_sndbuf;
	getsockopt(sockFd, SOL_SOCKET, SO_SNDBUF, &def_sndbuf, &optlen);
	ngamsLogDebug("Default TCP buffer size: %d\n", def_sndbuf);

	int sndbuf_size = def_sndbuf;
	if (setsndbuf != 0) {
		sndbuf_size = setsndbuf;
		if (sndbuf_size <= 0) {
			sndbuf_size = def_sndbuf;
		} else {
			int rcvbuf_size = sndbuf_size;
			int tmp = sndbuf_size;
			setsockopt(sockFd, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(int)); //set receiver buffer as well
			setsockopt(sockFd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, sizeof(int));
			optlen = sizeof sndbuf_size;
			getsockopt(sockFd, SOL_SOCKET, SO_SNDBUF, &sndbuf_size, &optlen);
			printf("Set buffer size to %d, requested for %d", sndbuf_size, tmp);
		}
	}

	/* Prepare and send the HTTP headers */
	if (ngamsGetAuthorization()) {
		memset(authHdr, 0, sizeof(ngamsBIG_BUF));
		sprintf(authHdr, "\015\012Authorization: Basic %s",
				ngamsGetAuthorization());
	} else
		*authHdr = '\0';
	sprintf(header, "POST /%.256s HTTP/1.0\015\012"
		"User-agent: %s\015\012"
		"Content-Type: %s\015\012"
		"Content-Length: %llu\015\012"
		"Content-Disposition: %s%s\015\012\012", path, ngamsUSER_AGENT,
			mimeType, contLen, contentDisp, authHdr);
	hdrLen = strlen(header);

	/* Send the data if any */
	if (contLen) {
		ngamsLogDebug("Finish sending header. Try to send data ...");
		if (*srcFilename != '\0') {
			/* JKN/2008-02-18: CHECK */
			/*if ((fileFd=open(srcFilename, (O_RDONLY | O_LARGEFILE))) == -1)*/
			if ((fileFd = open(srcFilename, O_RDONLY)) == -1) {
				ngamsLogDebug("Error opening file: %s of size: %lu, for "
					"transmission to archive system", srcFilename, contLen);
				retCode = ngamsERR_FILE;
				goto errExit;
			}
			int gap = 0;
			char* inBuf = (char *) malloc(sndbuf_size * sizeof(char));
			if (hdrLen > sndbuf_size) {
				//If the HTTP header is longer then buffer, send the header on its own
				if (write(sockFd, header, hdrLen) != hdrLen) {
					retCode = ngamsERR_WR_HD;
					if (inBuf != NULL) {
						free(inBuf);
					}
					goto errExit;
				}
			} else {
				gap = hdrLen;
				memcpy(inBuf, header, gap);
			}
			while ((bytesRead = read(fileFd, inBuf + gap, sndbuf_size - gap)) > 0) {
				void *p = inBuf;
				// reliable write
				while (bytesRead > 0) {
					ssize_t bytes_written = write(sockFd, p, bytesRead + gap);
					if (bytes_written <= 0) {
						ngamsLogError("Error while sending data to NGAS server: %s", strerror(errno));
						goto readResp;
					}
					bytesRead -= bytes_written;
					p += bytes_written;
				}
				gap = 0;
			}
			if (bytesRead < 0) {
				ngamsLogDebug(
						"Error reading file(%s) while sending data to socket(%d). errno(%d):%s",
						srcFilename, sockFd, errno, strerror(errno));
				//usleep(20);
				//bytesRead = read(fileFd, inBuf, 10240);
				ngamsLogDebug("Try again on reading file(%s), bytes:(%d)",
						srcFilename, bytesRead);
				retCode = ngamsERR_FILE;
				if (inBuf != NULL) {
					free(inBuf);
				}
				goto errExit;
			}

			if (fileFd > 0) {
				close(fileFd);
			}
			if (inBuf != NULL) {
				free(inBuf);
			}

		} else {
			//send header first
			if (write(sockFd, header, hdrLen) != hdrLen) {
				retCode = ngamsERR_WR_HD;
				goto errExit;
			}
			if (write(sockFd, data, contLen) != contLen) {
				retCode = ngamsERR_WR_DATA;
				goto errExit;
			}
		}
	} else {
		//send header only
		if (write(sockFd, header, hdrLen) != hdrLen) {
			retCode = ngamsERR_WR_HD;
			goto errExit;
		}
	}

readResp:
	ngamsLogDebug(
			"Finish sending data. Try to get reply's header from server...");
	/* Receive the reply for the request */
	if ((retCode = ngamsRecvHttpHdr(&sockFd, httpHdr, httpResp, repDataRef,
			repDataLen)) != ngamsSTAT_SUCCESS)
		goto errExit;

	/* If specified, read the data from the socket and store it into
	 * a buffer of memory allocated.
	 */
	ngamsLogDebug("Finish sending data. Try to get reply's data from server...");
	if (repDataLen) {
		if ((retCode
				= ngamsRecvData(&(repDataRef->pdata), *repDataLen, &sockFd))
				!= ngamsSTAT_SUCCESS) {
			if (repDataRef->pdata)
				free(repDataRef->pdata);
			memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
			goto errExit;
		}
	}
	if (sockFd > 0) {
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
	}
	ngamsLogDebug("Leaving _ngamsHttpPost()");
	return ngamsSTAT_SUCCESS;

	errExit: if (sockFd > 0) {
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
	}
	if (fileFd > 0)
		close(fileFd);
	ngamsLogDebug("Leaving _ngamsHttpPost()/FAILURE. Status: %d", retCode);
	return retCode;
}

/**
 int ngamsHttpPost(const char*              host,
 const int                port,
 const char*              userAgent,
 const char*              path,
 const char*              mimeType,
 const char*              contentDisp,
 const char*              srcFilename,
 const char*              data,
 const ngamsDATA_LEN      dataLen,
 ngamsHTTP_DATA*          repDataRef,
 ngamsDATA_LEN*           repDataLen,
 ngamsHTTP_RESP*          httpResp,
 ngamsHTTP_HDR            httpHdr)
 Execute an HTTP POST and send possible data referred to in the body
 of the HTTP request.

 userAgent:     User agent to put in the HTTP query string.

 path:          Path (=URL) for the HTTP GET.

 mimeType:      Mime-type of the possible data contained in the body of
 the HTTP request.

 contentDisp:   Value of Content-Disposition HTTP header.

 srcFilename:   If different from '\0', the data contained in this file
 will be send in the body of the HTTP request.

 data:          String buffer containing data to be send in the body of
 the HTTP request. This is mutual exclusive with possible
 data given in the file referred to by srcFilename.

 dataLen:       Length of possible data given in string buffer.

 repDataRef:    Pointer to instance of ngamsHTTP_DATA structure containing
 possible response data received.

 repDataLen:    Length of possible response data received.

 httpResp:      Instance of ngamsHTTP_RESP structure containing information
 about the HTTP response.

 httpHdr:       Instance of the ngamsHTTP_HDR structure containing the
 HTTP headers received in the HTTP response.

 Returns:       ngamsSTAT_SUCCESS or error code returned by the functions:

 ngamsPrepSock(), ngamsRecvHttpHdr(), ngamsGetHttpHdrEntry(),
 ngamsRecvData()

 - or one of the following error codes:

 ngamsERR_FILE, ngamsERR_WR_HD, ngamsERR_WR_DATA.
 */
int ngamsHttpPost(const char* host, const int port, const float timeout, const char* userAgent,
		const char* path, const char* mimeType, const char* contentDisp,
		const char* srcFilename, const char* data, const ngamsDATA_LEN dataLen,
		ngamsHTTP_DATA* repDataRef, ngamsDATA_LEN* repDataLen,
		ngamsHTTP_RESP* httpResp, ngamsHTTP_HDR httpHdr) {
	char* locHost;
	int locPort, maxTries, tryCount = 0, foundList;
	int stat;
	ngamsSRV_INFO* srvInfoP;

	ngamsLogDebug("Entering ngamsHttpPost() ...");

	_ngamsGetSrvInfoObj(host, &foundList, &srvInfoP);
	if (!foundList) {
		stat = _ngamsHttpPost(host, port, timeout, userAgent, path, mimeType,
				contentDisp, srcFilename, data, dataLen, repDataRef,
				repDataLen, httpResp, httpHdr);
		ngamsLogDebug("Leaving ngamsHttpPost(). Status: %d", stat);
		if (stat != ngamsSTAT_SUCCESS)
			goto errExit;
	} else {
		/* Simple implementation: Just try the next server if the present
		 * one failed. Could be refined.
		 */
		maxTries = srvInfoP->numberOfSrvs;
		int idx = -1;
		while (tryCount < maxTries) {
			_ngamsGetNextSrv(&idx, srvInfoP, &locHost, &locPort);
			stat = _ngamsHttpPost(locHost, locPort, timeout, userAgent, path, mimeType,
					contentDisp, srcFilename, data, dataLen, repDataRef,
					repDataLen, httpResp, httpHdr);
			if (stat == ngamsSTAT_SUCCESS)
				break;
			tryCount += 1;
		}
		if (tryCount == maxTries) {
			/* IMPL: Should probably make a summarizing response somehow
			 containing info from all attempts . */
			stat = ngamsSTAT_FAILURE;
			goto errExit;
		}
	}

	ngamsLogDebug("Leaving ngamsHttpPost()");
	return ngamsSTAT_SUCCESS;

	errExit: ngamsLogDebug("Leaving ngamsHttpPost()/FAILURE. Status: %d", stat);
	return stat;
}

/**
 int ngamsHttpPostOpen(const char*              host,
 const int                port,
 const char*              userAgent,
 const char*              path,
 const char*              mimeType,
 const char*              contentDisp,
 const ngamsDATA_LEN      dataLen)
 Open an HTTP POST session and send the header of the HTTP request.

 userAgent:     User agent to put in the HTTP query string.

 path:          Path (=URL) for the HTTP POST.

 mimeType:      Mime-type of the possible data contained in the body of
 the HTTP request.

 contentDisp:   Value of Content-Disposition HTTP header.

 srcFilename:   If different from '\0', the data contained in this file
 will be send in the body of the HTTP request.

 data:          String buffer containing data to be send in the body of
 the HTTP request. This is mutual exclusive with possible
 data given in the file referred to by srcFilename.

 dataLen:       Length of possible data given in string buffer.

 Returns:       socket file descriptor or error code returned by the functions:

 ngamsPrepSock(), ngamsRecvHttpHdr(), ngamsGetHttpHdrEntry(),
 ngamsRecvData()

 - one of the following error codes:

 ngamsERR_WR_HD
 */
int ngamsHttpPostOpen(const char* host, const int port, const float timeout, const char* userAgent,
		const char* path, const char* mimeType, const char* contentDisp,
		const ngamsDATA_LEN dataLen) {
	char header[ngamsBUFSIZE];
	int sockFd = 0;
	int retCode = 0;
	int hdrLen;
	ngamsBIG_BUF authHdr;
	ngamsDATA_LEN contLen = 0;

	if (dataLen >= 0)
		contLen = dataLen;

	/* Open the socket connection to the server */
	if ((sockFd = ngamsPrepSock(host, port, timeout)) < 0) {
		retCode = sockFd;
		goto errExit;
	}

	/* Prepare and send the HTTP headers */
	if (ngamsGetAuthorization()) {
		memset(authHdr, 0, sizeof(ngamsBIG_BUF));
		sprintf(authHdr, "\015\012Authorization: Basic %s",
				ngamsGetAuthorization());
	} else
		*authHdr = '\0';
	sprintf(header, "POST /%.256s HTTP/1.0\015\012"
		"User-agent: %s\015\012"
		"Content-Type: %s\015\012"
		"Content-Length: %lld\015\012"
		"Content-Disposition: %s%s\015\012\012", path, ngamsUSER_AGENT,
			mimeType, contLen, contentDisp, authHdr);
	hdrLen = strlen(header);
	if (write(sockFd, header, hdrLen) != hdrLen) {
		retCode = ngamsERR_WR_HD;
		goto errExit;
	}

	return sockFd;

	errExit: if (sockFd > 0) {
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
	}
	return retCode;
}

/**
 int ngamsHttpPostSend(const int             sockFd,
 const char*           srcFilename,
 const char*           data,
 const ngamsDATA_LEN   dataLen)
 Send data to an HTTP stream socket opened by ngamsHttpPostOpen. This
 function can be called multiple times and the connection should be
 closed afterwards using ngamsHttpPostClose.

 sockFd:        Socket file descriptor as returned by ngamsHttpPostOpen.

 srcFilename:   If different from '\0', the data contained in this file
 will be send in the body of the HTTP request.

 data:          String buffer containing data to be send in the body of
 the HTTP request. This is mutual exclusive with possible
 data given in the file referred to by srcFilename.

 Returns:       ngamsSTAT_SUCCESS
 or ngamsERR_FILE, ngamsERR_WR_DATA
 */
int ngamsHttpPostSend(int sockFd, const char* srcFilename, const char* data,
		const ngamsDATA_LEN dataLen) {
	char inBuf[1024];
	int fileFd = 0;
	int retCode = 0;
	int foo = sockFd;
	struct stat statBuf;
	ngamsDATA_LEN contLen = 0;
	ngamsDATA_LEN bytesRead;

	/* Figure out the length of the data to send */
	if (*srcFilename != '\0') {
		if (stat(srcFilename, &statBuf) == -1) {
			retCode = ngamsERR_FILE;
			goto errExit;
		}
		contLen = statBuf.st_size;
	} else
		contLen = dataLen;

	/* Send the data if any */
	if (contLen) {
		if (*srcFilename != '\0') {
			if ((fileFd = open(srcFilename, O_RDONLY)) == -1) {
				retCode = ngamsERR_FILE;
				goto errExit;
			}
			while ((bytesRead = read(fileFd, inBuf, 1024)) > 0) {
				if (write(foo, inBuf, bytesRead) != bytesRead) {
					retCode = ngamsERR_WR_DATA;
					goto errExit;
				}
			}
			if (bytesRead < 0) {
				retCode = ngamsERR_FILE;
				goto errExit;
			}
			if (fileFd > 0)
				close(fileFd);
		} else {
			if (write(foo, data, contLen) != contLen) {
				retCode = ngamsERR_WR_DATA;
				goto errExit;
			}
		}
	}

	return ngamsSTAT_SUCCESS;

	errExit: if (fileFd > 0)
		close(fileFd);
	if ((foo > 0) && (retCode == ngamsERR_WR_DATA)) {
		close(foo);
		if (unregisterSock(foo))
			ngamsLogError("fail to unregister socket(%d) in the list", foo);
		ngamsLogDebug("Socket(%d) closed.", foo);
	}
	return retCode;
}

/**
 int ngamsHttpPostClose(const int           sockFd,
 ngamsHTTP_DATA*     repDataRef,
 ngamsDATA_LEN*      repDataLen,
 ngamsHTTP_RESP*     httpResp,
 ngamsHTTP_HDR       httpHdr)
 Retrieve possible return data from an HTTP connection opened by
 ngamsHttpPostOpen and close the socket afterwards.

 sockFd:        Socket file-descriptor opened by ngamsHttpPostOpen.

 repDataRef:    Pointer to instance of ngamsHTTP_DATA structure containing
 possible response data received.

 repDataLen:    Length of possible response data received.

 httpResp:      Instance of ngamsHTTP_RESP structure containing information
 about the HTTP response.

 httpHdr:       Instance of the ngamsHTTP_HDR structure containing the
 HTTP headers received in the HTTP response.

 Returns:       ngamsSTAT_SUCCESS or error code returned by the functions:

 ngamsRecvHttpHdr(), ngamsGetHttpHdrEntry(),
 ngamsRecvData()
 */
int ngamsHttpPostClose(int sockFd, ngamsHTTP_DATA* repDataRef,
		ngamsDATA_LEN* repDataLen, ngamsHTTP_RESP* httpResp,
		ngamsHTTP_HDR httpHdr) {
	int retCode;

	memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
	*repDataLen = 0;

	/* Receive the reply for the request */
	if ((retCode = ngamsRecvHttpHdr(&sockFd, httpHdr, httpResp, repDataRef,
			repDataLen)) != ngamsSTAT_SUCCESS)
		goto errExit;

	/* If specified, read the data from the socket and store it into
	 * a buffer of memory allocated.
	 */
	if (repDataLen) {
		if ((retCode
				= ngamsRecvData(&(repDataRef->pdata), *repDataLen, &sockFd))
				!= ngamsSTAT_SUCCESS) {
			if (repDataRef->pdata)
				free(repDataRef->pdata);
			memset(repDataRef, 0, sizeof(ngamsHTTP_DATA));
			goto errExit;
		}
	}
	if (sockFd > 0) {
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
	}

	return ngamsSTAT_SUCCESS;

	errExit: if (sockFd > 0) {
		close(sockFd);
		if (unregisterSock(sockFd))
			ngamsLogError("fail to unregister socket(%d) in the list", sockFd);
		ngamsLogDebug("Socket(%d) closed.", sockFd);
	}
	return retCode;
}

/**
 *******************************************************************************
 */

/**
 *******************************************************************************
 Functions to Handle Logging.
 *******************************************************************************
 */

/* Log Write information. */
pthread_mutex_t _logMutex;
void _ngamsLockLogWR() {
	pthread_mutex_lock(&_logMutex);
}
void _ngamsUnlockLogWR() {
	pthread_mutex_unlock(&_logMutex);
}

/**
 void ngamsLog_v(const char*            type,
 const ngamsLOG_LEVEL   level,
 const char*            format,
 va_list                vaParList)
 Generic log function taken a variable list of input parameters.

 type:        Type of log.

 level:       Log Level (intensity, [0; 5]).

 format:      Format of log string in C format style.

 vaParList:   Compiled list of variable input parameters. This must be
 generated with the system call va_start(). After this function
 returns, this must be freed with va_end() to free allocated
 memory.

 Returns:     Void.
 */
void ngamsLog_v(const char* type, const ngamsLOG_LEVEL level,
		const char* format, va_list vaParList) {
	int fd = 0;
	ssize_t bytes_written;
	ngamsHUGE_BUF logMsg, tmpLogMsg;
	ngamsMED_BUF isoTime;
	ngamsSTAT stat;

	/* Create log line */
	ngamsGenIsoTime(3, isoTime);
	while (strlen(isoTime) != 23)
		ngamsGenIsoTime(3, isoTime);
	vsprintf((char*) tmpLogMsg, format, vaParList);
	memset(logMsg, 0, sizeof(ngamsHUGE_BUF));
	sprintf(logMsg, "%s [%s] %s [%lu]\n", isoTime, type, tmpLogMsg,
			pthread_self());

	_ngamsLockLogWR();

	/* Log on stdout */
	if (level <= _verboseLevel)
		fputs(logMsg, stdout);

	/* Log in log file */
	if (level <= _logLevel) {
		/* IMPL: Support buffering to avoid opening and closing the file all the time */
		if ((fd = open(_logFile, O_WRONLY | O_CREAT, 0644)) == -1) {
			stat = ngamsERR_OPEN_LOG_FILE;
			goto errExit;
		}
		lseek(fd, 0, SEEK_END);
		bytes_written = write(fd, logMsg, strlen(logMsg));
		if (bytes_written == -1) {
			ngamsLogError("Error while writing log line to %s: %s", _logFile, strerror(errno));
		}
		if (fd > 0)
			close(fd);
	}

	errExit:
	_ngamsUnlockLogWR();
}

/**
 ngamsSTAT ngamsPrepLog(const ngamsMED_BUF   logFile,
 const ngamsLOG_LEVEL logLevel,
 const int            logRotate,
 const int            logHistory)
 Set up the logging environment for the NG/AMS C-Client functions.

 logFile:         Name of log file.

 logLevel:        Log level (intensity, [0; 5]).

 logRotate:       Seconds since midnight when the log file should be rotated.

 logHistory:      Number of log files (=days) to keep.

 Returns:         ngamsSTAT_SUCCESS or ngamsERR_OPEN_LOG_FILE.
 */
ngamsSTAT ngamsPrepLog(const ngamsMED_BUF logFile,
		const ngamsLOG_LEVEL logLevel, const int logRotate,
		const int logHistory) {
	int fd;

	strcpy(_logFile, logFile);
	_logLevel = logLevel;
	_logRotate = logRotate;
	_logHistory = logHistory;

	/* Create the log file, RW for owner, R for group/others */
	if ((fd = open(logFile, (O_RDWR | O_CREAT), ngamsSTD_PERMS)) == -1)
		return ngamsERR_OPEN_LOG_FILE;
	close(fd);
	return ngamsSTAT_SUCCESS;
}

/**
 void ngamsSetVerboseLevel(const ngamsLOG_LEVEL level)
 Set the Verbose Level to be used. Verbose Logs are written to stdout.

 level:    Verbose Level (intensity, [0; 5]).

 Returns:  Void.
 */
void ngamsSetVerboseLevel(const ngamsLOG_LEVEL level) {
	_verboseLevel = level;
}

/**
 void ngamsInitLogLevel()
 Set the Verbose Level to the value of the environment variable NGAMS_LOG_LEVEL
 (if defined).

 Returns:  Void.
 */
void ngamsInitLogConds() {
	static int initialized;
	char* charP;

	if (!initialized) {
		if ((charP = getenv(ngamsLOG_FILE_ENV)) != NULL)
			strcpy(_logFile, charP);
		if ((charP = getenv(ngamsLOG_LEVEL_ENV)) != NULL)
			_logLevel = atoi(charP);
		if ((charP = getenv(ngamsLOG_VERBOSE_ENV)) != NULL)
			ngamsSetVerboseLevel(atoi(charP));
		initialized = 1;
	}
}

/**
 void ngamsLogEmerg(const char*  format, ...)
 Generate an Emergency Log entry (EMERG).

 format:   Format of log entry.

 ...:      Values to be written in log format.

 Returns:  Void.
 */
void ngamsLogEmerg(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("EMERG", LEV0, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogAlert(const char*  format, ...)
 Generate an Alert Log entry (ALERT).

 See ngamsLogEmerg().
 */
void ngamsLogAlert(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("ALERT", LEV0, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogCrit(const char*  format, ...)
 Generate a Critical Log entry (CRIT).

 See ngamsLogEmerg().
 */
void ngamsLogCrit(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("CRIT", LEV0, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogError(const char*  format, ...)
 Generate an Error Log entry (ERROR).

 See ngamsLogEmerg().
 */
void ngamsLogError(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("ERROR", LEV1, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogWarning(const char*  format, ...)
 Generate a Warning Log entry (WARNING).

 See ngamsLogEmerg().
 */
void ngamsLogWarning(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("WARNING", LEV2, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogNotice(const char*  format, ...)
 Generate a Notice Log entry (NOTICE).

 See ngamsLogEmerg().
 */
void ngamsLogNotice(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("NOTICE", LEV3, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogInfo(const char*  format, ...)
 Generate an Info Log entry (INFO).

 See ngamsLogEmerg().
 */
void ngamsLogInfo(const ngamsLOG_LEVEL level, const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	va_start(vaParList, format);
	ngamsLog_v("INFO", level, format, vaParList);
	va_end(vaParList);
}

/**
 void ngamsLogDebug(const char*  format, ...)
 Generate an Debug Log entry (DEBUG).

 See ngamsLogEmerg().
 */
void ngamsLogDebug(const char* format, ...) {
	va_list vaParList;

	ngamsInitLogConds();
	if ((_logLevel >= LEV5) || (_verboseLevel >= LEV5)) {
		va_start(vaParList, format);
		ngamsLog_v("DEBUG", LEV5, format, vaParList);
		va_end(vaParList);
	}
}

/**
 ngamsSTAT ngamsLogFileRotate(const ngamsLOG_LEVEL  tmpLogLevel,
 const ngamsMED_BUF    systemId,
 ngamsMED_BUF          rotatedLogFile)
 Check if the log file should rotated and rotate it if needed.

 tmpLogLevel:     Temporary Log Level to apply during the process of
 rotating the log file.

 systemId:        ID of the system.

 rotatedLogFile:  Name of the rotated log file.

 Returns:         ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsLogFileRotate(const ngamsLOG_LEVEL tmpLogLevel,
		const ngamsMED_BUF systemId, ngamsMED_BUF rotatedLogFile) {
	char* chrP;
	struct timeval timeEpoch;
	struct tm* timeDay;
	unsigned long int secsOfDay, secsLastRot;
	ngamsMED_BUF path, logFilename, tmpLogBuf;

	if (_logRotate == -1)
		return ngamsSTAT_SUCCESS;
	*rotatedLogFile = '\0';

	gettimeofday(&timeEpoch, NULL);
	timeDay = localtime((time_t*) &timeEpoch.tv_sec);
	secsOfDay = ((3600 * timeDay->tm_hour) + (60 * timeDay->tm_min)
			+ timeDay->tm_sec);

	/* Get time for last rotatation and determine from this if we should
	 * rotate now.
	 */
	ngamsLoadFile(_logFile, tmpLogBuf, sizeof(ngamsMED_BUF));
	if ((chrP = strstr(tmpLogBuf, ngamsLOG_ROT_PREFIX)) == NULL)
		secsLastRot = 0;
	else {
		/* The log entry has a format like, e.g.:
		 *
		 * 2003-12-24T08:36:18.133 [INFO] LOG-ROTATE: 1072254978 \
		 * - SYSTEM-ID: <ID>
		 */
		sscanf((chrP + strlen(ngamsLOG_ROT_PREFIX) + 2), "%ld", &secsLastRot);
	}

	if (((timeEpoch.tv_sec - secsLastRot) > (24 * 3600)) && (secsOfDay
			>= _logRotate)) {
		ngamsLogInfo(tmpLogLevel, "Rotating log file ...");
		ngamsSplitFilename(_logFile, path, logFilename);
		sprintf(rotatedLogFile, "%s/%s_%ld_%s", path, ngamsLOG_ROT_PREFIX,
				(long int) timeEpoch.tv_sec, logFilename);
		if (rename(_logFile, rotatedLogFile) == -1) {
			ngamsLogError("Serious error ocurred rotating log file: %s "
				"- rotated log file: %s!", _logFile, rotatedLogFile);
			return ngamsSTAT_FAILURE;
		}
		ngamsPrepLog(_logFile, _logLevel, _logRotate, _logHistory);
		ngamsLogInfo(tmpLogLevel, "%s: %ld - SYSTEM-ID: %s",
				ngamsLOG_ROT_PREFIX, timeEpoch.tv_sec, systemId);
	}

	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsCleanUpRotLogFiles(const ngamsLOG_LEVEL  tmpLogLevel)
 Check if there are rotated log files to remove and remove them if this is
 the case.

 tmpLogLevel:    Temporary Log Level to apply during the remove process.

 Returns:        ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsCleanUpRotLogFiles(const ngamsLOG_LEVEL tmpLogLevel) {
	DIR* dirPtr = NULL;
	struct dirent* dirEnt = NULL;
	struct timeval timeEpoch;
	unsigned long int secsLastRot, daysSinceLastRot;
	ngamsMED_BUF tmpLogFilename, logFilePath, logFilename;

	if (_logHistory == -1)
		return ngamsSTAT_SUCCESS;

	ngamsSplitFilename(_logFile, logFilePath, logFilename);
	if ((dirPtr = opendir(logFilePath)) == NULL) {
		ngamsLogError("Error opening Log Files Directory: %s", logFilePath);
		goto errExit;
	}
	while ((dirEnt = readdir(dirPtr)) != NULL) {
		if (strstr(dirEnt->d_name, ngamsLOG_ROT_PREFIX) != NULL) {
			sprintf(tmpLogFilename, "%s/%s", logFilePath, dirEnt->d_name);

			/* The format of the names of the rotated log files is, e.g.:
			 *
			 * LOG_ROTATE_<Secs/Epoch Rotation>_ngamsArchiveClient.log
			 *
			 * Get the seconds since epoch for when the log file was rotated.
			 */
			sscanf((dirEnt->d_name + strlen(ngamsLOG_ROT_PREFIX) + 1), "%ld",
					&secsLastRot);
			gettimeofday(&timeEpoch, NULL);
			daysSinceLastRot = ((timeEpoch.tv_sec - secsLastRot) / (3600 * 24));
			if (daysSinceLastRot >= _logHistory) {
				ngamsLogInfo(tmpLogLevel, "Removing rotated log file: %s",
						tmpLogFilename);
				remove(tmpLogFilename);
			}
		}
	}
	closedir(dirPtr);

	return ngamsSTAT_SUCCESS;

	errExit: if (dirPtr != NULL)
		closedir(dirPtr);
	return ngamsSTAT_FAILURE;
}

/**
 *******************************************************************************
 */

/**
 *******************************************************************************
 Various Utility Functions.
 *******************************************************************************
 */

/**
 void _ngamsSetAuthorization(const char*  authUserPass)
 Function to set autorization user/password, which will subsequently be
 sent with all requests automatically if defined.

 authUserPass:   Encrypted user/password. Should be provided by the responsible
 of the NG/AMS Server side (encrypted).

 Returns:        Void.
 */
void ngamsSetAuthorization(const char* authUserPass) {
	_authorization = malloc(strlen(authUserPass) + 1);
	strcpy(_authorization, authUserPass);
}

/**
 void _ngamsGetAuthorization(void)
 Function to retrieve the autorization user/password defined in the module.

 Returns:        Pointer to authorization user/password (encrypted).
 */
char* ngamsGetAuthorization(void) {
	return _authorization;
}

/**
 void ngamsAddParAndVal(ngamsPAR_ARRAY*  parArray,
 const char*      par,
 const char*      val)
 Add a parameter and a corresponding value in the instance of the
 ngamsPAR_ARRAY structure.

 parArray:    Instance of the ngamsPAR_ARRAY structure.

 par:         Name of parameter.

 val:         Value in connection with paameter. Can be '\0'.

 Returns:     Void.
 */
void ngamsAddParAndVal(ngamsPAR_ARRAY* parArray, const char* par,
		const char* val) {
	strcpy(parArray->parArray[parArray->idx], par);
	strcpy(parArray->valArray[parArray->idx], val);
	parArray->idx++;
}

/**
 ngamsSTAT ngamsCmd2No(const ngamsSMALL_BUF     cmdStr,
 ngamsCMD*                cmdCode)
 Convert a command given as string into the corresponding code (integer).

 cmdStr:    Command name as string.

 cmdCode:   Command code as defined by the enumerated type ngamsCMD.

 Returns:   Command code or ngamsERR_UNKNOWN_CMD.
 */
ngamsSTAT ngamsCmd2No(const ngamsSMALL_BUF cmdStr, ngamsCMD* cmdCode) {
	if (strcmp(cmdStr, ngamsCMD_ARCHIVE_STR) == 0)
		*cmdCode = ngamsCMD_ARCHIVE;
	else if (strcmp(cmdStr, ngamsCMD_QARCHIVE_STR) == 0)
		*cmdCode = ngamsCMD_QARCHIVE;
	else if (strcmp(cmdStr, ngamsCMD_CHECKFILE_STR) == 0)
		*cmdCode = ngamsCMD_CHECKFILE;
	else if (strcmp(cmdStr, ngamsCMD_PARCHIVE_STR) == 0)
		*cmdCode = ngamsCMD_PARCHIVE;
	else if (strcmp(cmdStr, ngamsCMD_CLONE_STR) == 0)
		*cmdCode = ngamsCMD_CLONE;
	else if (strcmp(cmdStr, ngamsCMD_DISCARD_STR) == 0)
		*cmdCode = ngamsCMD_DISCARD;
	else if (strcmp(cmdStr, ngamsCMD_EXIT_STR) == 0)
		*cmdCode = ngamsCMD_EXIT;
	else if (strcmp(cmdStr, ngamsCMD_INIT_STR) == 0)
		*cmdCode = ngamsCMD_INIT;
	else if (strcmp(cmdStr, ngamsCMD_LABEL_STR) == 0)
		*cmdCode = ngamsCMD_LABEL;
	else if (strcmp(cmdStr, ngamsCMD_ONLINE_STR) == 0)
		*cmdCode = ngamsCMD_ONLINE;
	else if (strcmp(cmdStr, ngamsCMD_OFFLINE_STR) == 0)
		*cmdCode = ngamsCMD_OFFLINE;
	else if (strcmp(cmdStr, ngamsCMD_REGISTER_STR) == 0)
		*cmdCode = ngamsCMD_REGISTER;
	else if (strcmp(cmdStr, ngamsCMD_REMDISK_STR) == 0)
		*cmdCode = ngamsCMD_REMDISK;
	else if (strcmp(cmdStr, ngamsCMD_REMFILE_STR) == 0)
		*cmdCode = ngamsCMD_REMFILE;
	else if (strcmp(cmdStr, ngamsCMD_RETRIEVE_STR) == 0)
		*cmdCode = ngamsCMD_RETRIEVE;
	else if (strcmp(cmdStr, ngamsCMD_STATUS_STR) == 0)
		*cmdCode = ngamsCMD_STATUS;
	else if (strcmp(cmdStr, ngamsCMD_SUBSCRIBE_STR) == 0)
		*cmdCode = ngamsCMD_SUBSCRIBE;
	else if (strcmp(cmdStr, ngamsCMD_UNSUBSCRIBE_STR) == 0)
		*cmdCode = ngamsCMD_UNSUBSCRIBE;
	else {
		*cmdCode = 0;
		return ngamsERR_UNKNOWN_CMD;
	}

	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsCmd2Str(const ngamsCMD    cmdCode,
 ngamsSMALL_BUF    cmdStr)
 Convert an NG/AMS command given as a code (integer) to a string.

 cmdCode,
 cmdStr:    See ngamsCmd2No().

 Returns:   Command as string.
 */
ngamsSTAT ngamsCmd2Str(const ngamsCMD cmdCode, ngamsSMALL_BUF cmdStr) {
	switch (cmdCode) {
	case ngamsCMD_ARCHIVE:
		strcpy(cmdStr, ngamsCMD_ARCHIVE_STR);
		break;
	case ngamsCMD_QARCHIVE:
		strcpy(cmdStr, ngamsCMD_QARCHIVE_STR);
		break;
	case ngamsCMD_PARCHIVE:
		strcpy(cmdStr, ngamsCMD_PARCHIVE_STR);
		break;
	case ngamsCMD_CHECKFILE:
		strcpy(cmdStr, ngamsCMD_CHECKFILE_STR);
		break;
	case ngamsCMD_CLONE:
		strcpy(cmdStr, ngamsCMD_CLONE_STR);
		break;
	case ngamsCMD_DISCARD:
		strcpy(cmdStr, ngamsCMD_DISCARD_STR);
		break;
	case ngamsCMD_EXIT:
		strcpy(cmdStr, ngamsCMD_EXIT_STR);
		break;
	case ngamsCMD_INIT:
		strcpy(cmdStr, ngamsCMD_INIT_STR);
		break;
	case ngamsCMD_LABEL:
		strcpy(cmdStr, ngamsCMD_LABEL_STR);
		break;
	case ngamsCMD_ONLINE:
		strcpy(cmdStr, ngamsCMD_ONLINE_STR);
		break;
	case ngamsCMD_OFFLINE:
		strcpy(cmdStr, ngamsCMD_OFFLINE_STR);
		break;
	case ngamsCMD_REGISTER:
		strcpy(cmdStr, ngamsCMD_REGISTER_STR);
		break;
	case ngamsCMD_REMDISK:
		strcpy(cmdStr, ngamsCMD_REMDISK_STR);
		break;
	case ngamsCMD_REMFILE:
		strcpy(cmdStr, ngamsCMD_REMFILE_STR);
		break;
	case ngamsCMD_RETRIEVE:
		strcpy(cmdStr, ngamsCMD_RETRIEVE_STR);
		break;
	case ngamsCMD_STATUS:
		strcpy(cmdStr, ngamsCMD_STATUS_STR);
		break;
	case ngamsCMD_SUBSCRIBE:
		strcpy(cmdStr, ngamsCMD_SUBSCRIBE_STR);
		break;
	case ngamsCMD_UNSUBSCRIBE:
		strcpy(cmdStr, ngamsCMD_UNSUBSCRIBE_STR);
		break;
	default:
		*cmdStr = '\0';
		return ngamsERR_UNKNOWN_CMD;
		break;
	} /* end-switch (cmdCode) */

	return ngamsSTAT_SUCCESS;
}

/**
 void ngamsDumpStatStdout(const ngamsSTATUS*  status)
 Dump information contained in the instance of the ngamsSTATUS structure
 to stdout.

 Returns:     Void.
 */
void ngamsDumpStatStdout(const ngamsSTATUS* status) {
	if (*status->date != '\0')
		printf("\nDate:           %s", status->date);
	printf("\nError Code:     %d", status->errorCode);
	if (*status->hostId != '\0')
		printf("\nHost ID:        %s", status->hostId);
	if (*status->message != '\0')
		printf("\nMessage:        %s", status->message);
	if (*status->status != '\0')
		printf("\nStatus:         %s", status->status);
	if (*status->state != '\0')
		printf("\nState:          %s", status->state);
	if (*status->subState != '\0')
		printf("\nSub-State:      %s", status->subState);
	if (*status->version != '\0')
		printf("\nNG/AMS Version: %s", status->version);
	printf("\n\n");
}

/**
 char* ngamsEncodeUrlVal(const char* urlVal,
 const int   skipScheme)
 Encode the value given as input parameter to replace special
 characters to make the value suitable for usage in a URL.

 urlVal:            Value to be encoded.

 skipScheme:        If the value is initiated with an HTTP scheme
 (ftp:, http:, file:), this will not be encoded
 if this flag is set to 1.

 encodedUrl:        The encoded URL. It is the responsibility of the client
 application to allocate a buffer, big enough to hold the
 encoded value. Note, this may be considerably longer than
 the un-encoded value.

 Returns:           Void.
 */
void ngamsEncodeUrlVal(const char* urlVal, const int skipScheme,
		char* encodedUrl) {
	char* strP;
	int idx = 0;

	*encodedUrl = '\0';
	if (skipScheme) {
		if ((strstr(urlVal, "http:") == urlVal) || (strstr(urlVal, "file:")
				== urlVal))
			idx = 5;
		else if (strstr(urlVal, "ftp:") == urlVal)
			idx = 4;
	}

	strP = (char*) (urlVal + idx);
	strncpy(encodedUrl, urlVal, idx);
	*(encodedUrl + idx) = '\0';
	while (*strP != '\0') {
		if (*strP == ':')
			strcat(encodedUrl, "%3A");
		else if (*strP == '?')
			strcat(encodedUrl, "%3F");
		else if (*strP == '=')
			strcat(encodedUrl, "%3D");
		else if (*strP == '&')
			strcat(encodedUrl, "%26");
		else if (*strP == '*')
			strcat(encodedUrl, "%2A");
		else if (*strP == '\'')
			strcat(encodedUrl, "%27");
		else if (*strP == '"')
			strcat(encodedUrl, "%22");
		else if (*strP == '%')
			strcat(encodedUrl, "%25");
		else if (*strP == '+')
			strcat(encodedUrl, "%2B");
		else if (*strP == '\\')
			strcat(encodedUrl, "%2F");
		else if (*strP == '(')
			strcat(encodedUrl, "%28");
		else if (*strP == ')')
			strcat(encodedUrl, "%29");
		else if (*strP == ' ')
			strcat(encodedUrl, "%20");
		else
			strncat(encodedUrl, strP, 1);
		strP++;
	}
}

/**
 void ngamsFreeStatus(ngamsSTATUS*         status)

 Free the memory occupied by the status object.

 status:     Pointer to instance of the ngamsSTATUS structure.

 Returns:    Void.
 */
void ngamsFreeStatus(ngamsSTATUS* status) {
	int i = 0;
	while (status->replyData[i] != NULL) {
		free(status->replyData[i]);
		status->replyData[i] = NULL;
	}
}

/**
 void ngamsGenIsoTime(const int      prec,
 ngamsMED_BUF   dayTime)
 Generate an ISO8601 time string of the format:

 <YYYY>-<MM><DD>T<HH>:<MM>:<SS>[.<ss>]

 The time stamp will be generated at the time of invocation of the function.

 prec:        Precision (number of decimals).

 dayTime:     Generated ISO8601 time stamp.

 Returns:     Void.
 */
void ngamsGenIsoTime(const int prec, ngamsMED_BUF dayTime) {
	char day[16], month[16], date[16], timeStr[16], year[16];
	char date2[16];
	struct timeval timeEpoch;
	ngamsMED_BUF format;
	struct tm* timeNow;
	ngamsMED_BUF tmpBuf, tmpBuf2;
	int intVal;

	gettimeofday(&timeEpoch, NULL);
	timeNow = gmtime((time_t*) &timeEpoch.tv_sec);
	strcpy(tmpBuf, asctime(timeNow));

	/* Now, reformat:
	 *
	 * "Sun Sep 16 01:03:52 1973\n"
	 *
	 * into:
	 *
	 * "1973-09-16T01:03:52"
	 */
	sscanf(tmpBuf, "%s %s %s %s %s", day, month, date, timeStr, year);

	/* Convert Month as string to number */
	if (strstr(month, "Jan") != NULL)
		sprintf(month, "01");
	else if (strstr(month, "Feb") != NULL)
		sprintf(month, "02");
	else if (strstr(month, "Mar") != NULL)
		sprintf(month, "03");
	else if (strstr(month, "Apr") != NULL)
		sprintf(month, "04");
	else if (strstr(month, "May") != NULL)
		sprintf(month, "05");
	else if (strstr(month, "Jun") != NULL)
		sprintf(month, "06");
	else if (strstr(month, "Jul") != NULL)
		sprintf(month, "07");
	else if (strstr(month, "Aug") != NULL)
		sprintf(month, "08");
	else if (strstr(month, "Sep") != NULL)
		sprintf(month, "09");
	else if (strstr(month, "Oct") != NULL)
		sprintf(month, "10");
	else if (strstr(month, "Nov") != NULL)
		sprintf(month, "11");
	else
		sprintf(month, "12");

	/* If date is less than 10 put a 0 in front */
	intVal = atoi(date);
	if (intVal < 10)
		sprintf(date2, "0%d", intVal);
	else
		sprintf(date2, "%d", intVal);

	/* Remove possible newline character in the year */
	if (*(year + strlen(year) - 1) == '\n')
		*(year + strlen(year) - 1) = '\0';

	/* Print the DAYTIM: "1973-09-16T01:03:52[.SSS]" */
	sprintf(dayTime, "%s-%s-%sT%s", year, month, date2, timeStr);

	/* Add fractions of seconds if specified */
	if (prec) {
		sprintf(format, "%%.%df", prec);
		sprintf(tmpBuf, format, (timeEpoch.tv_usec / 1e6));
		strcpy(tmpBuf2, (tmpBuf + 1));
		strcat(dayTime, tmpBuf2);
	}
}

/**
 ngamsSTAT ngamsGetHostName(ngamsSMALL_BUF hostName)
 Return the host name of the local host.

 hostName:    Buffer which will contain the hostname.

 Returns:     ngamsSTAT_SUCCESS.
 */
ngamsSTAT ngamsGetHostName(ngamsSMALL_BUF hostName) {
	char* chrP;

	if ((chrP = getenv("HOSTNAME")) == NULL)
		if ((chrP = getenv("HOST")) == NULL)
			return ngamsSTAT_FAILURE;
	strcpy(hostName, chrP);

	return ngamsSTAT_SUCCESS;
}

/**
 char* ngamsGetParVal(ngamsPAR_ARRAY*  parArray,
 const char*      par)
 Return the value associated to a parameter. If the parameter is not defined,
 NULL is returned.

 parArray:    Instance of the ngamsPAR_ARRAY structure.

 par:         Name of parameter.

 val:         Value in connection with paameter. Can be '\0'.

 Returns:     Void.
 */
char* ngamsGetParVal(ngamsPAR_ARRAY* parArray, const char* par) {
	char *val = NULL;
	int idx = 0;

	while (idx < parArray->idx) {
		if (strcmp(par, parArray->parArray[idx]) == 0) {
			val = parArray->valArray[idx];
			break;
		}
		idx++;
	}

	return val;
}

/**
 ngamsSTAT ngamsGetXmlAttr(const char*    xmlDoc,
 const char*    pt,
 const char*    attr,
 const int      maxValLen,
 char*          value)
 Get the value of an XML attribute contained in an NG/AMS XML Status Document.

 xmlDoc:       Buffer containing the NT/AMS XML Status Document.

 pt:           Name of point in which the attribute is contained.

 attr:         Name of the attribute to retrieve the value for.

 maxValLen:    Maximum length of the attribute value to read.

 value:        Buffer allocated by the calling function containing the
 value of the attribute.

 Returns:      The status code: ngamsSTAT_SUCCESS or ngamsERR_INV_REPLY.
 */
ngamsSTAT ngamsGetXmlAttr(const char* xmlDoc, const char* pt, const char* attr,
		const int maxValLen, char* value) {
	char* ptP;
	char* attrP;
	char* valP;
	int idx = 0;
	ngamsSMALL_BUF tmpPt, tmpAttr;

	sprintf(tmpPt, "<%s", pt);
	sprintf(tmpAttr, "%s=", attr);

	/* Find first the element then the attribute */
	if ((ptP = strstr(xmlDoc, tmpPt)) == NULL)
		return ngamsERR_INV_REPLY;
	ptP += strlen(tmpPt);
	if ((attrP = strstr(ptP, tmpAttr)) == NULL)
		return ngamsERR_INV_REPLY;

	/* Find the start of the attribute value. Expect something like:
	 * <Status Date="2001-08-30T16:56:01.101" HostId="arcus2" ...
	 */
	valP = attrP;
	while ((*valP != '\0') && (*valP != '"'))
		valP++;
	if (*valP == '\0')
		return ngamsERR_INV_REPLY;
	valP++;

	/* Get out the value */
	while ((idx < maxValLen) && (*valP != '\0') && (*valP != '"')) {
		value[idx++] = *valP;
		valP++;
	}
	if ((*valP == '\0') || (idx == maxValLen))
		return ngamsERR_INV_REPLY;
	value[idx] = '\0';

	return ngamsSTAT_SUCCESS;
}

/**
 void ngamsInitStatus(ngamsSTATUS*         status)

 Initialize the ngamsSTATUS structure, making it ready for usage within
 the NG/AMS C-API functions.

 status:   Pointing to instance of the ngamsSTATUS structure containing
 the information to print out.

 Returns:  Void.
 */
void ngamsInitStatus(ngamsSTATUS* status) {
	memset(status, 0, sizeof(ngamsSTATUS));
}

/**
 int ngamsIsDir(const char*  filename)

 Return 1 if the filename given is a directory, otherwise 0 is returned.

 filename:    Name of directory to probe.

 Returns:     1 = directory, 0 = not a directory.
 */
int ngamsIsDir(const char* filename) {
	struct stat statBuf;

	if (stat(filename, &statBuf) == -1)
		return 0;
	if ((statBuf.st_mode & S_IFMT) == S_IFDIR)
		return 1;
	return 0;
}

/**
 char* ngamsLicense(void)

 Return pointer to buffer containing the NG/AMS License Agreement.

 Returns:   Pointer to static buffer containing the license agreement
 for NG/AMS.
 */
char* ngamsLicense(void) {
	return _ngamsLicense();
}

/**
 int ngamsLogCodeInStatus(const ngamsSTATUS*     status,
 const char*            errId)
 Probe if the given log/status code is contained in the ngamsSTAT object.

 errId:     ID of the error, e.g. NGAMS_ER_DAPI_BAD_FILE.

 Returns:   1 if specified log code is contained in the status object.
 */
int ngamsLogCodeInStatus(const ngamsSTATUS* status, const char* errId) {
	ngamsSMALL_BUF codeBuf;
	sprintf(codeBuf, "%s:", errId);
	if (strstr(status->message, codeBuf) != NULL)
		return 1;
	else
		return 0;
}

/**
 ngamsSTAT ngamsLoadFile(const char*    filename,
 char*          buf,
 const int      maxSize)
 Load the given file into the buffer allocated by the calling function.

 filename:      Name of file to load.

 buf:           Buffer in which the contents of the file will be written.

 maxSize:       Maximum size to read.

 Returns:       ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsLoadFile(const char* filename, char* buf, const int maxSize) {
	int fd, size;

	ngamsLogInfo(LEV4, "Opening/loading file: %s ...", filename);
	if ((fd = open(filename, O_RDONLY, 0)) == -1) {
		ngamsLogError("Error ocurred opening file: %s", filename);
		return ngamsSTAT_FAILURE;
	}
	size = read(fd, buf, maxSize);
	buf[size] = '\0';
	close(fd);
	ngamsLogInfo(LEV4, "Opened/loaded file: %s", filename);

	return ngamsSTAT_SUCCESS;
}

/**
 char* ngamsManPage(void)
 Return reference to the man-page of the NG/AMS C-Client.

 Returns:    Pointer to static buffer containing the man-page
 for the NG/AMS C-Client.
 */
char* ngamsManPage(void) {
	return _ngamsManPage();
}

/**
 void ngamsResetParArray(ngamsPAR_ARRAY*  parArray)
 Initialize the instance of the ngamsPAR_ARRAY structure.

 parArray:  Instance of the ngamsPAR_ARRAY structure.

 Returns:   Void.
 */
void ngamsResetParArray(ngamsPAR_ARRAY* parArray) {
	memset(parArray, 0, sizeof(ngamsPAR_ARRAY));
}

/**
 ngamsSTAT ngamsSaveInFile(const char*  filename,
 const char*  buf)
 Save the contents of the buffer in the given file.

 filename:   Name of file in which to store the contents of buffer.

 buf:        Buffer containing the information to be stored in the file.

 Returns:    ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsSaveInFile(const char* filename, const char* buf) {
	int fd;
	ssize_t bytes_written;

	if ((fd = open(filename, (O_WRONLY | O_CREAT), ngamsSTD_PERMS)) == -1) {
		ngamsLogError("Error ocurred creating file: %s", filename);
		return ngamsSTAT_FAILURE;
	}
	bytes_written = write(fd, buf, strlen(buf));
	if (bytes_written == -1) {
		ngamsLogError("Error while writing data to %s: %s", filename, strerror(errno));
		return ngamsSTAT_FAILURE;
	}
	close(fd);

	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsSplitFilename(const char*     complPath,
 ngamsMED_BUF    path,
 ngamsMED_BUF    filename)
 Split a filename is path and actual filename.

 complPath:   Complete path of filename.

 path:        Path of filename.

 filename:    Filename.

 Returns:     ngamsSTAT_SUCCESS.
 */
ngamsSTAT ngamsSplitFilename(const char* complPath, ngamsMED_BUF path,
		ngamsMED_BUF filename) {
	char* chrP;

	memset(path, 0, sizeof(ngamsMED_BUF));
	memset(filename, 0, sizeof(ngamsMED_BUF));
	chrP = ngamsStrRStr(complPath, "/");
	strncpy(path, complPath, (chrP - complPath));
	strcpy(filename, (chrP + 1));

	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsSplitSrvAddr(const char*  srvAddr,
 char*        host,
 int*         port)
 Split a server address given as 'node:port' into its components.

 srvAddr:   Server address.

 host:      Host name of the address.

 port:      Port number of the address.

 Returns:   ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsSplitSrvAddr(const char* srvAddr, char* host, int* port) {
	char* chrPtr;

	if ((chrPtr = strchr(srvAddr, ':')) == NULL)
		return ngamsSTAT_FAILURE;
	strncpy(host, srvAddr, (chrPtr - srvAddr));
	*(host + (chrPtr - srvAddr)) = '\0';
	*port = atoi(chrPtr + 1);
	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsSplitParVal(const char*  parVal,
 char*        par,
 char*        val)
 Split a server address given as 'node:port' into its components.

 parVal:     Parameter value set (<Par>=<Val>).

 par:        Parameter name.

 val:        Value.

 Returns:   ngamsSTAT_SUCCESS or ngamsSTAT_FAILURE.
 */
ngamsSTAT ngamsSplitParVal(const char* parVal, char* par, char* val) {
	char* chrPtr;

	if ((chrPtr = strchr(parVal, '=')) == NULL)
		return ngamsSTAT_FAILURE;
	strncpy(par, parVal, (chrPtr - parVal));
	*(par + (chrPtr - parVal)) = '\0';
	strcpy(val, (chrPtr + 1));
	return ngamsSTAT_SUCCESS;
}

/**
 ngamsSTAT ngamsStat2Str(const ngamsSTAT  statNo,
 ngamsMED_BUF     statStr)

 Convert a status code (ngamsSTAT) to a readable string.

 statNo:    Status as code.

 statStr:   Status as string.

 Returns:   ngamsSTAT_SUCCESS or ngamsERR_UNKNOWN_STAT.
 */
ngamsSTAT ngamsStat2Str(const ngamsSTAT statNo, ngamsMED_BUF statStr) {
	switch (statNo) {
	case ngamsSTAT_SUCCESS:
		strcpy(statStr, "Status OK");
		break;
	case ngamsERR_UNKNOWN_CMD:
		strcpy(statStr, "Unknown command issued");
		break;
	case ngamsERR_INV_TARG_FILE:
		strcpy(statStr, "Invalid target filename specified");
		break;
	case ngamsERR_UNKNOWN_STAT:
		strcpy(statStr, "Unknown status code");
		break;
	case ngamsERR_INV_PARS:
		strcpy(statStr, "Illegal parameters given");
		break;
	case ngamsERR_HOST:
		strcpy(statStr, "No such host");
		break;
	case ngamsERR_SOCK:
		strcpy(statStr, "Cannot create socket");
		break;
	case ngamsERR_CON:
		strcpy(statStr, "Cannot connect to host/server");
		break;
	case ngamsERR_COM:
		strcpy(statStr, "Problem communicating with server");
		break;
	case ngamsERR_TIMEOUT:
		strcpy(statStr, "Timeout encountered while communicating with "
			"server");
		break;
	case ngamsERR_WR_HD:
		strcpy(statStr, "Write error on socket while writing header");
		break;
	case ngamsERR_WR_DATA:
		strcpy(statStr, "Write error on socket while writing data");
		break;
	case ngamsERR_INV_REPLY:
		strcpy(statStr, "Invalid reply from data server");
		break;
	case ngamsERR_ALLOC_MEM:
		strcpy(statStr, "Cannot allocate memory");
		break;
	case ngamsERR_RD_DATA:
		strcpy(statStr, "Read error while reading data");
		break;
	case ngamsERR_FILE:
		strcpy(statStr, "Invalid filename specified");
		break;
	case ngamsERR_OPEN_LOG_FILE:
		strcpy(statStr, "Could not open specified log file");
		break;
	case ngamsSRV_OK:
		strcpy(statStr, "Request sucessfully handled by server");
		break;
	case ngamsSRV_INV_QUERY:
		strcpy(statStr, "Invalid query");
		break;
	default:
		return ngamsERR_UNKNOWN_STAT;
		break;
	} /* end-switch (statNo) */
	return ngamsSTAT_SUCCESS;
}

/**
 int ngamsSuccess(const ngamsSTAT*  status)

 Return 1 if the status object contains a response indicating success.

 Returns:   1 = SUCCESS, 0 = FAILURE.
 */
int ngamsSuccess(const ngamsSTATUS* status) {
	if (strcmp(status->status, NGAMS_SUCCESS) == 0)
		return 1;
	else
		return 0;
}

/**
 char* ngamsStrRStr(const char* haystack,
 const char* needle)
 Carry out a reverse string search.

 haystack:   String to search.

 needle:     Sub-string to look for.

 Returns:    Pointer to first reverse occurrence of needle or NULL if not
 found.
 */
char* ngamsStrRStr(const char* haystack, const char* needle)

{
	int i = strlen(haystack);
	while (i >= 0)
		if (strstr((haystack + --i), needle) != NULL)
			break;
	if (i >= 0)
		return (char*) (haystack + i);
	else
		return NULL;
}

/**
 void ngamsToUpper(char* str)

 Convert a string to upper characters.

 str:      Pointer to string to convert.

 Returns:  Upper-cased string.
 */
void ngamsToUpper(char* str) {
	int i;
	for (i = 0; i < strlen(str); i++)
		*(str + i) = (char) toupper((int) *(str + i));
}

/**
 void ngamsTrimString(char*  str,
 char*  trimChars)
 Trim a string according to the given trim characters.

 str:          String to be trimmed.

 trimChars:    String buffer with the preceeding/trailing characters
 to trim away.

 Returns:      Trimmed string.
 */
void ngamsTrimString(char* str, const char* trimChars) {
	char* tmpStr = NULL;
	char* strPtr;

	/* Trim from left */
	strPtr = str;
	while ((*strPtr != '\0') && (strchr(trimChars, *strPtr) != NULL))
		strPtr++;
	tmpStr = malloc(strlen(strPtr) + 1);
	strcpy(tmpStr, strPtr);

	/* Trim from right */
	strPtr = (tmpStr + strlen(tmpStr) - 1);
	if (strPtr < tmpStr)
		strPtr = tmpStr; //for the string that has no character, but only null
	while ((strPtr != tmpStr) && (strchr(trimChars, *strPtr) != NULL))
		*strPtr-- = '\0';
	strcpy(str, tmpStr);
	free(tmpStr);
}

/**
 int ngamsSplitString(char*       str,
 const char* splitPat,
 const int   maxSubStr,
 char*       subStr[],
 int*        noOfSubStr)

 Splits a string up into sub-strings according to a given pattern.
 If the given pattern occurs several times in sequence, they are all skipped.


 str:          String to be splitted.

 splitPat:     Pattern which to use for splitting up the string.

 maxSubStr:    Maximum number of substrings to put in the provided string array.

 subStr:       Array with the substrings resulting from splitting up the input
 string. Note, the calling application must allocate this array.

 noOfSubStr:   Number of tokens extracted.

 Returns:      Trimmed string.
 */
int ngamsSplitString(char* str, const char* splitPat, const int maxSubStr,
		ngamsMED_BUF subStr[], int* noOfSubStr) {
	char* tokenP;
	char* strP;
	char* prevPos;
	int elCount;

	strP = str;
	elCount = 0;
	memset(subStr, 0, (maxSubStr * sizeof(ngamsMED_BUF)));
	while (elCount < maxSubStr) {
		if ((tokenP = strtok_r(strP, splitPat, &prevPos)) == NULL)
			break;

		/* Skip to next if the tokenP points to another split pattern, if there
		 * would be several in a sequence.
		 */
		if (strstr(tokenP, splitPat) == tokenP) {
			elCount++;
			continue;
		}

		/* Copy token into output array. */
		strcpy(subStr[elCount], tokenP);
		strP = NULL;
		elCount++;
	}
	if (elCount == maxSubStr)
		return -1;

	*noOfSubStr = elCount;
	return 0;
}

/**
 char* ngamsVersion(void)

 Return pointer to buffer with the NG/AMS version information.

 Returns:   Pointer to internal, static buffer containing the version
 for the NG/AMS C-API.
 */
char* ngamsVersion(void) {
	static char verBuf[64];

	sprintf(verBuf, "%s/%s", ngamsNGAMS_SW_VER, ngamsVER_DATE);
	return verBuf;
}

/**
 *******************************************************************************
 */

/* EOF */

