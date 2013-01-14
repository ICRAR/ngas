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
 * "@(#) $Id: ngamsArchiveClient.c,v 1.19 2010/05/04 09:44:20 hsommer Exp $"
 *
 * who       when      what
 * --------  --------  --------------------------------------------------
 * jknudstr  22/12/03  created
 */

/************************************************************************
 *   NAME
 *   ngamsArchiveClient - NG/AMS Archive Client
 *
 *   SYNOPSIS
 *   Invoke the tool without command parameters to get a help page.
 *
 *   DESCRIPTION
 *   Small client application used to archive files into the NGAS System,
 *   possibly located on a geographically different site than the data
 *   provider.
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

/* static char *rcsId="@(#) $Id: ngamsArchiveClient.c,v 1.19 2010/05/04 09:44:20 hsommer Exp $"; */
/* static void *use_rcsId = ((void)&use_rcsId,(void *) &rcsId); */


#include <unistd.h>
#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/time.h>
#include <unistd.h>
#include <db.h>
#include <pthread.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <errno.h>
#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>
#include <string>

#include "ngams.h"
#include "ngamsArchiveClient.h"
#include "ArchiveController.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace std;



/**************************************************************************
 * Various utility functions.
 */
//char* ngamsArchCliManPage(void);
//char *_ngamsManPage(void);

//bool ngamsStarted = false;
bool ngamsTerminatedByDC = false;

/* Parse input parameters */
ngamsARCHIVE_CLIENT_REGISTRY registry = {{0}};

//Dave Pallot: Added a mutex for the ngamsGetdirectory static functions
pthread_mutex_t g_dirmutex;

void ngamsCorrectUsage()
{
    printf("man page");
}


char* ngamsGetQueueDir(const char*  rootDir)
{
    static ngamsMED_BUF  _queueDir = {0};
    pthread_mutex_lock(&g_dirmutex);
    if (_queueDir[0] == '\0')
    	sprintf(_queueDir, "%s/%s/%s", rootDir,	ngamsARCH_CLI_DIR, ngamsARCH_CLI_QUE_DIR);
    pthread_mutex_unlock(&g_dirmutex);
    return _queueDir;
}


char* ngamsGetArchDir(const char*  rootDir)
{
    static ngamsMED_BUF  _archivedDir = {0};
    pthread_mutex_lock(&g_dirmutex);
    if (_archivedDir[0] == '\0')
    	sprintf(_archivedDir, "%s/%s/%s", rootDir, ngamsARCH_CLI_DIR, ngamsARCH_CLI_ARC_DIR);
    pthread_mutex_unlock(&g_dirmutex);
    return _archivedDir;
}


char* ngamsGetBadDir(const char*  rootDir)
{
    static ngamsMED_BUF  _badDir = {0};
    pthread_mutex_lock(&g_dirmutex);
    if (_badDir[0] == '\0')
    	sprintf(_badDir, "%s/%s/%s", rootDir, ngamsARCH_CLI_DIR, ngamsARCH_CLI_BAD_DIR);
    pthread_mutex_unlock(&g_dirmutex);
    return _badDir;
}


char* ngamsGetLogDir(const char*  rootDir)
{
    static ngamsMED_BUF  _logDir = {0};
    pthread_mutex_lock(&g_dirmutex);
    if (_logDir[0] == '\0')
    	sprintf(_logDir, "%s/%s/%s", rootDir, ngamsARCH_CLI_DIR, ngamsARCH_CLI_LOG_DIR);
    pthread_mutex_unlock(&g_dirmutex);
    return _logDir;
}


char* getPidFile(const char*  rootDir)
{
    static int initialized = 0;
    static ngamsMED_BUF pidFile = {0};

    pthread_mutex_lock(&g_dirmutex);
    if (!initialized)
	{
    	sprintf(pidFile, "%s/%s/.ngamsArchiveClient-PID", rootDir, ngamsARCH_CLI_DIR);
		initialized = 1;
	}
    pthread_mutex_unlock(&g_dirmutex);

    return pidFile;
}


void ngamsArchCliSignalHandler(int sigNo)
{
    ngamsLogInfo(LEV1, "Received signal: %d", sigNo);
    remove(getPidFile(NULL));
    ngamsLogInfo(LEV1, "Terminating ...");
    exit(0);
}

void ngamsGetBaseName(const char*  filename,
		      ngamsMED_BUF baseName)
{
    const char*  chrP;

    chrP = strrchr((char*)filename, '/');
    if (chrP == NULL)
	{
	if (safeStrCp(baseName, filename,
		      sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	    safeStrCp(baseName, "ERROR COPYING SOURCE BUFFER",
		      sizeof(ngamsMED_BUF));
	}
    else
	{
	if (safeStrCp(baseName, (chrP + 1),
		      sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	    safeStrCp(baseName, "ERROR COPYING SOURCE BUFFER",
		      sizeof(ngamsMED_BUF));
	}
}

/**************************************************************************/


/**************************************************************************
 * Stuff to handle the threads running in the application
 */

ngamsBOOL hasKeyDbm(DB*                 dbmHandle,
		    const ngamsMED_BUF  key)
{
    DBT dbmKey, dbmData;

    memset(&dbmKey, 0, sizeof(DBT));
    memset(&dbmData, 0, sizeof(DBT));
    ngamsMED_BUF  tmpKey;
    safeStrCp(tmpKey, key, sizeof(ngamsMED_BUF));
    dbmKey.data = tmpKey;
    dbmKey.size = strlen(key);
    if (dbmHandle->get(dbmHandle, NULL, &dbmKey, &dbmData, 0) == 0)
	return TRUE;
    else
	return FALSE;
}


ngamsSTAT getFromDbm(DB*                 dbmHandle,
		     const ngamsMED_BUF  key,
		     ngamsMED_BUF        value)
{
    DBT dbmKey, dbmData;

    memset(&dbmKey, 0, sizeof(dbmKey));
    memset(&dbmData, 0, sizeof(dbmData));
    ngamsMED_BUF  tmpKey;
    if (safeStrCp(tmpKey, key, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	return ngamsSTAT_FAILURE;
    dbmKey.data = tmpKey;
    dbmKey.size = strlen(key);
    if (dbmHandle->get(dbmHandle, NULL, &dbmKey, &dbmData, 0) == 0)
	{
	if (safeStrNCp(value, (char*)dbmData.data, dbmData.size,
		       sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	    return ngamsSTAT_FAILURE;
	return ngamsSTAT_SUCCESS;
	}
    else
	{
	*value = '\0';
	return ngamsSTAT_FAILURE;
	}
}

ngamsSTAT putInDbm(DB*                 dbmHandle,
		   const char*         key,
		   const ngamsMED_BUF  value)
{
    DBT dbmKey, dbmData;

    memset(&dbmKey, 0, sizeof(dbmKey));
    memset(&dbmData, 0, sizeof(dbmData));
    ngamsMED_BUF  tmpKey;
    if (safeStrCp(tmpKey, key, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	return ngamsSTAT_FAILURE;
    dbmKey.data = tmpKey;
    dbmKey.size = strlen(key);
    ngamsMED_BUF tmpData;
    if (safeStrCp(tmpData, value, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	return ngamsSTAT_FAILURE;
    dbmData.data = tmpData;
    dbmData.size = strlen(tmpData);
    if (dbmHandle->put(dbmHandle, NULL, &dbmKey, &dbmData, 0) == 0)
	{
	dbmHandle->sync(dbmHandle, 0);
	return ngamsSTAT_SUCCESS;
	}
    else
	return ngamsSTAT_FAILURE;
}

ngamsSTAT delFromDbm(DB*           dbmHandle,
		     const char*   key)
{
    DBT dbmKey;

    memset(&dbmKey, 0, sizeof(dbmKey));
    ngamsMED_BUF  tmpKey;
    if (safeStrCp(tmpKey, key, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	return ngamsSTAT_FAILURE;
    dbmKey.data = tmpKey;
    dbmKey.size = strlen(key);
    if (dbmHandle->del(dbmHandle, NULL, &dbmKey, 0) == 0)
	return ngamsSTAT_SUCCESS;
    else
	return ngamsSTAT_FAILURE;
}


/* Return TRUE if the entry is in the queue, otherwise FALSE.
 */
ngamsBOOL fileBeingProcessed(ngamsARCHIVE_CLIENT_REGISTRY*  regP,
			     const ngamsMED_BUF             sourceFile)
{
    ngamsBOOL inArchiveQ, inProcessingQ;

    pthread_mutex_lock(&(regP->archiveQueueMutex));
    inArchiveQ = hasKeyDbm(regP->queueDbmHandle, sourceFile);
    inProcessingQ = hasKeyDbm(regP->procDbmHandle, sourceFile);
    pthread_mutex_unlock(&(regP->archiveQueueMutex));

    if ((inArchiveQ == TRUE) || (inProcessingQ == TRUE))
	return TRUE;
    else
	return FALSE;
}

/* Put entry in queue. If this went OK, return ngamsSTAT_SUCCESS, else
 * if not possible (=no space) return ngamsSTAT_FAILURE.
 */
ngamsSTAT putEntryInQ(ngamsARCHIVE_CLIENT_REGISTRY*  regP,
		      const ngamsMED_BUF             sourceFile)
{
    pthread_mutex_lock(&(regP->archiveQueueMutex));

    /* Check if this file is already being processed */
    if (hasKeyDbm(regP->queueDbmHandle, sourceFile) ||
	hasKeyDbm(regP->procDbmHandle, sourceFile))
	{
	/* OK, assume file is already being processed - do nothing */
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	return ngamsSTAT_SUCCESS;
	}

    /* Create a new entry in the queue + DBM */
    int newWrIdx = ((regP->archiveQueueWrIdx + 1) % ngamsMAX_FILES_IN_Q);
    if (safeStrCp(regP->archiveQueue[newWrIdx], sourceFile,
		  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	{
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	return ngamsSTAT_FAILURE;
	}
    if (putInDbm(regP->queueDbmHandle, sourceFile,
		 sourceFile) == ngamsSTAT_FAILURE)
	{
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	ngamsLogError("Problem storing element: %s in DBM", sourceFile);
	return ngamsSTAT_FAILURE;
	}
    /* Increment the queue write index and queue count */
    regP->archiveQueueWrIdx = newWrIdx;
    (regP->archiveQueueCount)++;

    pthread_mutex_unlock(&(regP->archiveQueueMutex));

    return ngamsSTAT_SUCCESS;
}

/* If entry was taken from the queue, the name of the source file will be
 * kept in the sourceFile parameter. Else sourceFile will be a null string.
 * Entries are popped from the DBM.
 */
ngamsSTAT getNextEntryFromQ(ngamsARCHIVE_CLIENT_REGISTRY*  regP,
			    ngamsMED_BUF                   sourceFile)
{
    pthread_mutex_lock(&(regP->archiveQueueMutex));

    int newRdIdx = ((regP->archiveQueueRdIdx + 1) % ngamsMAX_FILES_IN_Q);
    *sourceFile = '\0';
    if (*(regP->archiveQueue[newRdIdx]) == '\0')
	{
	/* There seems to be no entries pending right now - do nothing */
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	return ngamsSTAT_SUCCESS;
	}

    /* Read out the name of the next file, delete the entry in the memory
     * queue and in the DBM.
     */
    if (safeStrCp(sourceFile, regP->archiveQueue[newRdIdx],
		  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	{
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	return ngamsSTAT_FAILURE;
	}
    if (delFromDbm(regP->queueDbmHandle, sourceFile) == ngamsSTAT_FAILURE)
	{
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	if (TRUE)
	    {
	    /* TODO: Proper error handling */
	    ngamsLogError("Error deleting archive request entry: %s from "
			  "Queue DBM", sourceFile);
	    return ngamsSTAT_FAILURE;
	    }
	else
	    {
	    /* TODO: Work-around error handling */
	    *(regP->archiveQueue[newRdIdx]) = '\0';
	    *sourceFile = '\0';
	    return ngamsSTAT_SUCCESS;
	    }
	}
    if (putInDbm(regP->procDbmHandle, sourceFile,
		 sourceFile) == ngamsSTAT_FAILURE)
	{
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	ngamsLogError("Problem storing element: %s in Processing DBM",
		      sourceFile);
	return ngamsSTAT_FAILURE;
	}
    *(regP->archiveQueue[newRdIdx]) = '\0';
    regP->archiveQueueRdIdx = newRdIdx;

    pthread_mutex_unlock(&(regP->archiveQueueMutex));

    return ngamsSTAT_SUCCESS;
}


/* Remove the given file from the queue(s).
 */
ngamsSTAT deleteEntryFromQ(ngamsARCHIVE_CLIENT_REGISTRY*  regP,
			   const char*                    sourceFile)
{
    pthread_mutex_lock(&(regP->archiveQueueMutex));

    if (!hasKeyDbm(regP->procDbmHandle, sourceFile))
	{
	/* OK, seems file is not being processed. */
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	return ngamsSTAT_SUCCESS;
	}
    if (delFromDbm(regP->procDbmHandle, sourceFile) == ngamsSTAT_FAILURE)
	{
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	ngamsLogError("Error deleting archive request entry: %s from "
		      "Processing DBM", sourceFile);
	return ngamsSTAT_FAILURE;
	}
    /* Decrement the queue count */
    (regP->archiveQueueCount)--;

    pthread_mutex_unlock(&(regP->archiveQueueMutex));

    return ngamsSTAT_SUCCESS;
}


/* Thread that takes care of monitoring the Archive Queue and of scheduling
 * new files for archiving.
 *
 *
 */
void* _archiveQMonThread(void*  ptr)
{
    ngamsLogInfo(LEV1, "Starting execution of Archive Queue Monitoring "
		 "Thread");

    ngamsARCHIVE_CLIENT_REGISTRY  *regP;
    regP = (ngamsARCHIVE_CLIENT_REGISTRY*)ptr;
    while (!ngamsTerminatedByDC)
	{
	/* Only check the queue if the archive queue is not full. Criteria is
	 * that if there is less files in the queue than twice the number of
	 * streams, a new scan is made.
	 */
	if (regP->archiveQueueCount < (2 * regP->streams))
	    {
			if (ngamsCheckArchiveQueue(regP) == ngamsSTAT_FAILURE) //here is the calling entry from DataCapture manager (stopTransferring()) for archiving! chen.wu@icrar.org
			{
				ngamsLogError("Serious error ocurred in server loop while "
						  "checking Archive File Queue!");
			}
	    }

	ngamsSleep(regP->archiveQueuePollTime);

	/* Check if thread should stop execution */
	if (!(regP->threadRunPermission)) pthread_exit(NULL);
	} /* end-while (1) */
    return NULL;
}

bool hasPendingFilesToArchive(ngamsARCHIVE_CLIENT_REGISTRY  *regP, int* newRdIdx) {
	*newRdIdx = ((regP->archiveQueueRdIdx + 1) % ngamsMAX_FILES_IN_Q);
	if (*(regP->archiveQueue[*newRdIdx]) == '\0') {
		return false;
	} else {
		return true;
	}
}


bool hasPendingPostToSend(ngamsARCHIVE_CLIENT_REGISTRY  *regP, int* newRdIdx) {
	*newRdIdx = ((regP->sendToMCQueueRdIdx + 1) % ngamsMAX_FILES_IN_Q);
	if (*(regP->sendToMCQueue[*newRdIdx]) == '\0') {
		return false;
	} else {
		return true;
	}
}

ngamsSTAT ngamsSendPostToMC(ngamsARCHIVE_CLIENT_REGISTRY*  regP,   const char* postContent) {

	//ngamsMED_BUF rawURI, encodedURI, baseName;
	//ngamsBIG_BUF postContent;
	ngamsBIG_BUF cdisposition;
	//ngamsGetBaseName(sourceFile, baseName);
	//sprintf(rawURI, "http://mwangas/RETRIEVE?file_id=%s", baseName);
	//ngamsEncodeUrlVal(rawURI, 0, encodedURI);
	//sprintf(postContent, "file_id=%s&uri=%s", baseName, encodedURI);

	ngamsHTTP_DATA repDataRef;
	ngamsHTTP_RESP httpResp;
	ngamsHTTP_HDR httpHdr;

	ngamsDATA_LEN repDataLen = 0;

	ngamsMED_BUF srcFile;
	*srcFile = '\0';

	//ngamsInitLogConds();

	//"202.8.37.243"

	if (ngamsHttpPost("10.128.99.9", 8000, ngamsUSER_AGENT, "ngas/ARCHIVE_URI/", "application/x-www-form-urlencoded", cdisposition, srcFile, postContent, strlen(postContent), &repDataRef, &repDataLen, &httpResp, httpHdr)
					!= ngamsSTAT_SUCCESS) {
		ngamsLogError("Fail to push URIs to Django and the M&C database.");
	}
	//printf("ref data = %s", repDataRef.pdata);
	//printf("http resp = %s", httpResp.message);

	return ngamsSTAT_SUCCESS; //ngamsSTAT_FAILURE
}


/**
 * The consumer of the SentToMCQueue
 *
 */
void* _sendToMCThread(void* ptr) {
	ngamsLogInfo(LEV1, "Starting execution of SentToMC Thread");
	bool isSentToMCRunning = true;

	ngamsARCHIVE_CLIENT_REGISTRY  *regP;
	regP = (ngamsARCHIVE_CLIENT_REGISTRY*)ptr;
	ngamsMED_BUF postContent;

	while (isSentToMCRunning) {
		pthread_mutex_lock(&(regP->sendToMCQueueMutex));

		if (ngamsTerminatedByDC) {
			isSentToMCRunning = false;
			pthread_mutex_unlock(&(regP->sendToMCQueueMutex));
			continue;
		}

		int newRdIdx = 0;

		while (!ngamsTerminatedByDC && !hasPendingPostToSend(regP, &newRdIdx)) {
			pthread_cond_wait(&(regP->sendToMCQueueCond), &(regP->sendToMCQueueMutex));
		}

		if (ngamsTerminatedByDC) {
			isSentToMCRunning = false;
			pthread_mutex_unlock(&(regP->sendToMCQueueMutex));
			continue;
		}

		*postContent = '\0';
		if (safeStrCp(postContent, regP->sendToMCQueue[newRdIdx], sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE) {
			ngamsLogError("Error copying post content from newRdIdx to postContent");
			pthread_mutex_unlock(&(regP->sendToMCQueueMutex));
			continue;
		}
		*(regP->sendToMCQueue[newRdIdx]) = '\0';
		regP->sendToMCQueueRdIdx = newRdIdx;

		pthread_mutex_unlock(&(regP->sendToMCQueueMutex)); //release the lock so that the producer thread can at least do something while the HTTP post is being sent
		if (*postContent == '\0') {
			ngamsLogError("Problems in obtaining the post content to be sent.");
			continue;
		}

		/* Actual HTTP Post is here!!!

		if (ngamsSendPostToMC(regP, postContent) == ngamsSTAT_FAILURE) {
			ngamsLogError("Error sending HTTP Post to MC Web server: %s", postContent);
			//log it for now
			//continue;
		}
		*/

		/* Actual Thrift msg is here */
		string combinedMsg = std::string(postContent);
		std::vector<std::string> strs;
		boost::split(strs, combinedMsg, boost::is_any_of("|"));

		string uriToSend = strs[0];

		string sizeStr = strs[1];
		char *endptr;
		size_t sizeToSend = strtoll(sizeStr.c_str(), &endptr, 10);

		int errCode = atoi(strs[2].c_str());


		/*
		size_t found = combinedMsg.find('|');
		if (found == string::npos) {
			uriToSend = combinedMsg;
		} else {
			uriToSend = combinedMsg.substr(0, found);
			string sizeStr = combinedMsg.substr(found + 1, combinedMsg.length() - 1 - found);
			char *endptr;
			sizeToSend = strtoll(sizeStr.c_str(), &endptr, 10);
		}
		*/

		try {
			boost::shared_ptr<TSocket> socket(new TSocket(registry.mcHost, registry.mcPort));
			boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
			boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
			ArchiveControllerClient arc(protocol);
			transport->open();

			if (errCode == ngamsSTAT_SUCCESS) {
				arc.fileArchived(boost::asio::ip::host_name(), uriToSend, sizeToSend);
			} else {
				arc.fileArchiveError(boost::asio::ip::host_name(), uriToSend, errCode, "See DataCapture/src/ngams.h <typedef enum ngamsSTAT>");
			}
			transport->close();
		} catch (apache::thrift::transport::TTransportException& ex)  {
			ngamsLogError("Thrift transport exception while sending to MC %s", ex.what());
		} catch (std::exception& e) {
			ngamsLogError("Exception thrown for Thrifting %s", e.what());
		}
		pthread_mutex_lock(&(regP->sendToMCQueueMutex));
		/* Decrement the queue count */
		(regP->sendToMCQueueCount)--;

		//notify main thread that an HTTP post is sent, this is to (chen.wu@icrar.org)
		//(1) tell the main thread that it might be okay to finish the application and quit if the conditions are met
		pthread_cond_signal(&(regP->sendToMCQueueCountCond));

		pthread_mutex_unlock(&(regP->sendToMCQueueMutex));
		/* Check if thread should stop execution */
		if (!(regP->threadRunPermission)) pthread_exit(NULL);
	}

	return NULL;
}
/**
 *  Put into the queue the post content, which will be sent as an HTTP POST request
 *  to the M&C Web Server
 */
int ngamsScheduleSendToMC(ngamsMED_BUF postContent) {
	ngamsARCHIVE_CLIENT_REGISTRY*  regP = &registry;
	pthread_mutex_lock(&(regP->sendToMCQueueMutex));

	int newWrIdx = ((regP->sendToMCQueueWrIdex + 1) % ngamsMAX_FILES_IN_Q);
	if (safeStrCp(regP->sendToMCQueue[newWrIdx], postContent, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE) {
		pthread_mutex_unlock(&(regP->sendToMCQueueMutex));
		return ngamsSTAT_FAILURE;
	}

	/* Increment the queue write index and queue count */
	regP->sendToMCQueueWrIdex = newWrIdx;
	(regP->sendToMCQueueCount)++;

	pthread_cond_signal(&(regP->sendToMCQueueCond));
	pthread_mutex_unlock(&(regP->sendToMCQueueMutex));
	return ngamsSTAT_SUCCESS;
}


/**
 *  New archive thread
 *
 *  chen.wu@icrar.org
 *
 */
void* _archiveThread(void* ptr) {
	ngamsLogInfo(LEV1, "Starting execution of Archive Thread");
	bool isArchiveRunning = true; //how can we make this variable thread unique?

	ngamsARCHIVE_CLIENT_REGISTRY  *regP;
	regP = (ngamsARCHIVE_CLIENT_REGISTRY*)ptr;
	ngamsMED_BUF sourceFile;

	while (isArchiveRunning) {
		pthread_mutex_lock(&(regP->archiveQueueMutex));

		if (ngamsTerminatedByDC) {
			isArchiveRunning = false;
			pthread_mutex_unlock(&(regP->archiveQueueMutex));
			continue;
		}

		int newRdIdx = 0;

		while (!ngamsTerminatedByDC && !hasPendingFilesToArchive(regP, &newRdIdx)) {
			pthread_cond_wait(&(regP->archiveQueueCond), &(regP->archiveQueueMutex));
		}

		if (ngamsTerminatedByDC) {
			isArchiveRunning = false;
			pthread_mutex_unlock(&(regP->archiveQueueMutex));
			continue;
		}

		//now we can actually do something

	    /* Read out the name of the next file, delete the entry in the memory
	     * queue and in the DBM.
	     */
		*sourceFile = '\0';
		if (safeStrCp(sourceFile, regP->archiveQueue[newRdIdx], sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE) {
			ngamsLogError("Error copying filename from newRdIdx to sourceFile");
			pthread_mutex_unlock(&(regP->archiveQueueMutex));
			continue;
		}
	    if (delFromDbm(regP->queueDbmHandle, sourceFile) == ngamsSTAT_FAILURE) {
			ngamsLogError("Error deleting archive request entry: %s from Queue DBM", sourceFile);
			pthread_mutex_unlock(&(regP->archiveQueueMutex));
			continue;
		}
	    if (putInDbm(regP->procDbmHandle, sourceFile, sourceFile) == ngamsSTAT_FAILURE) {
			ngamsLogError("Problem storing element: %s in Processing DBM", sourceFile);
	    	pthread_mutex_unlock(&(regP->archiveQueueMutex));
			continue;
		}
	    *(regP->archiveQueue[newRdIdx]) = '\0';
	    regP->archiveQueueRdIdx = newRdIdx;

	    pthread_mutex_unlock(&(regP->archiveQueueMutex)); //release lock so that other threads can at least do something while the file is being archived / sent
	    if (*sourceFile == '\0') {
	    	ngamsLogError("Problems in obtaining the filename to be archived.");
			continue;
	    }

	    /* Actual archiving is here!!! */

	    /* first figure out the size of the file for logging purpose */
	    int64_t fileSize = 0;
	    struct stat statBuf;
	    if (stat(sourceFile, &statBuf) == -1) {
			ngamsLogDebug("Error calling stat() on file: %s. Error: %s", sourceFile, strerror(errno));
		} else {
			fileSize = statBuf.st_size;
		}

	    ngamsSTAT archiveStatus = ngamsArchiveFile(regP, sourceFile);

	    if (archiveStatus != ngamsSTAT_SUCCESS) {
	    	ngamsLogError("Error archiving file: %s", sourceFile);
	    	/**
	    	if (delFromDbm(regP->procDbmHandle, sourceFile) == ngamsSTAT_FAILURE) {// remove it from the "being processed" queue, so get ready for next schedule if necessary
	    		ngamsLogError("Error deleting the request entry being processed: %s from Processing DBM", sourceFile);
	    	 }
	    	 **/
	    	//continue; //cannot continue otherwise it won't get deleted from all those queues, and counters
	    }
	    if (regP->sendToMMC) {
			ngamsMED_BUF rawURI, baseName; //encodedURI
			ngamsBIG_BUF postContent;
			ngamsGetBaseName(sourceFile, baseName);
			sprintf(rawURI, "http://mwangas/RETRIEVE?file_id=%s", baseName);
			//ngamsEncodeUrlVal(rawURI, 0, encodedURI);
			sprintf(postContent, "%s|%llu|%d", rawURI, fileSize, archiveStatus);

			ngamsScheduleSendToMC(postContent);
	    }

	    pthread_mutex_lock(&(regP->archiveQueueMutex));

	    //copied from deleteEntryFromQ(regP, sourceFile), which was invoked originally within the ngamsArchiveFile function
	    if (!hasKeyDbm(regP->procDbmHandle, sourceFile)) {
	    	/* OK, seems file is not being processed. */
	    	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	    	continue;
	    }

	    if (delFromDbm(regP->procDbmHandle, sourceFile) == ngamsSTAT_FAILURE) {
	    	ngamsLogError("Error deleting archive request entry: %s from Processing DBM", sourceFile);
	    	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	    	continue;
	    }

	    /* Decrement the queue count */
	    (regP->archiveQueueCount)--;

	    //notify main thread that a file archive is done, this is to (chen.wu@icrar.org)
	    //(1) tell the main thread that it is okay to finish the application and quit
	    //(2) report the current archiving progress to the main thread, which can then do the flow/congestion control
	    pthread_cond_signal(&(regP->archiveQueueCountCond));

	    pthread_mutex_unlock(&(regP->archiveQueueMutex));
	    /* Check if thread should stop execution */
	    if (!(regP->threadRunPermission)) pthread_exit(NULL);
	}

	return NULL;


}

/* Thread that take care of archiving files in the queue into the remote
 * NGAS systems. May run in several instances.
 */
void* _archiveWorkingThread(void*  ptr)
{
    ngamsLogInfo(LEV1, "Starting execution of Archive Thread");

    ngamsARCHIVE_CLIENT_REGISTRY  *regP = (ngamsARCHIVE_CLIENT_REGISTRY*)ptr;
    ngamsMED_BUF sourceFile = {0};

    while (!ngamsTerminatedByDC)
	{
		if (getNextEntryFromQ(regP, sourceFile) == ngamsSTAT_FAILURE)
		{
			ngamsLogError("Error requesting file from queue!");
		}

		if (*sourceFile != '\0')
		{
			if (ngamsArchiveFile(regP, sourceFile) == ngamsSTAT_FAILURE)
				ngamsLogError("Error archiving file: %s", sourceFile);
		}

		/* Check if thread should stop execution */
		if (!(regP->threadRunPermission))
			pthread_exit(NULL);

		/* Make a small sleep to avoid that the thread runs wild */
		ngamsSleep(0.200);
	} /* end-while (1) */
    return NULL;
}

/* Thread that handles the clean up of the Archived Files Queue.
 */
void* _cleanUpThread(void*  ptr)
{
	ngamsLogInfo(LEV1, "Starting execution of Clean Up Thread");

	ngamsARCHIVE_CLIENT_REGISTRY  *regP = (ngamsARCHIVE_CLIENT_REGISTRY*)ptr;

	while (!ngamsTerminatedByDC)
	{
		ngamsCleanUpArchivedFiles(regP, true);

		/*if (ngamsCleanUpArchivedFiles(regP, true) == ngamsSTAT_FAILURE)
		{
			ngamsLogError("Error invoking ngamsCleanUpArchivedFiles()!");
		}*/

		/* Check if thread should stop execution */
		if (!(regP->threadRunPermission))
			pthread_exit(NULL);

		/* Make a small sleep to avoid that the thread runs wild */
		ngamsSleep(0.500);
	} /* end-while (1) */
	return NULL;
}

/**************************************************************************/


ngamsSTAT ngamsExecCmd(const char*    cmd,
		       ngamsHUGE_BUF  res)
{
    FILE*  fp;

    memset(res, 0, sizeof(ngamsHUGE_BUF));
    ngamsLogInfo(LEV4, "Executing command: %s ...", cmd);
    if ((fp = popen(cmd, "r")) == NULL)
	{
	ngamsLogError("Error executing command: %s", cmd);
	return ngamsSTAT_FAILURE;
	}
    fread(res, sizeof(ngamsHUGE_BUF), sizeof(char), fp);
    ngamsLogInfo(LEV4, "Result of cmd: %s: %s", cmd, res);
    pclose(fp);

    return ngamsSTAT_SUCCESS;
}


ngamsSTAT ngamsServe(ngamsARCHIVE_CLIENT_REGISTRY*  regP)
{
    char*          dirs[] = {const_cast<char *>(""),
    		const_cast<char *>(ngamsARCH_CLI_LOG_DIR),
    		const_cast<char *>(ngamsARCH_CLI_QUE_DIR),
    		const_cast<char *>(ngamsARCH_CLI_ARC_DIR),
    		const_cast<char *>(ngamsARCH_CLI_BAD_DIR),
			     NULL};
    int            i = 0, tmpFd;
    ngamsMED_BUF   tmpName, rootDirLoc, tmpHostId, systemId, pidBuf, delCommand;
    ngamsSTAT      stat;

    /* Create the directory structure */
    if (safeStrCp(rootDirLoc, regP->rootDir,
		  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
	return ngamsSTAT_FAILURE;
    while (dirs[i] != NULL)
	{
		sprintf(tmpName, "%s/%s/%s", rootDirLoc, ngamsARCH_CLI_DIR, dirs[i]);
		/* Check if directory already exists */
		if ((tmpFd = open(tmpName, O_RDONLY, 0)) == -1) {
			if (mkdir(tmpName, ngamsSTD_DIR_PERMS) == -1) {
				fprintf(stderr, "\nCould not create directory: %s", tmpName);
				return ngamsSTAT_FAILURE;
			}
		} else { // if the directory already exists, do nothing. But remember to close the file descriptor that was just opened at line 910
			close(tmpFd);
			if (i == 2) {
				//remove all files in the queue
				sprintf(delCommand, "rm -f %s/*", tmpName); 
				int delre = system(delCommand);
				if (delre) {
					ngamsLogWarning("Queue: %s could not be cleaned while starting ngamsClient", tmpName);	
				}

			}
		}
		i++;
	}

    /* Store PID of the Archive Client in file in its working directory */
    remove(getPidFile(regP->rootDir));
    if ((tmpFd = creat(getPidFile(regP->rootDir), 0666)) == -1)
	{
	fprintf(stderr, "\nCould not create PID file: %s",
		getPidFile(regP->rootDir));
	return ngamsSTAT_FAILURE;
	}
    memset(pidBuf, 0, sizeof(ngamsMED_BUF));
    sprintf(pidBuf, "%d", (int)getpid());
    write(tmpFd, pidBuf, strlen(pidBuf));
    close(tmpFd);

    /* Set up logging properties */
    ngamsSetVerboseLevel((ngamsLOG_LEVEL) regP->verboseLevel);
    sprintf(tmpName, "%s/%s/%s/%s", rootDirLoc, ngamsARCH_CLI_DIR,
	    ngamsARCH_CLI_LOG_DIR, ngamsARCH_CLI_LOG_FILE);
    if ((stat = ngamsPrepLog(tmpName, (ngamsLOG_LEVEL) regP->logLevel,
			     regP->logRotate, regP->logHistory)) !=
	ngamsSTAT_SUCCESS)
	return stat;
    ngamsGetHostName(tmpHostId);
    sprintf(systemId, "%s@%s", ngamsARCH_CLI_NAME, tmpHostId);
    ngamsLogInfo(LEV1, "Initializing NG/AMS Archive Client - SYSTEM-ID: %s ",
		 systemId);

    /* Set up signal exit handlers */
    signal(SIGINT, ngamsArchCliSignalHandler);
    signal(SIGTERM, ngamsArchCliSignalHandler);
    signal(SIGHUP, ngamsArchCliSignalHandler);

    /* DBM for files being queued. */
    int dbmStat;
    if ((dbmStat = db_create(&(regP->queueDbmHandle), NULL, 0)) != 0)
	{
	ngamsLogError("Error creating Archive Queue DBM. Error: %s",
		      db_strerror(dbmStat));
	return ngamsSTAT_FAILURE;
	}
    if ((dbmStat = regP->queueDbmHandle->open(regP->queueDbmHandle, NULL,
					      NULL, NULL, DB_HASH, DB_CREATE,
					      0664)) != 0)
	{
	ngamsLogError("Error opening Archive Queue DBM. Error: %s",
		      db_strerror(dbmStat));
	return ngamsSTAT_FAILURE;
	}
    /* DBM for files being processed. */
    if ((dbmStat = db_create(&(regP->procDbmHandle), NULL, 0)) != 0)
	{
	ngamsLogError("Error creating Processing DBM. Error: %s",
		      db_strerror(dbmStat));
	return ngamsSTAT_FAILURE;
	}
    if ((dbmStat = regP->procDbmHandle->open(regP->procDbmHandle, NULL, NULL,
					     NULL, DB_HASH, DB_CREATE,
					     0664)) != 0)
	{
	ngamsLogError("Error opening Processing DBM. Error: %s",
		      db_strerror(dbmStat));
	return ngamsSTAT_FAILURE;
	}

    /* Start the threads */
    int             creStat;
    pthread_attr_t  thrAttr;
    pthread_attr_init(&thrAttr);
    pthread_attr_setdetachstate(&thrAttr, PTHREAD_CREATE_JOINABLE);

//    /* Create Archive Queue Monitoring Thread */
      /* Disable the Archive Queue Monitoring Thread */
//    creStat = pthread_create(&(regP->archiveQMonThread), &thrAttr,
//			     _archiveQMonThread, regP);
//    if (creStat != 0)
//	{
//	ngamsLogError("Error creating thread: Archive Monitoring Thread");
//	return ngamsSTAT_FAILURE;
//	}

    /* Create Archive Threads */
    int n;
    for (n = 0; n < regP->streams; n++)
	{
	creStat = pthread_create(&(regP->archiveThreads[n]), &thrAttr, _archiveThread, regP);
	if (creStat != 0) {
		ngamsLogError("Error creating thread: Archive Thread/%d", (n + 1));
	    return ngamsSTAT_FAILURE;
	    }
	}
    /* Create Clean Up Thread */
    creStat = pthread_create(&(regP->cleanUpThread), &thrAttr,
			     _cleanUpThread, regP);
    if (creStat != 0) {
		ngamsLogError("Error creating thread: Clean Up Thread");
		return ngamsSTAT_FAILURE;
	}

    /* Create Send To MC Thread*/
    creStat = pthread_create(&(regP->sendToMCThread), &thrAttr, _sendToMCThread, regP);
    if (creStat != 0) {
    	ngamsLogError("Error creating thread: Send To MC Thread");
    	return ngamsSTAT_FAILURE;
    }

    //ngamsStarted = true;
    ngamsLogInfo(LEV1, "NG/AMS Archive Client initialized");

    /* Wait for the threads to finish */
    ngamsLogInfo(LEV1,"Serving ...");
//    void*  thrValPtr;
//    if (pthread_join(regP->archiveQMonThread, &thrValPtr) != 0)
//	ngamsLogError("Error returned from pthread_join() for "
//		      "Archive Queue Monitoring Thread");
//    for (n = 0; n < regP->streams; n++)
//	{
//	if (pthread_join(regP->archiveThreads[n], &thrValPtr) != 0)
//	    ngamsLogError("Error returned from pthread_join() for "
//			  "Archive Thread/%d", (n + 1));
//	}
//    if (pthread_join(regP->cleanUpThread, &thrValPtr) != 0)
//	ngamsLogError("Error returned from pthread_join() for "
//		      "Clean Up Thread");
//    ngamsLogInfo(LEV1,"ngamsArchiveClient (Server) terminating"); //chen.wu@icrar.org
//
//    ngamsStarted = false;
    return ngamsSTAT_SUCCESS;
}




ngamsSTAT ngamsMoveFile2StatDir(const char*  statDir,
				const char*  filename,
				const char*  queueFilename,
				const char*  xmlStat)
{
    ngamsMED_BUF trgFilename, xmlStatDocFile, isoTime;

    ngamsGenIsoTime(3, isoTime);
    sprintf(trgFilename, "%s/%s___%s", statDir, isoTime, filename);
    sprintf(xmlStatDocFile, "%s/%s___%s%s", statDir, isoTime, filename, ngamsARCH_CLI_STAT_EXT);
    ngamsLogInfo(LEV1, "Moving handled file (or link): %s to directory: %s ...", queueFilename, trgFilename);

    if (rename(queueFilename, trgFilename) == -1)
	{
		ngamsLogError("Serious error ocurred moving handled "
				  "file: %s from Archive Queue Directory to "
				  "directory: %s! Terminating!",
				  queueFilename, trgFilename);

		return ngamsSTAT_FAILURE;
	}

    ngamsLogInfo(LEV1, "Moved handled file (or link): %s to the directory: %s ...", queueFilename, trgFilename);
    ngamsLogInfo(LEV1, "Creating XML document with status from NG/AMS: %s", xmlStatDocFile);

    if (ngamsSaveInFile(xmlStatDocFile, xmlStat) == ngamsSTAT_FAILURE)
    	return ngamsSTAT_FAILURE;

    return ngamsSTAT_SUCCESS;
}


void ngamsGenNgamsStatSum(const ngamsSTATUS*  status,
			  ngamsHUGE_BUF       statBuf)
{
    sprintf(statBuf, "NG/AMS Status: Date %s - Error Code: %d - Host ID: %s "
	    "- Status: %s - State: %s - Sub-State: %s - Version: %s - "
	    "Message: %s", status->date, status->errorCode, status->hostId,
	    status->status, status->state, status->subState, status->version,
	    status->message);
}


/**
 * parameter sourceFile qualified name of the file to be archived
 *
 * This will be directly called from DataCaptureManager
 * 1. It pushes the current filename into the queue directly
 * 2. It notifies the _archiveThread
 *
 * This function is based on the combination of
 *
 * fileBeingProcessed(regP, queueFilename)
 * putEntryInQ(regP, queueFilename)
 *
 * chen.wu@icrar.org
 */
int ngamsScheduleArchive(ngamsMED_BUF sourceFile) {
	ngamsARCHIVE_CLIENT_REGISTRY*  regP = &registry;

	ngamsBOOL inArchiveQ, inProcessingQ;

	pthread_mutex_lock(&(regP->archiveQueueMutex));
	inArchiveQ = hasKeyDbm(regP->queueDbmHandle, sourceFile);
	inProcessingQ = hasKeyDbm(regP->procDbmHandle, sourceFile);

	if ((inArchiveQ != TRUE) && (inProcessingQ != TRUE)) {
		/* Create a new entry in the queue + DBM */
		int newWrIdx = ((regP->archiveQueueWrIdx + 1) % ngamsMAX_FILES_IN_Q);
		if (safeStrCp(regP->archiveQueue[newWrIdx], sourceFile, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE) {
			pthread_mutex_unlock(&(regP->archiveQueueMutex));
			return ngamsSTAT_FAILURE;
		}
		if (putInDbm(regP->queueDbmHandle, sourceFile, sourceFile) == ngamsSTAT_FAILURE) {
			pthread_mutex_unlock(&(regP->archiveQueueMutex));
			ngamsLogError("Problem storing element: %s in DBM", sourceFile);
			return ngamsSTAT_FAILURE;
		}
		 /* Increment the queue write index and queue count */
		regP->archiveQueueWrIdx = newWrIdx;
		(regP->archiveQueueCount)++;

		/* Notify all the waiting _archiveThreads */
		pthread_cond_broadcast(&(regP->archiveQueueCond)); //there could be more than one archiveThread!
	}
	pthread_mutex_unlock(&(regP->archiveQueueMutex));
	return ngamsSTAT_SUCCESS;
}




ngamsSTAT ngamsCheckArchiveQueue(ngamsARCHIVE_CLIENT_REGISTRY*  regP)
{
    DIR*             dirPtr = NULL;
    struct dirent*   dirEnt = NULL;
    //ngamsHUGE_BUF    ngamsStatBuf, checksumRes;
    ngamsMED_BUF     queueFilename;//checksumCmd, tmpEnc;
    //ngamsSTAT        stat;
    //ngamsSTATUS      status;

    if ((dirPtr = opendir(ngamsGetQueueDir(regP->rootDir))) == NULL)
	{
	ngamsLogError("Error opening Archive Queue Directory: %s",
		      ngamsGetQueueDir(regP->rootDir));
	goto errExit;
	}
    while ((dirEnt = readdir(dirPtr)) != NULL)
	{
	if (dirEnt->d_name[0] != '.')
	    {
	    sprintf(queueFilename, "%s/%s", ngamsGetQueueDir(regP->rootDir),
		    dirEnt->d_name);
	    if (!fileBeingProcessed(regP, queueFilename))
		{
		ngamsLogInfo(LEV1, "Scheduling file for archiving: %s ...",
			     queueFilename);
		if (putEntryInQ(regP, queueFilename) == ngamsSTAT_FAILURE)
		    {
		    ngamsLogError("Problem queuing element: %s",
				  queueFilename);
		    goto errExit;
		    }
		} /* end-if (!fileBeingProcessed(regP, queueFilename)) */
	    } /* end-if (dirEnt->d_name[0] != '.') */
	} /* end-while ((dirEnt = readdir(dirPtr)) != NULL) */
    closedir(dirPtr);

    return ngamsSTAT_SUCCESS;

 errExit:
    if (dirPtr != NULL) closedir(dirPtr);
    /*if (dirEnt != NULL) free(dirEnt);*/
    return ngamsSTAT_FAILURE;
}


ngamsSTAT ngamsArchiveFile(ngamsARCHIVE_CLIENT_REGISTRY*  regP, const char* sourceFile)
{
    ngamsHUGE_BUF ngamsStatBuf = {0};
    ngamsHUGE_BUF checksumRes = {0};
    ngamsMED_BUF checksumCmd = {0};
    ngamsMED_BUF tmpEnc = {0};
    ngamsMED_BUF baseName = {0};
    ngamsSTAT stat;
    ngamsSTATUS status = {{0}};

    // Dave Pallot
    //memset(&status, 0, sizeof(ngamsSTATUS));

    ngamsLogInfo(LEV1, "Attempting to archive file: %s ...", sourceFile);

    ngamsGetBaseName(sourceFile, baseName);

    if (*(regP->checksum) != '\0')
	{
		sprintf(checksumCmd, "%s %s", regP->checksum, sourceFile);
		if (ngamsExecCmd(checksumCmd, checksumRes) != ngamsSTAT_SUCCESS)
		{
			ngamsLogError("Error generating checksum using Checksum "
				  "Plug-In: %s", regP->checksum);
			return ngamsSTAT_FAILURE;
		}

		ngamsEncodeUrlVal(regP->checksum, 1, tmpEnc);
		ngamsAddParAndVal(&(regP->parArray), "checksum_util", tmpEnc);
		ngamsEncodeUrlVal(checksumRes, 1, tmpEnc);
		ngamsAddParAndVal(&(regP->parArray), "checksum_result", tmpEnc);
		ngamsLogInfo((ngamsLOG_LEVEL)3,"Checksum of file: %s generated with "
				 "Checksum Plug-In: %s: %s", sourceFile,
				 regP->checksum, checksumRes);
	}
//    ngamsLogInfo(3,"Archiving file: %s ...", sourceFile);
//    printf(">>>> %d\n\n",ngamsCMD_QARCHIVE);
	if (strcmp(regP->serverCmd, ngamsCMD_ARCHIVE_STR) != 0)
	{
		stat = ngamsGenSendData(regP->remoteHost, regP->remotePort,
							ngamsCMD_QARCHIVE, ngamsNO_TIME_OUT, sourceFile,
							regP->mimeType, &(regP->parArray), &status);
	}
	else
	{
		stat = ngamsGenSendData(regP->remoteHost, regP->remotePort,
								ngamsCMD_ARCHIVE, ngamsNO_TIME_OUT, sourceFile,
								regP->mimeType, &(regP->parArray), &status);
	}

	ngamsGenNgamsStatSum(&status, ngamsStatBuf);
//    ngamsLogInfo(3,"Status of archiving file: %s: %s", sourceFile, ngamsStatBuf);
    if ((stat == ngamsSTAT_FAILURE) ||
		(status.errorCode != ngamsSTAT_SUCCESS) ||
		(strstr(status.status, NGAMS_FAILURE) != NULL))
	{
    	// Dave Pallot
    	//ngamsLogError("ngamsGenSendData Error: %d\n", stat);
	/* The following actions are taken:
	 *
	 * o Bad File:               Move file to Bad Files Directory. (we should send warning message when the application is shutdown)
	 * o File Back-Log Buffered: Move file to Archived Directory.
	 * o Other Errors:           Keep file in Archive Queue. (e.g. server is down, disks on the server cannot be mounted, etc.).
	 */
    	if ((strstr(status.message,"NGAMS_ER_DAPI_BAD_FILE")!=NULL) ||
			(strstr(status.message, "NGAMS_ER_UNKNOWN_MIME_TYPE1") != NULL))
		{
			ngamsLogError("File: %s was classified as bad by NG/AMS. Moving "
				  "to Bad Files Directory.", sourceFile);
			ngamsLogError(ngamsStatBuf);

			if (ngamsMoveFile2StatDir(ngamsGetBadDir(regP->rootDir), baseName,
						  sourceFile, status.replyData[0]) == ngamsSTAT_FAILURE)
			{
				ngamsLogError("Error moving source file: %s to bad files "
					  "directory: %s", sourceFile,
					  ngamsGetBadDir(regP->rootDir));
					 //Dave Pallot
				if (status.replyData[0])
						free(status.replyData[0]);

				return ngamsERR_FILE_MOVE_TO_BAD;
			}
				//else it will still return ngamsSTAT_SUCCESS
		}
		else if (strstr(status.message, "NGAMS_WA_BUF_DATA") != NULL)
		{
			ngamsLogWarning("File: %s could not be archived. "
					"File has been back-log buffered.",
					sourceFile);
			ngamsLogInfo(LEV1, ngamsStatBuf);

			if (ngamsMoveFile2StatDir(ngamsGetArchDir(regP->rootDir),
						  baseName, sourceFile,
						  status.replyData[0])== ngamsSTAT_FAILURE)
			{
				ngamsLogError("Error moving source file: %s to archived files "
						  "directory: %s", sourceFile,
						  ngamsGetBadDir(regP->rootDir));
				//Dave Pallot
				if (status.replyData[0])
					free(status.replyData[0]);

				return ngamsERR_FILE_MOVE_TO_ARCHIVED;
			}
				//else it will still return ngamsSTAT_SUCCESS
		}
		else //other errors, including this one: Message: 'NoneType' object has no attribute 'getMountPoint'
	    {
			ngamsLogWarning("File: %s could not be archived. Preparing for removing it from Archive Queue.", sourceFile);
			ngamsLogWarning(ngamsStatBuf);

			/*** in order to avoid excessive usage on ramdisk, remove the file immediately ***/
			ngamsLogWarning("Removing fail-to-archive file %s directly from the queue to save ramdisk space.", sourceFile);

			if (remove(sourceFile) != 0) {
				ngamsLogWarning("Error when removing fail-to-archive file %s from the queue, ramdisk may oveflow!", sourceFile);
			}
			/* may cause lots of delay when calling sync()*/
			//ngamsLogInfo(LEV1, "Syncing before Removed file: %s", sourceFile);
			sync();
			/*remove(sourceFile);*/
			ngamsLogInfo(LEV1, "Removed fail-to-archive file: %s", sourceFile);

			if (status.errorCode != ngamsSTAT_SUCCESS) {
				 //Dave Pallot
				if (status.replyData[0])
						free(status.replyData[0]);

				 return status.errorCode;//but we have to inform others that this archiving attempt is a failure
			}
			else
			{
				if (strstr(status.message, "getMountPoint") != NULL) {
					ngamsLogWarning("Actually, ErrorCode = %d", NGAMS_ER_DISK_NOT_MOUNTED);
					 //Dave Pallot
					if (status.replyData[0])
						free(status.replyData[0]);

					return NGAMS_ER_DISK_NOT_MOUNTED;
				}
				else
				{
					//Dave Pallot
					if (status.replyData[0])
						free(status.replyData[0]);

					return NGAMS_ER_UNKNOWN;
				}
			}
	    }
	}
    else
	{
		ngamsLogInfo(LEV1, ngamsStatBuf);

		/* If immediate clean up is requested, clean up immediately */
		if (regP->immediateCleanUp)
		{
			ngamsLogInfo(LEV1, "File in Archive Queue Directory: %s, "
				 "has been archived. Removing copy.", sourceFile);

			/* First rename, then remove it */
			ngamsMED_BUF  removeName;
			sprintf(removeName, "%s/.REMOVED_%s", ngamsGetQueueDir(NULL),
				baseName);
			rename(sourceFile, removeName);
			remove(removeName);
			/* may cause lots of delay when calling sync()*/
			//ngamsLogInfo(LEV1, "Syncing before Removed file: %s", sourceFile);
			sync();
			/*remove(sourceFile);*/
			ngamsLogInfo(LEV1, "Removed file: %s", sourceFile);
	    }
		else
	    {
			/* Move the file to the Archived Files Directory */
			if (ngamsMoveFile2StatDir(ngamsGetArchDir(regP->rootDir), baseName,
						  sourceFile, status.replyData[0])== ngamsSTAT_FAILURE)
			{
				ngamsLogError("Error moving source file: %s to archived files "
						  "directory: %s", sourceFile,
						  ngamsGetArchDir(regP->rootDir));
				//Dave Pallot
				if (status.replyData[0])
					free(status.replyData[0]);

				return ngamsERR_FILE_MOVE_TO_ARCHIVED;
			}
	    }
	}

	//Dave Pallot
	if (status.replyData[0])
		free(status.replyData[0]);

    //deleteEntryFromQ(regP, sourceFile);
	//printf("SUCCESS IN NGAS ARCHIVE FILE\n");

    return ngamsSTAT_SUCCESS;
}


ngamsSTAT ngamsCleanUpArchivedFiles(ngamsARCHIVE_CLIENT_REGISTRY*  regP, bool ignoreTimeout)
{
	//DIR*             dirPtr = NULL;
    //struct dirent*   dirEnt = NULL;
    //struct stat      statBuf;
    //struct timeval   timeNow;
    ngamsMED_BUF archFile = {0};
    ngamsMED_BUF xmlStatDocFile = {0};
    //ngamsHUGE_BUF    xmlStatBuf, msgBuf;
    //ngamsPAR_ARRAY   parArray;
    //ngamsSMALL_BUF   fileId, fileVer;
    //ngamsSTATUS      status;

    int retcode;
	DIR* dirPtr = NULL;
    struct dirent entry;
    struct dirent* result = NULL;

    if ((dirPtr = opendir(ngamsGetArchDir(regP->rootDir))) == NULL)
	{
		ngamsLogError("Error opening Archived Files Directory: %s", ngamsGetArchDir(regP->rootDir));
		goto errExit;
	}

    while ((retcode = readdir_r(dirPtr, &entry, &result)) == 0 && result != NULL)
	{
		/* Handle only the actual files, the XML status documents are handled
		 * together with the files.
		 */
		if ((entry.d_name[0] != '.') && (strstr(entry.d_name, ngamsARCH_CLI_STAT_EXT) == NULL))
		{
			sprintf(archFile, "%s/%s", ngamsGetArchDir(regP->rootDir), entry.d_name);
			sprintf(xmlStatDocFile, "%s%s", archFile, ngamsARCH_CLI_STAT_EXT);

			//simply delete files and continue
			if (remove(archFile) == -1)
				ngamsLogError("Remove Failed: %s", archFile);

			if (remove(xmlStatDocFile) == -1)
				ngamsLogError("Remove Failed: %s", xmlStatDocFile);

			/* Check if the time for keeping the file in the Archived Files
			 * Directory has elapsed
			 */
			/*if (stat(xmlStatDocFile, &statBuf) == -1) {
				ngamsLogError("Error querying file status for file: %s",
						  archFile);
				goto errExit;
			}
			if (!ignoreTimeout) {
				gettimeofday(&timeNow, NULL);
				if ((timeNow.tv_sec - statBuf.st_ctime) < regP->cleanUpTimeOut) {
					continue;
				}
			}
			ngamsLogInfo(LEV1, "File in Archived Files Directory: "
				 "%s could be removed (time from creation expired) "
				 "...", archFile);
			*/
			/*if (regP->checkSvrBeforeRm == 0) { //no need to obtain FileId, status, and do checking, added by Chen Wu (chen.wu@icrar.org)
				remove(archFile); //simply delete files and continue
				ngamsLogInfo(LEV1, "Removing File Archive Status XML Document file: %s", xmlStatDocFile);
				remove(xmlStatDocFile);
				continue;
			}*/
				/* Get File ID + File Version from the XML status document
			 */
			/*if (ngamsLoadFile(xmlStatDocFile, xmlStatBuf,
					  sizeof(ngamsHUGE_BUF)) == ngamsSTAT_FAILURE)
			{
			ngamsLogError("Error loading XML status document: %s",
					  xmlStatDocFile);
			goto errExit;
			}
			if (ngamsGetXmlAttr(xmlStatBuf, "FileStatus", "FileId",
					sizeof(ngamsSMALL_BUF),
					fileId) == ngamsSTAT_FAILURE)
			{
			ngamsLogError("Error error retrieving attribute "
					  "FileStatus:FileId from XML status document: %s",
					  xmlStatDocFile);
			goto errExit;
			}
			if (ngamsGetXmlAttr(xmlStatBuf, "FileStatus", "FileVersion",
					sizeof(ngamsSMALL_BUF),
					fileVer) == ngamsSTAT_FAILURE)
			{
			ngamsLogError("Error error retrieving attribute "
					  "FileStatus:FileVersion from XML status "
					  "document: %s", xmlStatDocFile);
			goto errExit;
			}
			if ((*fileId == '\0') || (*fileVer == '\0'))
			{
			if (ngamsGetXmlAttr(xmlStatBuf, "Status", "Message",
						sizeof(ngamsHUGE_BUF),
						msgBuf) == ngamsSTAT_FAILURE)
				{
				ngamsLogError("Error error retrieving attribute "
					  "Status:Message from XML status "
					  "document: %s", xmlStatDocFile);
				goto errExit;
				}
			if (strstr(msgBuf, "NGAMS_WA_BUF_DATA") != NULL)
				{
				ngamsLogError("XML Status Document related to file: %s, "
					  "indicates that files was Back-Log "
					  "Buffered. Remove status files manually.",
					  archFile);
				continue;
				}
			else
				{
				ngamsLogError("Format of XML Status Document related "
					  "to file: %s seems to be mal-formed. "
					  "Buffered. Remove status files manually.",
					  archFile);
				continue;
				}
			}*/

			/* Before deleting a file, it is checked if it is avaliable
			 * in the remote NGAS Archive
			 */
			/*ngamsResetParArray(&parArray);
			ngamsAddParAndVal(&parArray, "file_id", fileId);
			ngamsAddParAndVal(&parArray, "file_version", fileVer);
			if (ngamsGenSendCmd(regP->remoteHost, regP->remotePort,
					ngamsNO_TIME_OUT,
					ngamsCMD_CHECKFILE_STR, &parArray,
					&status) == ngamsSTAT_FAILURE)
			{
			ngamsLogError("Error sending command to NG/AMS Server: "
					  "%s/%d", regP->remoteHost, regP->remotePort);
			continue;
			}
			if (strstr(status.message, "NGAMS_INFO_FILE_OK") != NULL)
			{*/
			/* The specified file is available in the NGAS Archive,
			 * we can go ahead and delete the file from the
			 * Archived Files Directory
			 */
			/*ngamsLogInfo(LEV1, "File in Archived Files Directory: %s, "
					 "has been archived with File ID: %s and "
					 "File Version: %s. Removing copy.", archFile,
					 fileId, fileVer);
			remove(archFile);
			ngamsLogInfo(LEV1, "Removing File Archive Status XML "
					 "Document file: %s", xmlStatDocFile);
			remove(xmlStatDocFile);
			}
			else
			{
			ngamsLogInfo(LEV3, "File in Archived Files Directory: %s, "
					 "which should have been archived with File ID: "
					 "%s and File Version: %s, is not in NGAS "
					 "Archive. Keeping file in Archived Files Queue.",
					 archFile, fileId, fileVer);
			}*/
		}
	}

    closedir(dirPtr);

    return ngamsSTAT_SUCCESS;

 errExit:
    if (dirPtr != NULL)
    	closedir(dirPtr);
    // once the dirPtr is closed, the pointer dirEnt is no longer valid, so should not free it again. i.e. double deallocate memory will produce invalide pointer run-time error!
    // see examples on http://pubs.opengroup.org/onlinepubs/009695399/functions/readdir.html and http://pubs.opengroup.org/onlinepubs/009695399/functions/closedir.html
    //if (dirEnt != NULL) free(dirEnt); //added by Chen Wu (chen.wu@icrar.org)
    return ngamsSTAT_FAILURE;
}

int terminateNgams(bool force) {
	ngamsARCHIVE_CLIENT_REGISTRY* regP = &registry;
	// Dave Pallot: This should never be NULL
	/*if (&registry == NULL) {
		return ngamsSTAT_FAILURE;
	}*/
	if (force) { //force to quite, and do not wait until current archiveThreads finish gracefully
		ngamsTerminatedByDC = true;
	} else {
		pthread_mutex_lock(&(regP->archiveQueueMutex));
		while (regP->archiveQueueCount > 0) { //wait until all files in the queue are archived
			pthread_cond_wait(&(regP->archiveQueueCountCond), &(regP->archiveQueueMutex));
		}
		pthread_mutex_unlock(&(regP->archiveQueueMutex));

		pthread_mutex_lock(&(regP->sendToMCQueueMutex));
		while (regP->sendToMCQueueCount > 0) { //wait until all post content in the queue are sent to the MC Web server
			pthread_cond_wait(&(regP->sendToMCQueueCountCond), &(regP->sendToMCQueueMutex));
		}
		pthread_mutex_unlock(&(regP->sendToMCQueueMutex));

		ngamsTerminatedByDC = true;
	}

	void*  thrValPtr;
	pthread_mutex_lock(&(regP->archiveQueueMutex));

	/* Notify all the archiveThreads that are still waiting */
	pthread_cond_broadcast(&(regP->archiveQueueCond));
	// notify the sendToMCThread if it is still waiting
	pthread_cond_signal(&(regP->sendToMCQueueCond));

	pthread_mutex_unlock(&(regP->archiveQueueMutex));

	for (int n = 0; n < regP->streams; n++) {
	if (pthread_join(regP->archiveThreads[n], &thrValPtr) != 0)
		ngamsLogError("Error returned from pthread_join() for Archive Thread/%d", (n + 1));
	}

//	if (pthread_join(regP->archiveQMonThread, &thrValPtr) != 0)
//	ngamsLogError("Error returned from pthread_join() for "
//			  "Archive Queue Monitoring Thread");

	if (pthread_join(regP->cleanUpThread, &thrValPtr) != 0) {
		ngamsLogError("Error returned from pthread_join() for Clean Up Thread");
	}

	if (pthread_join(regP->sendToMCThread, &thrValPtr) != 0) {
		ngamsLogError("Error returned from pthread_join() for SendToMC Thread");
	}

	if (!force) {
		//remove files left by the cleanUpThread
		//sync();
		ngamsCleanUpArchivedFiles(regP, true);
	}

	//close berkeleydb as well
	regP->queueDbmHandle->close(regP->queueDbmHandle, 0);
	regP->procDbmHandle->close(regP->procDbmHandle, 0);
	
	ngamsLogInfo(LEV1,"ngamsArchiveClient (Server) terminating"); //chen.wu@icrar.org

	return ngamsSTAT_SUCCESS;
}


/* *
 * This used to be the main function, modified by
 * chen.wu@icrar.org
 */
int ngamsInit (const char* serverAndPort, const char* rootDir, int logLevel, int cleanTimeout, int sendToMMC, const char* MC_host, int MC_port) {
    ngamsMED_BUF      tmpVal;//, statStr, tmpPar;
    //ngamsSTAT        stat;
    //ngamsCMD         cmdCode;
    ngamsInitApi();
//
//    /* Parse input parameters */
    //ngamsARCHIVE_CLIENT_REGISTRY  registry;
    memset(&registry, 0, sizeof(ngamsARCHIVE_CLIENT_REGISTRY));

    registry.threadRunPermission = 1;
    *registry.remoteHost = '\0';
    registry.remotePort = -1;
    //safeStrCp(registry.remoteHost, "mwa01.icrar.org", sizeof(ngamsMED_BUF));
    //registry.remoteHost = "mwa01.icrar.org";
    //registry.remotePort = 7777;
    //*registry.servers = '\0';

    string sap(serverAndPort);
    size_t found = sap.find(',');
    if (found == string::npos) {
    	//if there is only one server, repteat a few more times to enable the "getNextServer", useful when the server is too busy and needs to retry for a repeatedSAP times
    	int repeatedSAP = 3; 
    	for (int i = 0; i < repeatedSAP - 1;i++) {
		sap += "," + sap;
    	}
    }

    if (safeStrCp(registry.servers, sap.c_str(), sizeof(ngamsBIG_BUF)) == ngamsSTAT_FAILURE)
    		return ngamsSTAT_FAILURE;
    ngamsParseSrvList(registry.servers);
    //*registry.rootDir = '\0';
    if (safeStrCp(registry.rootDir, rootDir, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
    		return ngamsSTAT_FAILURE;
    //*registry.mimeType = '\0';
    safeStrCp(registry.mimeType, "application/octet-stream", sizeof(ngamsSMALL_BUF));
    *registry.auth = '\0';
    registry.archiveQueuePollTime = 30.0;
    *registry.checksum = '\0';
    //registry.cleanUpTimeOut = 604800;  /* = 7 days */
    registry.cleanUpTimeOut = cleanTimeout;
    registry.sendToMMC = sendToMMC;
    if (sendToMMC) {
		if (safeStrCp(registry.mcHost, MC_host, sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
					return ngamsSTAT_FAILURE;
		registry.mcPort = MC_port;
    }
    registry.streams = 1;
    //registry.logLevel = 3;
    registry.logLevel = logLevel;
    registry.logRotate = 43200;    /* = 12:00am */
    registry.logHistory = 30;      /* = 30 days */
    registry.archiveLog = 0;       /* Don't archive log files after rotation */
    registry.checkSvrBeforeRm = 0; /* Do not check server before removing archived files added by Chen Wu (chen.wu@icrar.org)*/
    registry.verboseLevel = LEV0;
    pthread_cond_init(&(registry.archiveQueueCond), NULL);
    pthread_cond_init(&(registry.archiveQueueCountCond), NULL);
    pthread_mutex_init(&(registry.archiveQueueMutex), NULL);

    //Dave Pallot: mutex for directory
    pthread_mutex_init(&g_dirmutex, NULL);

    pthread_cond_init(&(registry.sendToMCQueueCond), NULL);
    pthread_cond_init(&(registry.sendToMCQueueCountCond), NULL);
    pthread_mutex_init(&(registry.sendToMCQueueMutex), NULL);

    ngamsResetParArray(&registry.parArray);
    strcpy(tmpVal,"QARCHIVE"); /*Use QARCHIVE command by default*/
    safeStrCp(registry.serverCmd, tmpVal,sizeof(ngamsMED_BUF));
    ngamsTerminatedByDC = false;
    return ngamsServe(&registry);

//    for (i = 1; i < argc; i++)
//	{
//	if (safeStrCp(tmpPar, argv[i], sizeof(ngamsMED_BUF)) ==
//	    ngamsSTAT_FAILURE)
//	    return ngamsSTAT_FAILURE;
//	ngamsToUpper(tmpPar);
//	if (strcmp(tmpPar, "-ARCHIVELOG") == 0)
//	    registry.archiveLog = 1;
//	else if (strcmp(tmpPar, "-SKIPSERVERCHECHKING") == 0) { /*added by Chen Wu (chen.wu@icrar.org)*/
//	    registry.checkSvrBeforeRm = 0;
//	    }
//	else if (strcmp(tmpPar, "-ARCHIVEPAR") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (ngamsSplitParVal(argv[i], tmpPar, tmpVal) == ngamsSTAT_FAILURE)
//		      goto correctUsage;
//	    ngamsAddParAndVal(&registry.parArray, tmpPar, tmpVal);
//	    }
//	else if (strcmp(tmpPar, "-AUTH") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.auth, argv[i],
//			  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    }
//	else if (strcmp(tmpPar, "-CHECKSUM") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.checksum, argv[i],
//			  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    }
//	else if (strcmp(tmpPar, "-CLEANUPTIMEOUT") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.cleanUpTimeOut = atoi(argv[i]);
//	    if (registry.cleanUpTimeOut == 0) registry.immediateCleanUp = 1;
//	    }
//	else if ((strcmp(tmpPar, "-H") == 0) || (strcmp(tmpPar, "-HELP") == 0))
//	    goto correctUsage;
//	else if (strcmp(tmpPar, "-HOST") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.remoteHost, argv[i],
//			  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    }
//	else if (strcmp(tmpPar, "-PORT") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.remotePort = atoi(argv[i]);
//	    }
//	else if (strcmp(tmpPar, "-LICENSE") == 0)
//	    {
//	    printf("%s", "Lesser GPL");
//	    exit(0);
//	    }
//	else if (strcmp(tmpPar, "-LOGHISTORY") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.logHistory = atoi(argv[i]);
//	    }
//	else if (strcmp(tmpPar, "-LOGLEVEL") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.logLevel = atoi(argv[i]);
//	    }
//	else if (strcmp(tmpPar, "-LOGROTATE") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.logRotate = atoi(argv[i]);
//	    }
//	else if (strcmp(tmpPar, "-MIMETYPE") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.mimeType, argv[i],
//			  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    }
//	else if (strcmp(tmpPar, "-POLLTIME") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.archiveQueuePollTime = atof(argv[i]);
//	    /* Put restriction for now on the polling period */
//	    if (registry.archiveQueuePollTime < 0.01)
//		registry.archiveQueuePollTime = 0.010;
//	    }
//	else if (strcmp(tmpPar, "-ROOTDIR") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.rootDir, argv[i],
//			  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    if (getLastChar(registry.rootDir) == '/')
//		registry.rootDir[strlen(registry.rootDir) - 1] = '\0';
//	    }
//	else if (strcmp(tmpPar, "-SERVERS") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.servers, argv[i],
//			  sizeof(ngamsBIG_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    }
//	else if (strcmp(tmpPar, "-SERVERCMD") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    if (safeStrCp(registry.serverCmd, argv[i],
//			  sizeof(ngamsMED_BUF)) == ngamsSTAT_FAILURE)
//		return ngamsSTAT_FAILURE;
//	    if (strcmp(registry.serverCmd, ngamsCMD_ARCHIVE_STR) != 0) {//assuming this is QARCHIVE, copied from line 810
//	    		registry.checkSvrBeforeRm = 0;//added by Chen Wu (chen.wu@icrar.org)
//            }
//            printf(">>>>>  %s\n",registry.serverCmd);
//	    }
//	else if (strcmp(tmpPar, "-STREAMS") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.streams = atoi(argv[i]);
//	    }
//	else if (strcmp(tmpPar, "-V") == 0)
//	    {
//	    if (++i == argc) goto correctUsage;
//	    if (*argv[i] == '-') goto correctUsage;
//	    registry.verboseLevel = atoi(argv[i]);
//	    }
//	else if (strcmp(tmpPar, "-VERSION") == 0)
//	    {
//	    printf("%s\n", ngamsVersion());
//	    exit(0);
//	    }
//	else
//	    {
//	    printf("\n\nFound illegal command line parameter: %s\n", tmpPar);
//	    goto correctUsage;
//	    }
//	}

    /* end-for (int i = 1; i < argc; i++) */
//    if ((((*registry.remoteHost == '\0') || (registry.remotePort == -1)) &&
//	 (*registry.servers == '\0')) || (*registry.rootDir == '\0'))
//	goto correctUsage;
//    if (*registry.servers != '\0')
//	{
//	if (ngamsParseSrvList(registry.servers) != ngamsSTAT_SUCCESS)
//	    goto correctUsage;
//	}

    /* Set Authorization key if specified */
    //if (*registry.auth != '\0') ngamsSetAuthorization(registry.auth);

    /* Initialize + start to serve */
//    if ((stat = ngamsServe(&registry)) != ngamsSTAT_SUCCESS)
//	goto errExit;

    /* Initialize + start to serve */

    //exit(0); //if everything is okay, continue with the current (main) thread


// correctUsage:
//    ngamsCorrectUsage();
//    exit(1);
//    return(1);

// errExit:
//    ngamsStat2Str(stat, statStr);
//    if (*statStr)
//	printf("\nError ocurred initializing NG/AMS Archive Client: "
//	       "\n\n%s\n\n", statStr);
//    else
//	printf("\n\nError ocurred initializing NG/AMS Archive Client!\n\n");
//    exit(1);
}




/* EOF */
