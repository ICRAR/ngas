#ifndef ngams_H
#define ngams_H

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
 * "@(#) $Id: ngams.h,v 1.17 2009/06/23 08:55:37 achen Exp $"
 *
 * who       when      what
 * --------  --------  ----------------------------------------------
 * jknudstr  30/08/01  created
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>

/* Various macros.
 */
#define ngamsARHIVE_REQ_MT        "ngas/archive-request"
#define ngamsUSER_AGENT           "NG/AMS C-API"
#define ngamsCFG_REF              "--CFG--"
#define ngamsNG_LOG_REF           "--NG--LOG--"
#define ngamsHTTP_MAX_HDRS        32
#define ngamsMAX_REPLY_DATA_BUFS  32
#define ngamsNO_TIME_OUT          -1
#define ngamsNEWLINE              "\n"
#define ngamsSTD_PERMS            0644   /* rw-r--r-- */
#define ngamsSTD_DIR_PERMS        0755   /* rwxr-xr-x */
#define ngamsMAX_GEN_PARS         16
#define ngamsMAX_SRVS             32
#define ngamsMAX_SOCKS            64

/* Properties for logging
 */
#define ngamsLOG_ROT_PREFIX       "LOG-ROTATE"


/* Data types
 */
typedef char             ngamsSMALL_BUF[128];
typedef char             ngamsMED_BUF[256];
typedef char             ngamsBIG_BUF[512];
typedef char             ngamsHUGE_BUF[16384];
typedef char             ngamsHTTP_HDR[128][256];
typedef signed long long ngamsDATA_LEN;

/* NG/AMS commands
 */
typedef enum {
    ngamsCMD_ARCHIVE,
    ngamsCMD_CHECKFILE,
    ngamsCMD_CLONE,
    ngamsCMD_DISCARD,
    ngamsCMD_EXIT,
    ngamsCMD_INIT,
    ngamsCMD_LABEL,
    ngamsCMD_ONLINE,
    ngamsCMD_OFFLINE,
    ngamsCMD_QARCHIVE,
    ngamsCMD_REGISTER,
    ngamsCMD_REMDISK,
    ngamsCMD_REMFILE,
    ngamsCMD_RETRIEVE,
    ngamsCMD_STATUS,
    ngamsCMD_SUBSCRIBE,
    ngamsCMD_UNSUBSCRIBE
} ngamsCMD;
#define ngamsCMD_ARCHIVE_STR      "ARCHIVE"
#define ngamsCMD_CHECKFILE_STR    "CHECKFILE"
#define ngamsCMD_CLONE_STR        "CLONE"
#define ngamsCMD_DISCARD_STR      "DISCARD"
#define ngamsCMD_EXIT_STR         "EXIT"
#define ngamsCMD_INIT_STR         "INIT"
#define ngamsCMD_LABEL_STR        "LABEL"
#define ngamsCMD_ONLINE_STR       "ONLINE"
#define ngamsCMD_OFFLINE_STR      "OFFLINE"
#define ngamsCMD_QARCHIVE_STR     "QARCHIVE"
#define ngamsCMD_REGISTER_STR     "REGISTER"
#define ngamsCMD_REMDISK_STR      "REMDISK"
#define ngamsCMD_REMFILE_STR      "REMFILE"
#define ngamsCMD_RETRIEVE_STR     "RETRIEVE"
#define ngamsCMD_STATUS_STR       "STATUS"
#define ngamsCMD_SUBSCRIBE_STR    "SUBSCRIBE"
#define ngamsCMD_UNSUBSCRIBE_STR  "UNSUBSCRIBE"


/* Return status
 */
#define NGAMS_SUCCESS             "SUCCESS"
#define NGAMS_FAILURE             "FAILURE"


/* Boolean data type.
 */
#ifndef TRUE
typedef enum {
    FALSE = 0,
    TRUE = 1
} ngamsBOOL;
#endif


/* Log Levels */
typedef enum {
    LEV0 = 0,
    LEV1,
    LEV2,
    LEV3,
    LEV4,
    LEV5
} ngamsLOG_LEVEL;

#define ngamsLOG_FILE_ENV    "NGAMS_LOG_FILE"
#define ngamsLOG_LEVEL_ENV   "NGAMS_LOG_LEVEL"
#define ngamsLOG_VERBOSE_ENV "NGAMS_VERBOSE_LEVEL"


/* NG/AMS C API Error codes
 */
typedef enum {
    ngamsSTAT_FAILURE      = 1,
    ngamsSTAT_SUCCESS      = 0,

    ngamsERR_HOST          = -1,
    ngamsERR_SOCK          = -2,
    ngamsERR_CON           = -3,

    ngamsERR_COM           = -4,
    ngamsERR_TIMEOUT       = -5,

    ngamsERR_WR_HD         = -100,
    ngamsERR_WR_DATA       = -101,
    ngamsERR_RD_DATA       = -102,
    ngamsERR_INV_REPLY     = -103,

    ngamsERR_FILE          = -200,
    ngamsERR_ALLOC_MEM     = -201,

    ngamsERR_UNKNOWN_STAT  = -1000,
    ngamsERR_UNKNOWN_CMD   = -1001,
    ngamsERR_INV_TARG_FILE = -1002,
    ngamsERR_INV_PARS      = -1003,

    ngamsERR_OPEN_LOG_FILE = -2000,

    ngamsSRV_OK            =  200,
    ngamsSRV_REDIRECT      =  303,

    ngamsSRV_INV_QUERY     =  400,

    /* A few important error codes from ngams/ngamsData/ngamsLogDef.xml
     * are here defined explicitly since they are used by one of the client
     * applications.
     *
     * IMPL: It would be better to be able to convert the NG/AMS XMl Log
     * Definition at compile time so that all the log definitions become
     * available from within the C-API.
     */
    NGAMS_ER_DAPI_BAD_FILE = 4003,
    NGAMS_WA_BUF_DATA      = 4022
} ngamsSTAT;


/* Type of server context switching.
 */
typedef enum {
    ngamsCTX_SWITCH_RANDOM  = 0,
    ngamsCTX_SWITCH_CYCLIC  = 1
} ngamsCTX_SWITCH_SCHEME;


/* Structure to contain complete status from NG/AMS
 */
typedef struct {
    ngamsSMALL_BUF   date;
    ngamsSTAT        errorCode;
    ngamsSMALL_BUF   hostId;
    ngamsHUGE_BUF    message;
    ngamsSMALL_BUF   status;
    ngamsSMALL_BUF   state;
    ngamsSMALL_BUF   subState;
    ngamsSMALL_BUF   version;
    char*            replyData[ngamsMAX_REPLY_DATA_BUFS];
} ngamsSTATUS;


/* Structure to hold server contact parameters.
 */
typedef struct {
    ngamsSMALL_BUF           id;
    ngamsCTX_SWITCH_SCHEME   scheme;
    ngamsMED_BUF             hosts[ngamsMAX_SRVS];
    int                      ports[ngamsMAX_SRVS];
    int                      numberOfSrvs ;
    int                      srvIdx;
} ngamsSRV_INFO;


/* Structure to hold command parameters.
 */
typedef struct {
    ngamsMED_BUF    parArray[ngamsMAX_GEN_PARS];
    ngamsMED_BUF    valArray[ngamsMAX_GEN_PARS];
    int             idx;
} ngamsPAR_ARRAY;


/* Union to handle the reference to the data.
 */
typedef struct {
    char*   pdata;
    int     fd;
} ngamsHTTP_DATA;


/* Structure to hold the HTTP response status.
 */
typedef struct {
    ngamsSMALL_BUF   version;
    int              status;
    ngamsHUGE_BUF    message;
} ngamsHTTP_RESP;


/* Function prototypes
 */
void ngamsAddParAndVal(ngamsPAR_ARRAY*  parArray,
		       const char*      par,
		       const char*      val);

char* ngamsGetParVal(ngamsPAR_ARRAY*    parArray,
		     const char*        par);

ngamsSTAT ngamsArchive(const char*      host,
                       const int        port,
		       const float      timeoutSecs,
                       const char*      fileUri,
                       const char*      mimeType,
		       const int        noVersioning,
                       const int        wait,
                       ngamsSTATUS*     status);

ngamsSTAT ngamsQArchive(const char*      host,
                       const int        port,
		       const float      timeoutSecs,
                       const char*      fileUri,
                       const char*      mimeType,
		       const int        noVersioning,
                       const int        wait,
                       ngamsSTATUS*     status);

ngamsSTAT ngamsArchiveFromMem(const char*   host,
			      const int     port,
			      const float   timeoutSecs,
			      const char*   fileUri,
			      const char*   bufPtr,
			      const int     size,
			      const char*   mimeType,
			      const int     noVersioning,
			      const int     wait,
			      ngamsSTATUS*  status);

ngamsSTAT ngamsCleanUpRotLogFiles(const ngamsLOG_LEVEL  tmpLogLevel);


ngamsSTAT ngamsClone(const char*   host,
                     const int     port,
		     const float   timeoutSecs,
		     const char*   fileId,
		     const int     fileVersion,
		     const char*   diskId,
		     const char*   targetDiskId,
		     const int     wait,
		     ngamsSTATUS*  status);

ngamsSTAT ngamsCmd2No(const ngamsSMALL_BUF    cmdStr,
                      ngamsCMD*               cmdCode);

ngamsSTAT ngamsCmd2Str(const ngamsCMD    cmdCode,
                       ngamsSMALL_BUF    cmdStr);

void ngamsDumpStatStdout(const ngamsSTATUS*   status);

void ngamsEncodeUrlVal(const char*  urlVal,
		       const int    skipScheme,
		       char*        encodedUrl);

void ngamsFreeStatus(ngamsSTATUS*   status);

void ngamsGenIsoTime(const int      prec,
		     ngamsMED_BUF   dayTime);

ngamsSTAT ngamsGenSendCmd(const char*            host,
			  const int              port,
			  const float            timeoutSecs,
			  const char*            cmd,
			  const ngamsPAR_ARRAY*  parArray,
			  ngamsSTATUS*           status);

ngamsSTAT ngamsGenSendData(const char*            host,
			   const int              port,
			   const ngamsCMD         cmdCode,
			   const float            timeoutSecs,
			   const char*            fileUri,
			   const char*            mimeType,
			   const ngamsPAR_ARRAY*  parArray,
			   ngamsSTATUS*           status);

char*  ngamsGetAuthorization(void);

ngamsSTAT ngamsGetHostName(ngamsSMALL_BUF hostName);

ngamsSTAT ngamsGetHttpHdrEntry(ngamsHTTP_HDR   httpHdr,
			       const char*     hdrName,
			       const char*     fieldName,
			       ngamsMED_BUF    value);

ngamsSRV_INFO*  _ngamsSrvInfo();

ngamsSTAT ngamsGetXmlAttr(const char*   xmlDoc,
			  const char*   pt,
			  const char*   attr,
			  const int     maxValLen,
			  char*         value);

ngamsSTAT ngamsExit(const char*     host,
                    const int       port,
		    const float     timeoutSecs,
                    const int       wait,
                    ngamsSTATUS*    status);

int ngamsHttpGet(const char*         host,
		 const int           port,
		 const char*         userAgent,
		 const char*         path,
		 const int           receiveData,
		 ngamsHTTP_DATA*     repDataRef,
		 ngamsDATA_LEN*      dataLen,
		 ngamsHTTP_RESP*     httpResp,
		 ngamsHTTP_HDR       httpHdr);

void ngamsInitApi(void);

void ngamsInitStatus(ngamsSTATUS*   status);

int ngamsIsDir(const char*    filename);

ngamsSTAT ngamsLabel(const char*      host,
                     const int        port,
		     const float      timeoutSecs,
                     const char*      slotId,
                     ngamsSTATUS*     status);

char* ngamsLicense(void);

int ngamsLogCodeInStatus(const ngamsSTATUS*  status,
			 const char*         errId);

char* ngamsManPage(void);

ngamsSTAT ngamsOffline(const char*       host,
                       const int         port,
		       const float       timeoutSecs,
		       const int         force,
                       const int         wait,
                       ngamsSTATUS*      status);

ngamsSTAT ngamsOnline(const char*    host,
                      const int      port,
		      const float    timeoutSecs,
                      const int      wait,
                      ngamsSTATUS*   status);

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
		  ngamsHTTP_HDR            httpHdr);

int ngamsHttpPostOpen(const char*             host,
		      const int               port,
		      const char*             userAgent,
		      const char*             path,
		      const char*             mimeType,
		      const char*             contentDisp,
		      const ngamsDATA_LEN     dataLen);

int ngamsHttpPostSend(int                      sockFd,
		      const char*              srcFilename,
		      const char*              data,
		      const ngamsDATA_LEN      dataLen);

int ngamsHttpPostClose(int                 sockFd,
		       ngamsHTTP_DATA*     repDataRef,
		       ngamsDATA_LEN*      repDataLen,
		       ngamsHTTP_RESP*     httpResp,
		       ngamsHTTP_HDR       httpHdr);


ngamsSTAT ngamsLoadFile(const char*   filename,
			char*         buf,
			const int     maxSize);

void ngamsLog_v(const char*            type,
		const ngamsLOG_LEVEL   level,
		const char*            format,
		va_list                vaParList);

void ngamsLogAlert(const char*  format, ...);

void ngamsLogCrit(const char*   format, ...);

void ngamsLogDebug(const char*  format, ...);

void ngamsLogEmerg(const char*  format, ...);

ngamsSTAT ngamsLogFileRotate(const ngamsLOG_LEVEL  tmpLogLevel,
			     const ngamsMED_BUF    systemId,
			     ngamsMED_BUF          rotatedLogFile);

void ngamsLogInfo(const ngamsLOG_LEVEL level,
		  const char*          format, ...);

void ngamsLogError(const char*   format, ...);

void ngamsLogNotice(const char*  format, ...);

void ngamsLogWarning(const char* format, ...);

ngamsSTAT ngamsPrepLog(const ngamsMED_BUF   logFile,
		       const ngamsLOG_LEVEL logLevel,
		       const int            logRotate,
		       const int            logHistory);

ngamsSTAT ngamsParseSrvListId(const char*   listId,
			      const char*   servers);

ngamsSTAT ngamsParseSrvList(const char*  servers);

ngamsSTAT ngamsRegister(const char*    host,
			const int      port,
			const float    timeoutSecs,
			const char*    path,
			const int      wait,
			ngamsSTATUS*   status);

ngamsSTAT ngamsRemDisk(const char*     host,
		       const int       port,
		       const float     timeoutSecs,
		       const char*     diskId,
		       const int       execute,
		       ngamsSTATUS*    status);

ngamsSTAT ngamsRemFile(const char*     host,
		       const int       port,
		       const float     timeoutSecs,
		       const char*     diskId,
		       const char*     fileId,
		       const int       fileVersion,
		       const int       execute,
		       ngamsSTATUS*    status);

void ngamsResetParArray(ngamsPAR_ARRAY*  parArray);

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
			    ngamsDATA_LEN*      repDataLen,
			    ngamsSTATUS*        status);

ngamsSTAT _ngamsRetrieve2File(const char*       host,
			      const int         port,
			      const float       timeoutSecs,
			      const char*       fileRef,
			      const int         fileVersion,
			      const char*       processing,
			      const char*       processingPars,
			      const char*       targetFile,
			      ngamsMED_BUF      finalTargetFile,
			      ngamsSTATUS*      status,
			      const int         internal,
			      const char*       hostId);

ngamsSTAT ngamsRetrieve2File(const char*        host,
                             const int          port,
			     const float        timeoutSecs,
                             const char*        fileId,
                             const int          fileVersion,
                             const char*        processing,
			     const char*        processingPars,
                             const char*        targetFile,
			     ngamsMED_BUF       finalTargetFile,
                             ngamsSTATUS*       status);

ngamsSTAT ngamsGenRetrieve2File(const char*            host,
				const int              port,
				const float            timeoutSecs,
				const ngamsCMD         cmdCode,
				const ngamsPAR_ARRAY*  parArray,
				const char*            targetFile,
				ngamsMED_BUF           finalTargetFile,
				ngamsSTATUS*           status);

ngamsSTAT ngamsSaveInFile(const char*  filename,
			  const char*  buf);

char getLastChar(const char*  str);

ngamsSTAT safeStrNCp(char*        dest,
		     const char*  src,
		     const int    len,
		     const int    maxLen);

ngamsSTAT safeStrCp(char*        dest,
		    const char*  src,
		    const int    maxLen);

void ngamsSleep(const float sleepTime);

int ngamsSuccess(const ngamsSTATUS*  status);

void _setReplyTimeoutFromFloat(const float timeoutSecs);

void ngamsSetAuthorization(const char*  authUserPass);

void ngamsSetVerboseLevel(const ngamsLOG_LEVEL level);

void ngamsInitLogLevel();

ngamsSTAT ngamsSplitFilename(const char*    complPath,
			     ngamsMED_BUF   path,
			     ngamsMED_BUF   filename);

ngamsSTAT ngamsSplitSrvAddr(const char*  srvAddr,
                            char*        host,
                            int*         port);

ngamsSTAT ngamsSplitParVal(const char*  parVal,
                           char*        par,
                           char*        val);

ngamsSTAT ngamsStat2Str(const ngamsSTAT  statNo,
                        ngamsMED_BUF     statStr);

ngamsSTAT ngamsStatus(const char*         host,
                      const int           port,
		      const float         timeoutSecs,
                      ngamsSTATUS*        status);

char* ngamsStrRStr(const char*  str,
		   const char*  pat);

ngamsSTAT ngamsSubscribe(const char*       host,
			 const int         port,
			 const float       timeoutSecs,
			 const char*       url,
			 const int         priority,
			 const char*       startDate,
			 const char*       filterPlugIn,
			 const char*       filterPlugInPars,
			 ngamsSTATUS*      status);

void ngamsToUpper(char*  str);

void ngamsTrimString(char*        str,
		     const char*  trimChars);

int ngamsSplitString(char*          str,
		     const char*    splitPat,
		     const int      maxSubStr,
		     ngamsMED_BUF   subStr[],
		     int*           noOfSubStr);

ngamsSTAT ngamsUnsubscribe(const char*      host,
			   const int        port,
			   const float      timeoutSecs,
			   const char*      url,
			   ngamsSTATUS*     status);

char* ngamsVersion(void);


#ifdef __cplusplus
}
#endif

#endif /*!ngams_H*/
