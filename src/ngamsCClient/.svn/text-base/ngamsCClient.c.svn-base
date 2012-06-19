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
 * "@(#) $Id: ngamsCClient.c,v 1.6 2009/06/02 07:48:52 awicenec Exp $"
 *
 * who       when      what
 * --------  --------  --------------------------------------------------
 * jknudstr  30/08/01  created
 */

/************************************************************************
 *   NAME
 *   ngamsCClient - Application to communicate with the NG/AMS Server.
 *
 *   SYNOPSIS
 *   Invoke the tool without command parameters to get a help page.
 *
 *   DESCRIPTION
 *   Application to interact with the NG/AMS Server. It is possible
 *   to send all the commands, and in particular possible to archive
 *   and retrieve data.
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

/* static char *rcsId="@(#) $Id: ngamsCClient.c,v 1.6 2009/06/02 07:48:52 awicenec Exp $"; */
/* static void *use_rcsId = ((void)&use_rcsId,(void *) &rcsId); */

#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "ngams.h"


void correctUsage()
{
    printf("> ngamsCClient %s", (ngamsManPage() + 1));
}


/* Main
 */
int main (int argc, char*  argv[])
{
    int              dumpStatus = 0, wait = 1, port = -1, i;
    int              fileVersion = -1, noVersioning = 0, force = 0;
    int              execute = 0, priority = 10, repeat = 1, count;
    float            timeOut = ngamsNO_TIME_OUT;
    ngamsCMD         cmdCode;
    ngamsPAR_ARRAY   parArray;
    ngamsMED_BUF     tmpPar, host, servers, repHost, tmpBuf;
    ngamsMED_BUF     outputFile, finalOutputFile;
    ngamsMED_BUF     fileUri, fileId, mimeType, slotId, path;
    ngamsMED_BUF     diskId, processing, processingPars, url, startDate;
    ngamsMED_BUF     internal, hostId;
    ngamsMED_BUF     filterPlugIn, plugInPars, targetDiskId, authorization;
    ngamsSMALL_BUF   command, tmpDynPar;
    ngamsSTAT        stat = 0;
    ngamsSTATUS      status;


    ngamsInitApi();
    ngamsInitStatus(&status);
    ngamsResetParArray(&parArray);

    /* IMPL: Base the complete parameter handling on the generic par. array.
     *       still, there can be specific functions to handle certain commands
     *       requiring special handling e.g. ARCHIVE and RETRIEVE. Basically
     *       only parameters for these two commands need to have a special
     *       parsing.
     */
    *host = *servers = *repHost = '\0';
    *command = *outputFile = *fileUri = *fileId = *diskId = '\0';
    *mimeType = *slotId = *path = *processing = *processingPars = '\0';
    *url = *startDate = *filterPlugIn = *plugInPars = '\0';
    *hostId = *internal = *targetDiskId = *authorization= '\0';
    for (i = 1; i < argc; i++)
	{
	strcpy(tmpPar, argv[i]);
	ngamsToUpper(tmpPar);
	if (strcmp(tmpPar, "-AUTH") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(authorization, argv[i]);
	    }
	else if (strcmp(tmpPar, "-CFG") == 0)
	    strcpy(fileId, ngamsCFG_REF);
	else if (strcmp(tmpPar, "-CMD") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(command, argv[i]);
	    if (ngamsCmd2No(command, &cmdCode) != ngamsSTAT_SUCCESS)
		goto correctUsage;
	    }
	else if (strcmp(tmpPar, "-DISKID") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(diskId, argv[i]);
	    ngamsAddParAndVal(&parArray, "disk_id", argv[i]);
	    }
	else if (strcmp(tmpPar, "-EXECUTE") == 0)
	    {
	    execute = 1;
	    ngamsAddParAndVal(&parArray, "execute", "1");
	    }
	else if (strcmp(tmpPar, "-FILEACCESS") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    ngamsAddParAndVal(&parArray, "file_access", argv[i]);
	    }
	else if (strcmp(tmpPar, "-FILEID") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(fileId, argv[i]);
	    ngamsAddParAndVal(&parArray, "file_id", argv[i]);
	    }
	else if (strcmp(tmpPar, "-FILEURI") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(fileUri, argv[i]);
	    ngamsAddParAndVal(&parArray, "file_uri", argv[i]);
	    }
	else if (strcmp(tmpPar, "-FILEVERSION") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    fileVersion = atoi(argv[i]);
	    ngamsAddParAndVal(&parArray, "file_version", argv[i]);
	    }
	else if (strcmp(tmpPar, "-FILTERPLUGIN") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(filterPlugIn, argv[i]);
	    ngamsAddParAndVal(&parArray, "filter_plug_in", argv[i]);
	    }
	else if (strcmp(tmpPar, "-FILTERPLUGINPARS") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(plugInPars, argv[i]);
	    ngamsAddParAndVal(&parArray, "plug_in_pars", argv[i]);
	    }
	else if (strcmp(tmpPar, "-FORCE") == 0)
	    {
	    force = 1;
	    ngamsAddParAndVal(&parArray, "force", "1");
	    }
	else if (strcmp(tmpPar, "-HOST") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(host, argv[i]);
	    }
	else if (strcmp(tmpPar, "-HOSTID") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(hostId, argv[i]);
	    ngamsAddParAndVal(&parArray, "host_id", argv[i]);
	    }
	else if (strcmp(tmpPar, "-INTERNAL") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(internal, argv[i]);
	    }
	else if (strcmp(tmpPar, "-LICENSE") == 0)
	    {
	    printf("%s", ngamsLicense());
	    exit(0);
	    }
	else if (strcmp(tmpPar, "-MIMETYPE") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(mimeType, argv[i]);
	    }
	else if (strcmp(tmpPar, "-NGLOG") == 0)
	    strcpy(fileId, ngamsNG_LOG_REF);
	else if (strcmp(tmpPar, "-NOTIFEMAIL") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    ngamsAddParAndVal(&parArray, "notif_email", argv[i]);
	    }
	else if (strcmp(tmpPar, "-NOVERSIONING") == 0)
	    noVersioning = 1;
	else if (strcmp(tmpPar, "-NOWAIT") == 0)
	    {
	    wait = 0;
	    ngamsAddParAndVal(&parArray, "wait", "0");
	    }
	else if (strcmp(tmpPar, "-OUTPUTFILE") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    strcpy(outputFile, argv[i]);
	    }
	else if (strcmp(tmpPar, "-PATH") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(path, argv[i]);
	    ngamsAddParAndVal(&parArray, "path", argv[i]);
	    }
	else if (strcmp(tmpPar, "-PORT") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    port = atoi(argv[i]);
	    }
	else if (strcmp(tmpPar, "-PRIORITY") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    priority = atoi(argv[i]);
	    ngamsAddParAndVal(&parArray, "priority", argv[i]);
	    }
	else if (strcmp(tmpPar, "-PROCESSING") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(processing, argv[i]);
	    }
	else if (strcmp(tmpPar, "-PROCESSINGPARS") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(processingPars, argv[i]);
	    }
	else if (strcmp(tmpPar, "-REPEAT") == 0)
	    {
	    /* IMPL: Not yet documented. */
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    repeat = atoi(argv[i]);
	    }
	else if (strcmp(tmpPar, "-REQUESTID") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    ngamsAddParAndVal(&parArray, "request_id", argv[i]);
	    }
	else if (strcmp(tmpPar, "-SLOTID") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(slotId, argv[i]);
	    ngamsAddParAndVal(&parArray, "slot_id", argv[i]);
	    }
	else if (strcmp(tmpPar, "-STATUS") == 0)
	    dumpStatus = 1;
	else if (strcmp(tmpPar, "-STARTDATE") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(startDate, argv[i]);
	    ngamsAddParAndVal(&parArray, "start_date", argv[i]);
	    }
	else if (strcmp(tmpPar, "-SERVERS") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(servers, argv[i]);
	    if (ngamsParseSrvListId("", servers) != ngamsSTAT_SUCCESS)
		goto correctUsage;
	    }
	else if (strcmp(tmpPar, "-TARGETDISKID") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(targetDiskId, argv[i]);
	    ngamsAddParAndVal(&parArray, "target_disk_id", argv[i]);
	    }
	else if (strcmp(tmpPar, "-TIMEOUT") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    timeOut = atof(argv[i]);
	    }
	else if (strcmp(tmpPar, "-V") == 0)
	    printf("\nNOTE: Command line option -v is not yet implemented!\n");
	else if (strcmp(tmpPar, "-VERSION") == 0)
	    {
	    printf("%s\n", ngamsVersion());
	    exit(0);
	    }
	else if (strcmp(tmpPar, "-URL") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(url, argv[i]);
	    ngamsAddParAndVal(&parArray, "url", argv[i]);
	    }
	else if (strcmp(tmpPar, "-PAR") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    strcpy(tmpDynPar, argv[i]);
	    }
	else if (strcmp(tmpPar, "-VAL") == 0)
	    {
	    if (++i == argc) goto correctUsage;
	    if (*argv[i] == '-') goto correctUsage;
	    ngamsAddParAndVal(&parArray, tmpDynPar, argv[i]);
	    }
	else if ((strcmp(tmpPar, "-H") == 0) || (strcmp(tmpPar, "-HELP") == 0))
	    goto correctUsage;
	else
	    {
	    printf("\n\nFound illegal command line parameter: %s\n", tmpPar);
	    goto correctUsage;
	    }
	}   /* end-for (int i = 1; i < argc; i++) */
    if ((*host == '\0') && (*servers == '\0'))
	{
	if (getenv("HOSTNAME") != NULL)
	    strcpy(host, getenv("HOSTNAME"));
	else
	    goto correctUsage;
	}
    if (((*host == '\0') && (port == -1) && (*servers == '\0')) ||
	(*command == '\0'))
	goto correctUsage;
    /* Send timeout with the request */
    if (timeOut != -1)
	sprintf(tmpBuf, "%d", (int)(timeOut + 0.5));
    else
	sprintf(tmpBuf, "-1");
    ngamsAddParAndVal(&parArray, "time_out", tmpBuf);

    /* Authorization defined? */
    if (*authorization) ngamsSetAuthorization(authorization);

    /* Execute command - we use the specific command functions here to test
     * them + the generic one  in case generic parameter + value arrays were
     * specified.
     */
    for (count = 1; count <= repeat; count += 1)
	{
	if (repeat > 1) printf("\nCommand repetition counter: %d\n\n", count);
	if ((cmdCode != ngamsCMD_ARCHIVE) && (cmdCode != ngamsCMD_RETRIEVE) &&
			(cmdCode != ngamsCMD_QARCHIVE))
	    stat = ngamsGenSendCmd(host, port, timeOut, command, &parArray,
				   &status);
	else
	    {
	    switch (cmdCode)
		{
		case ngamsCMD_ARCHIVE:
		    stat = ngamsArchive(host, port, timeOut, fileUri, mimeType,
					noVersioning, wait, &status);
		    break;
		case ngamsCMD_QARCHIVE:
		    stat = ngamsQArchive(host, port, timeOut, fileUri, mimeType,
					noVersioning, wait, &status);
			break;
		default:
		    /* ngamsCMD_RETRIEVE */
		    if (*internal != '\0')
			stat = _ngamsRetrieve2File(host, port, timeOut,
						   internal,
						   fileVersion, processing,
						   processingPars, outputFile,
						   finalOutputFile, &status,
						   1, hostId);
		    else if ((strcmp(fileId, ngamsCFG_REF) == 0) ||
			     (strcmp(fileId, ngamsNG_LOG_REF) == 0))
			stat = _ngamsRetrieve2File(host, port, timeOut, fileId,
						   fileVersion, processing,
						   processingPars, outputFile,
						   finalOutputFile,  &status,
						   1, hostId);
		    else
			stat = ngamsRetrieve2File(host, port, timeOut, fileId,
						  fileVersion, processing,
						  processingPars, outputFile,
						  finalOutputFile, &status);
		    break;
		}
	    }

	/* Dump the NG/AMS status if requested */
	if (dumpStatus && (status.replyData[0] != NULL))
	    printf("%s\n", status.replyData[0]);
	else
	    {
	    if (*status.hostId != '\0')
		strcpy(repHost, status.hostId);
	    else if (*host != '\0')
		strcpy(repHost, host);
	    printf("\nStatus of request:\n\n");
	    if (*repHost != '\0')
		printf("Host:           %s\n", repHost);
	    if (port != -1)
		printf("Port:           %d\n", port);
	    printf("Command:        %s\n", command);
	    ngamsDumpStatStdout(&status);
	    }

	ngamsFreeStatus(&status);
	}   /* end-for (count = 0; count < repeat; count += 1) */

    if (stat != ngamsSTAT_SUCCESS) exit(1);
    exit(0);
    return(0);


 correctUsage:
    ngamsFreeStatus(&status);
    correctUsage();
    exit(1);
    return(1);
}


/* EOF */
