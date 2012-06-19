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
 * "@(#) $Id: ngamsCClientX.c,v 1.4 2008/08/19 20:51:50 jknudstr Exp $" 
 *
 * who       when      what
 * --------  --------  --------------------------------------------------
 * jknudstr  18/05/05  created
 */

/************************************************************************
 *   NAME
 *   ngamsCClientX - Generic application to communicate with the NG/AMS Server.
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

/* static char *rcsId="@(#) $Id: ngamsCClientX.c,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"; */
/* static void *use_rcsId = ((void)&use_rcsId,(void *) &rcsId); */

#include <strings.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "ngams.h"

char *_ngamsManPageX(void);


void _correctUsage()
{
    printf("> ngamsCClientX %s", (_ngamsManPageX() + 1));
}


void _getVal(const char *cmdLinePar,
	     char       *val)
{
    char  *chrP;
    
    if ((chrP = strchr(cmdLinePar, '=')) == NULL)
	*val = '\0';
    else
	strcpy(val, (chrP + 1));
}


/* Main
 */
int main (int argc, char *argv[])
{
    char             *fileUriP, *mimeTypeP;
    int              xmlReply = 0, i;
    int              repeat = 1, count;
    float            timeOut = ngamsNO_TIME_OUT;
    ngamsPAR_ARRAY   parArray;
    ngamsMED_BUF     tmpPar, tmpVal, host, servers, repHost, authorization;
    ngamsMED_BUF     fileUri, mimeType, outputFile, finalOutputFile;
    ngamsSMALL_BUF   command, tmpDynPar;
    ngamsSTAT        stat = 0;
    ngamsSTATUS      status;


    ngamsInitApi();
    ngamsInitStatus(&status);
    ngamsResetParArray(&parArray);
    *servers = *command = *authorization = '\0';
    for (i = 1; i < argc; i++)
	{
	strcpy(tmpPar, argv[i]);
	ngamsToUpper(tmpPar);
	if (strstr(tmpPar, "--AUTH=") != NULL)
	    {
	    _getVal(argv[i], tmpVal);
	    strcpy(authorization, tmpVal);  
	    }
	else if (strstr(tmpPar, "--CMD=") != NULL)
	    {
	    _getVal(argv[i], command);
	    }
	else if (strstr(tmpPar, "--LICENSE") != NULL)
	    {
	    printf("%s", ngamsLicense());
	    exit(0);
	    }
	else if (strstr(tmpPar, "--REPEAT=") != NULL)
	    {
	    _getVal(argv[i], tmpVal);
	    repeat = atoi(tmpVal);
	    }
	else if (strstr(tmpPar, "--XML") != NULL)
	    xmlReply = 1;
	else if (strstr(tmpPar, "--SERVERS=") != NULL)
	    {
	    _getVal(argv[i], servers);
	    if (ngamsParseSrvList(servers) != ngamsSTAT_SUCCESS)
		goto correctUsage;
	    }
	else if (strstr(tmpPar, "--TIMEOUT=") != NULL)
	    {
	    _getVal(argv[i], tmpVal);
	    timeOut = atof(tmpVal);
	    }
	else if (strstr(tmpPar, "--VERBOSE=") != NULL)
	    printf("\nNOTE: Command line option -v is not yet implemented!\n");
	else if (strstr(tmpPar, "--VERSION") != NULL)
	    { 
	    printf("%s\n", ngamsVersion());
	    exit(0);
	    }
	else if (strstr(tmpPar, "--PAR=") != NULL)
	    _getVal(argv[i], tmpDynPar);
 	else if (strstr(tmpPar, "--VAL") != NULL)
	    { 
	    _getVal(argv[i], tmpVal);
	    ngamsAddParAndVal(&parArray, tmpDynPar, tmpVal);
	    }
	else if ((strcmp(tmpPar, "-H") == 0) || 
		 (strstr(tmpPar, "--HELP") != NULL))
	    goto correctUsage;
	else
	    {
	    printf("\n\nFound illegal command line parameter: %s\n", tmpPar);
	    goto correctUsage;
	    }
	}   /* end-for (int i = 1; i < argc; i++) */
    if ((*servers == '\0') || (*command == '\0')) goto correctUsage;

    /* Authorization defined? */
    if (*authorization) ngamsSetAuthorization(authorization);
    
    /* Send timeout with the request */
    if (timeOut != -1)
	sprintf(tmpVal, "%d", (int)(timeOut + 0.5));
    else
	sprintf(tmpVal, "-1");
    ngamsAddParAndVal(&parArray, "time_out", tmpVal);

    /* Execute command.
     */	
    for (count = 1; count <= repeat; count += 1)
	{
	if (repeat > 1) printf("\nCommand repetition counter: %d\n\n", count);
	if (strcmp(command, ngamsCMD_ARCHIVE_STR) == 0)
	    {
	    if ((fileUriP = ngamsGetParVal(&parArray, "filename")) == NULL)
		*fileUri = '\0';
	    else
		strcpy(fileUri, fileUriP);
	    if ((mimeTypeP = ngamsGetParVal(&parArray, "mime_type")) == NULL)
		*mimeType = '\0';
	    else
		strcpy(mimeType, mimeTypeP);
	    stat = ngamsGenSendData(host, -1, ngamsCMD_ARCHIVE, timeOut,
				    fileUri, mimeType, &parArray, &status);
	    }
	else if (strcmp(command, ngamsCMD_RETRIEVE_STR) == 0)
	    {
	    stat = ngamsGenRetrieve2File(host, -1, timeOut, ngamsCMD_RETRIEVE,
					 &parArray, outputFile, 
					 finalOutputFile, &status);
	    }
	else
	    stat = ngamsGenSendCmd(host, -1, timeOut, command, &parArray,
				   &status);

	/* Dump the NG/AMS status if requested */
	if (xmlReply && (status.replyData[0] != NULL))
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
	    /* IMPL: Add port in ngamsSTATUS, which will contain the port
	     * number applied for the request. 
	     */
	    /* printf("Port:           %d\n", port); */
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
    _correctUsage();
    exit(1);
    return(1);
}


/* EOF */
