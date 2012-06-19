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
 * "@(#) $Id: FileReceivedEvent.java,v 1.2 2008/08/19 20:51:50 jknudstr Exp $"
 * 
 * Who       When        What
 * --------  ----------  ------------------------------------------------------
 * F.Woolfe  2003        First version
 * jknudstr  2007/06/20  Gave overhaul, included in ALMA NG/AMS Module.
 *
 */

package alma.ngas.client;

/**
 * Information concerning a file that has just been received. File arrived 
 * from NGAS because of a subscription.
 */
public class FileReceivedEvent {

    /**
     * The number of files received so far.
     */
    public int numOfFilesReceived;

    /**
     * 
     * @param numOfFilesReceived the number of files received so far.
     */
    public FileReceivedEvent(int numOfFilesReceived) {
	this.numOfFilesReceived=numOfFilesReceived;
    }
}

// EOF
