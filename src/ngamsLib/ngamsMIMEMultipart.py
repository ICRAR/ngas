#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

import time
from email.parser import Parser
from ngams import error, info

CRLF = '\r\n'

class MIMEMultipartHandler(object):
	"""
	A base class for MIME Multipart handler.
	"""

	def startContainer(self, containerName):
		"""
		Method invoked when the headers of the root MIME Multipart
		message have already been parsed, at which stage the name
		of the container should be known
		"""
		pass

	def endContainer(self):
		"""
		Method invoked when the root MIME Multipart message has
		been completely parsed, and therefore the parsing has finished
		"""
		pass

	def startFile(self, filename):
		"""
		Method invoked when a new part of the MIME Multipart message
		has been detected, and its own headers parsed. At this stage
		the filename of the file represented by this part should be known
		"""
		pass

	def handleData(self, data, moreExpected):
		"""
		Method invoked by the parser to pass down the data contained in
		the part currently being parsed. This method might be called more than
		once, and indicates whether more data should be expected to be passed
		down by the parser.

		In the particular case that this method cannot handle the full
		contents of the data passed down by the parser, it should return
		whatever data remained to be handled back to the parser. The base
		implementation of this method returns None, signaling that it has
		handled all the data successfully
		"""
		return None

	def endFile(self):
		"""
		Method invoked when the end of the file currently being streamed
		has been found by the parser.
		"""
		pass

class MIMEMultipartParser(object):
	"""
	A class that parses incoming ngams/container contents, which are
	encapsulated in the form of MIME multipart messages with the
	Content-type multipart/mixed.

	A message parser exists already in the system-wide email.parser package.
	However, it works by first consuming the whole content of a given stream
	(e.g., a file or a string) and then returning the representation of the
	stream in the form of email.message.Message objects. This is well suited
	for small messages; however as the total payload increases the email.parser
	Parser classes start consuming too much memory, rendering it unusable.

	On the other hand, this parser works by incrementally parsing the incoming
	contents (which are given via a file object) and then notifying a handler
	when events occur. In particular, it notifies the handler when the container
	headers have been read, when the headers of each part of the multipart message
	have been read (meaning that the contents of a file follow), the file contents
	themselves, and when the file contents have ended.
	"""

	class _ReadingState:
		"""A simple enumeration of the states on which the parser can be found at any given time"""
		mainheader, delimiter, headers, data = range(4)

	def __init__(self, handler, fd, totalSize, readSize):
		"""
		Initializes a new parser.

		The new parser will notify the given handler about important events
		that occur during parsing. The parser will read the stream pointed out
		by fd in readSize blocks (or less, if there is less than
		readSize bytes left on fd to read) until totalSize
		has been read.
		"""
		self._handler = handler
		self._fd = fd
		self._totalSize = totalSize
		self._readSize = readSize
		self._bytesRead = 0

	def getReadingTime(self):
		"""
		Returns the total time spent by this parser reading data from the
		original stream given at construction during the parsing routine.
		"""
		return self._readingTime

	def getBytesRead(self):
		"""
		Returns the number of bytes read by this parser from the original
		stream given at construction time during the parsing routine
		"""
		return self._bytesRead

	def parse(self):
		"""
		Parses the contents of the stream of data as a MIME Multipart message.

		This parser finishes parsing when the end of the MIME Multipart message
		is found, and not earlier. This means that stream of data that don't contain
		a proper MIME Multipart ending delimiter will potentially get stuck while
		calling this method.
		"""

		rdSize = self._readSize
		remSize = self._totalSize

		readingTime = 0

		readingFile = False
		prevBuf = None
		state = self._ReadingState.mainheader

		while True:

			# Don't try to over-read during the last reading
			if remSize < rdSize:
				rdSize = remSize

			# Read, read, read...
			rdt = time.time()
			buf = self._fd.read(rdSize)
			readingTime += (time.time() - rdt)
			remSize -= rdSize
			self._bytesRead += len(buf)

			# Anything coming from a previous iteration gets prefixed
			if prevBuf:
				buf = prevBuf + buf
			prevBuf = None

			# On the first stage we read the MIME multipart headers and parse them
			# If found, we start reading delimiters; otherwise we keep reading data
			if state == self._ReadingState.mainheader:
				endingIdx = buf.find(CRLF + CRLF)
				if endingIdx != -1:
					info(4, 'Parsing MIME multipart headers')
					state         = self._ReadingState.delimiter
					headers       = buf[:endingIdx + 4]
					buf           = buf[endingIdx + 4:]
					msg           = Parser().parsestr(headers, headersonly=True)
					boundary      = msg.get_param('boundary')
					containerName = msg.get_param('container_name')
					info(5, 'MIME multipart boundary: ' + boundary)

					# Fail if we're missing any of these
					if not boundary or not containerName:
						msg = 'Either \'boundary\' or \'container_name\' are not specified in the Content-type header'
						error(msg)
						raise Exception, msg

					self._handler.startContainer(containerName)

				else:
					prevBuf = buf
					continue

			# We can read delimiters either because we've just started
			# reading the body of the MIME multipart message or because
			# we just finished reading a particular part of the multipart
			if state == self._ReadingState.delimiter:

				# We come from reading a previous multipart part
				if readingFile:
					self._handler.endFile()
				readingFile = False

				# Look for both delimiter and final delimiter
				delIdx  = buf.find(CRLF + '--' + boundary + CRLF)
				fDelIdx = buf.find(CRLF + '--' + boundary + '--')
				if delIdx != -1:
					# We don't actually need the delimiter itself
					#delimiter = buf[:delIdx + 4 + len(boundary) + 2]
					buf       = buf[delIdx + 4 + len(boundary) + 2:]
					state     = self._ReadingState.headers
				elif fDelIdx != -1:
					self._handler.endContainer()
					break
				else:
					prevBuf = buf
					continue

			if state == self._ReadingState.headers:
				idx = buf.find(CRLF + CRLF)
				if idx != -1:
					info(4, 'Processing file headers')
					headers  = buf[:idx+4]
					buf      = buf[idx+4:]
					msg      = Parser().parsestr(headers, headersonly=True)
					filename = msg.get_filename()
					if not filename:
						msg = 'No filename found in internal multipart part header'
						raise Exception, msg
					state = self._ReadingState.data
					self._handler.startFile(filename)
					readingFile = True
				else:
					prevBuf = buf
					continue

			# When reading data, look for the next delimiter
			# When found, finish writing data, and pass the
			# delimiter to the ReadingState.delimiter state
			if state == self._ReadingState.data:
				delIdx  = buf.find(CRLF + '--' + boundary)
				if delIdx != -1:
					info(5, 'Found end of file ' + filename)
					state = self._ReadingState.delimiter
					prevBuf = buf[delIdx:]
					buf = buf[:delIdx]

				buf = self._handler.handleData(buf, state == self._ReadingState.data)
				if buf and len(buf) > 0:
					prevBuf = buf

		self._readingTime = readingTime
