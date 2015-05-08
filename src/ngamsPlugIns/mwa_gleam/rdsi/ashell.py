#!/usr/bin/python

import os
import re
import cmd
import sys
import glob
import time
import random
import getpass
import tempfile
import subprocess

# --- settings, do not edit (unless you know what you're doing)

production = {"id" : "ivec", "host" : "data.ivec.org", "port" : 443, "protocol" : "https" , "domain" : "ivec", "sid" : "" }
#development = {"id" : "dev", "host" : "192.168.115.171", "port" : 80, "protocol" : "http" , "domain" : "system", "sid" : "" }
development = {"id" : "dev", "host" : "202.8.39.121", "port" : 80, "protocol" : "http" , "domain" : "system", "sid" : "" }
current = production

sid_file = os.path.expanduser("~/.livearc_sid_" + current['host'])
need_auth = True
debug = True
version = 0.2

# --- simple script process execution
def run_command(script):
	global debug

	qsubProcess = subprocess.Popen(script, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdoutData, stderrData = qsubProcess.communicate()
	stdoutData = stdoutData.rstrip("\n")
	stderrData = stderrData.rstrip("\n")

	if (stderrData):
		if (debug):
			print stderrData
		raise Exception("process execution failed: %s" % (stderrData))

# TODO - potential bug report to arcitecta?
# TODO - reproduce first ... possibly by attempting relogin many hours later with expired SID
# this generates an error message "error: Connection refused" in stdout ... rather than stderr
# CURRENT - this will false positive if there is a filename containing "error"
#	if ("error" in stdoutData):
#		if (debug):
#			print stderrData
#		raise Exception("process execution failed")

	return stdoutData

# ---------------------

def print_server():
	global current
	print "Current LiveARC server %s://%s %s" % (current['protocol'], current['host'], current['port'])

# ---------------------

def print_user():
	global need_auth, current, client
	if (need_auth):
		print "Not authenticated"
	else:
		script = 'java -Dmf.host=%s -Dmf.port=%s -Dmf.transport=%s -Dmf.sid=%s -jar %s --app exec actor.self.describe' % \
				( current['host'], current['port'], current['protocol'], current['sid'], client )
		try:
			out = run_command(script)
			print out
		except:
			print "Not authenticated"
			need_auth=True

# ---------------------

def logout():
	global need_auth, sid_file

	try:
		os.remove(sid_file)
	except:
		print "Already logged out."

	need_auth = True

# ---------------------

def authenticate_test():
	global need_auth, current, client, sid_file

	need_auth = False

	if (os.path.isfile(sid_file)):
		with open (sid_file, "r") as myfile:
			current['sid'] = myfile.read().replace('\n', '')

#		print "sid = " + current['sid']

# login again if we get an error using existing auth token
		script = 'java -Dmf.host=%s -Dmf.port=%s -Dmf.transport=%s -Dmf.sid=%s -jar %s --app exec actor.self.describe' % \
				( current['host'], current['port'], current['protocol'], current['sid'], client )
		try:
			out = run_command(script)
		except:
			need_auth = True
	else:
		need_auth = True

# ---------------------
def authenticate():
	global need_auth, current, client

	domain = raw_input("Domain [%s]" % current['domain'])
	current['domain'] = domain or current['domain']
#	print "authentication domain: " + current['domain']
	user = raw_input("Username: ")
	password = getpass.getpass("Password: ")
# authentication command
	script = 'sid=`java -Dmf.host=%s -Dmf.port=%s -Dmf.transport=%s -jar %s --app exec logon %s %s %s`\n' % \
			( current['host'], current['port'], current['protocol'], client, current['domain'], user, password)
	script += 'echo $sid'
	out = run_command(script)

# if empty -> bad username/password probably 
	if (len(out) < 1):
		raise Exception("Authentication failed")
	current['sid'] = out
# save the authentication token
	fs = open(sid_file, "w")
	fs.write(current['sid'])
	fs.close()
	need_auth = False

# ---------------------

def unsafe_authenticate(user, password):
	global need_auth, current, client

# authentication command
	script = 'sid=`java -Dmf.host=%s -Dmf.port=%s -Dmf.transport=%s -jar %s --app exec logon %s %s %s`\n' % \
			( current['host'], current['port'], current['protocol'], client, current['domain'], user, password)
	script += 'echo $sid'
	out = run_command(script)

# if empty -> bad username/password probably 
	if (len(out) < 1):
		raise Exception("Authentication failed")
	current['sid'] = out
# save the authentication token
	fs = open(sid_file, "w")
	fs.write(current['sid'])
	fs.close()
	need_auth = False

# ---------------------


# --- execute a mediaflux command using an existing auth token 
def execute(cmd):
	global current, need_auth, client

	if (need_auth):
		print "Not logged in."
	else:
		script = 'java -Dmf.host=%s -Dmf.port=%s -Dmf.transport=%s -Dmf.sid=%s -jar %s --app exec "%s"' % \
			( current['host'], current['port'], current['protocol'], current['sid'], client, cmd )
		return run_command(script)

# ---------------------

class parser(cmd.Cmd):

# --- auto prompt set depending on authentication status
	def preloop(self):
		global current

		authenticate_test()
		if (need_auth):
			self.prompt = "%s:offline>" % current['id']
		else:
			self.prompt = "%s:online>" % current['id']
	def precmd(self, line):
		global current

		if (need_auth):
			self.prompt = "%s:offline>" % current['id']
		else:
			self.prompt = "%s:online>" % current['id']
        	return cmd.Cmd.precmd(self, line)
    
	def postcmd(self, stop, line):
		global current

		if (need_auth):
			self.prompt = "%s:offline>" % current['id']
		else:
			self.prompt = "%s:online>" % current['id']
		return cmd.Cmd.postcmd(self, stop, line)


# --- dont repeat the last command if nothing entered (default)
	def emptyline(self):
#		print "do nothing"
		return

# --- done
	def help_quit(self):
		print "Exit this shell.\n"
	def do_quit(self, line):
		exit(0)

# --- done
	def help_exit(self):
		print "Exit this shell.\n"
	def do_exit(self, line):
		exit(0)

# --- local command
	def help_ls(self):
		print "Displays LOCAL files in the current working directory.\n"

	def do_ls(self, line):
		try:
			out = run_command("ls " + line)
			print out
		except:
			print "Sorry, you can't do that."

# --- local command
	def help_cd(self):
		print "Changes the current LOCAL working directory.\n"

	def do_cd(self, line):
		try:
			os.chdir(line)
			out = run_command("pwd")
			print out
		except:
			print "Sorry, you can't do that."

# --- local command
	def help_pwd(self):
		print "Displays the current LOCAL working directory.\n"

	def do_pwd(self, line):
		out = run_command("pwd")
		print out

# --- asset listing
	def help_list(self):
		print "List the assets (files) stored on the current remote LiveARC server.\n"
		print "Usage: list <namespace>\n"
		print "Example: list /projects/MYPROJECT\n"

	def do_list(self, line):
		if (line):
			try:
				out = execute("asset.namespace.list :assets true :action get-name :size 3000 :namespace " + line)		
				print out
			except:
				print "No such namespace."
		else:
			print "You must specify a remote namespace to list (eg /projects)"

# --- asset upload
	def help_upload(self):
		print "Upload a local file or folder to a remote namespace (folder) on the current LiveARC server.\n"
		print "Usage: upload <full_path_to_file(s)> to <full_namespace_path>\n"
		print "Example: upload /home/sean/*.jpg to /projects/My Project/images\n"
		print "         upload /home/sean/mydirectory/ to /projects/My Project/\n"

# upload <src> to <dest>
# NB: in script mode need to escape/quote any wildcards (*) to avoid premature expansion
	def do_upload(self, line):

# NB: can't split on whitespace as some filenames/filepaths may have spaces
#		tokens = line.split()
		tokens = line.split('to')

		if len(tokens) == 2:
			src = glob.glob(tokens[0].strip())
			dest = re.escape(tokens[1].strip())
			for item in src:
				print "Uploading: " + item
				try:
					execute("import -namespace " + dest + " " + os.path.abspath(item))
				except:
					print "Failed to upload."
		else:
			print "Incorrect syntax, type 'help upload' for examples."

# --- asset download
	def help_download(self):
		print "Download asset(s) to a local directory.\n"
		print "Usage: download <asset_id_list> <local_directory<\n"
		print "Example: download 1234 9813 /Users/sean/downloads\n"

	def do_download(self, line):
# TODO - needs a lot of polishing
		tokens = line.split()
		nargs = len(tokens)
# TODO - if last arg not a path -> use cwd
		if (nargs < 2):
			print "Downloading requires at least two arguments: <asset_id> and <destination>"
			return

		for i in range(0, nargs-1): 
			try:
				out = execute("asset.get :id " + tokens[i])
				match = re.search(':name\s+"(.+?)"', out)
				if match:
					found = match.group(1)
					filepath = "file:/" + tokens[nargs-1] + "/" + found
# TODO - handle if exists (currently will just overwrite)
					print "downloading asset %s as: %s" % (tokens[i], filepath)
					execute("asset.get :id " + tokens[i] + " :out " + filepath)
			except:
				print "Failed: bad asset id or no permission to read?"


# ---- asset destroy
	def help_destroy(self):
		print "Destroy a remote asset (file) by its id number.\n\nUsage: destroy <asset_id_list>\n\nExample: destroy 1234 5490\n"

	def do_destroy(self, line):
		tokens = line.split()
		for token in tokens:
			try:
				float(token)
				execute("asset.destroy :id " + token)
			except:
				print "Failed to destroy bad asset id: " + token

# --- namespace create
	def help_mkfolder(self):
		print "Create a remote folder on the current LiveARC server.\n\nUsage: mkfolder /full/path/to/folder\n"

	def do_mkfolder(self, line):
		if (line):
			try:
				execute("asset.namespace.create :namespace " + line)		
			except:
				print "Failed to create namespace."
		else:
			print "You must specify a remote folder to create (eg /projects/MYPROJECT/new)"

# --- namespace destroy
	def help_rmfolder(self):
		print "Destroy a remote folder on the current LiveARC server.\n\nUsage: rmfolder /full/path/to/folder\nWARNING: Cannot be undone.\n"

	def do_rmfolder(self, line):
# TODO - are you sure prompt (if non empty)
		if (line):
			try:
				execute("asset.namespace.destroy :namespace " + line)		
			except:
				print "Failed to dewstroy namespace."
		else:
			print "You must specify a remote folder to destroy (eg /projects/MYPROJECT/new)"

# --- server authentication
	def help_login(self):
		print "Initiate login to the current remote LiveARC server.\n"

	def do_login(self, line):
		tokens = line.split()
		n = len(tokens)
		if n == 0:
			authenticate()		
		elif n == 2:
			unsafe_authenticate(tokens[0], tokens[1])
		else:
			print "Incorrect syntax for login."

# --- server authentication
	def help_logout(self):
		print "Destroy the cached credential for your current session with a remote LiveARC server.\n"

	def do_logout(self, line):
		logout()

# --- server reporting
	def help_status(self):
		print "Returns information on the current connection and user, if authenticated.\n"
		print "Usage: status\n"

	def do_status(self, line):
		print_server()
		print_user()

# --- server settings 
	def help_server(self):
		print "View or change the current LiveARC remote server to connect to.\n"
		print "Useage: if called with no arguments, returns the current server. If called with the server name and port, changes these values.\n"
		print "Example: server data.ivec.org 443\n"

	def do_server(self, line):
		global current, sid_file

		if (line):
			tokens = line.split()
			if (len(tokens) == 2):
				if (tokens[1] == "443"):
					newp = "https"
				else:
					newp = "http"

				current['host'] = tokens[0]
				current['port'] = tokens[1]
				current['protocol'] = newp
				sid_file = os.path.expanduser("~/.livearc_sid_" + current['host'])
# recheck sid auth
				authenticate_test()
			else:
				print "Setting the server requires two arguments: <hostname> and <port>"
		else:
			print_server()
			print_user()

# --- my shortcut (dev VM)
	def do_dev(self, line):
		global sid_file, current, development

		current = development
		sid_file = os.path.expanduser("~/.livearc_sid_" + current['host'])
		authenticate_test()
		print_server()
		print_user()

# --- my shortcut (production)
	def do_prod(self, line):
		global sid_file, current, production

		current = production
		sid_file = os.path.expanduser("~/.livearc_sid_" + current['host'])
		authenticate_test()
		print_server()
		print_user()

# --- process a single line
	def onecmd(self, s):
#		print 'onecmd(%s)' % s
		return cmd.Cmd.onecmd(self, s)
 

if __name__ == '__main__':
	global client

# check for java 
#	code = os.system("java -version")
#	if (code):
#		exit(-1)
# check for aterm.jar
	client = os.path.dirname(os.path.abspath(__file__)) + "/aterm.jar"
	if (os.path.isfile(client) == 0):
		raise Exception("Error: aterm.jar not found")

# welcome if not cmd line mode
	if (len(sys.argv) < 2):
		print "Welcome to ashell v%.1f, type 'help' for a list of commands" % version

# command interpreter
	my_parser = parser()
	if (len(sys.argv) < 2):
		my_parser.cmdloop()
	else:
# check if we have a valid sid
		authenticate_test()
# run supplied commands as an input script
		script = ' '.join(sys.argv[1:])
		tokens = script.split('+')
		for token in tokens:
			my_parser.onecmd(token)

