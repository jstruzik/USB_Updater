#!/usr/bin/env python
"""
File: usb_updater.py
Date: 2012-03-02

A simple script to mass update USB drives with a partition of useful tools and a bootable partition using
SysLinux. Tested with CentOS systems only.
"""

import urllib
import os
import sys
import csv
import subprocess
from multiprocessing import Process, Pool, Lock
import string
from optparse import OptionParser
import re
from time import ctime, sleep, time

DEBUG_LEVEL = 0
# location that the USB drives (aka /dev/sdX#)
MEDIA_DEV_ROOT = ''
# location that the USB drives will be mounted under
MEDIA_MOUNT_POINT_ROOT = ''
# Flag for whether we will be imaging the drives
IMAGE_DRIVES = False
# Flag for whether we will be syncing the drives (TOOLS)
SYNC_DRIVES = False

# variable to keep track of total number of drives found/processed, etc
numDrives = 0
# list of drive objects (initialized)
drives = []
# Flag for whether we should run in 'force' mode (unused at the moment)
force = False
# Flag for whether we will be generating an email
email = False
# Temporary file to save the email output to through the execution
emailFile = 'mainEmail.log'
# Email recipients, as a list
recipients = ['me@email.com']
# List of drives that failed/errored
failed_drives = []


def debug(text, level):
	"""Display a debug message if it has high enough priority
	@param text	 - debug message to display
	@param level - priority level of this message
	"""
	if level <= DEBUG_LEVEL and DEBUG_LEVEL > 0:
		print "\n[debug] " + text
		# if the email flag has been set, add the message to the email body
		if email:
			emailBuilder("[debug] " + text)
		# flush the cache
		sys.stdout.flush()
	
def emailBuilder(text):
	"""Appends 'text' to the email body
	@param text - the text to append
	"""
	emailBody.write("\n" + text)
	emailBody.flush()
	
def errorHandler(ErrorType, error, job, stderr="", quit=False):
	"""Handles possible exceptions/errors and reports them
	@param ErrorType - type of exception that occured (ValueError, OSError, etc)
	@param error - the error string
	@param job - the string describing the job that had an exception
	@param stderr - error output from the standard error stream
	@param quit - optional parameter to specify whether it should quit because of this exception
	"""
	debug(ErrorType + ": The following action failed: " + job, 0)
	debug("The command that failed: " + str(error), 0)
	if stderr != "":
		debug("STDERR: " + stderr, 0)
		debug("End STDERR.", 0)
	sys.stdout.flush()
	if quit:
		debug("Exiting...", 0)
		exit()
	
def sendEmail(body):
	"""Compiles and sends the email with the individual logs concatenated in a logical format
	@param body - the email body that had been built by 'emailBuilder()'
	"""
	if len(failed_drives) > 0: # Check for failed drives and note them if necessary
		body.write("\nPartitions that errored:\n")
		body.write("\n".join(failed_drives))
	body.write("\n-----------------------\nDrive-specific debugging\n")
	sb = [] # Begin putting all the drive logs together
	for cur in drives:
		sb.append('\n---------Log for:'+cur.getName()+'-----------\n')
		sb.append(cur.readEmail())
		sb.append('\n---------END OF LOG:'+cur.getName()+'-----------\n')
		os.system('rm -f ' + cur.getDebuggerFile())
	body.write(''.join(sb))
	body.write("\n\nSent email: " + ctime())
	# close the file after final write
	body.close()
	# reopen the file for read-only
	body = open(emailFile, "r")
	# read each line into variable bodyText
	bodyText = ""
	for line in body.readlines():
		bodyText += line
	# close the file after reading
	body.close()

	subjectLine = "USB Status Report"
	# If there is an error in the log, dump some information so that we can look at them later
	if bodyText.lower().__contains__('error:'):
		subjectLine = "USB Status Report - ERROR"
		os.system("/bin/dmesg > /scripts/dmesg-" + str(time()) + ".log")
		os.system("/bin/mount > /scripts/mount-" + str(time()) + ".log")

	# mailx command to send the email
	emailCommand = ["mailx",					#Use mailx to send the email
					"-s",						#Set the subject line to...
					subjectLine,
					"-r",						#Set the "From" value to...
					"server",
					" ".join(recipients)		#and send it to 'recipients' joined with a space
					]
	# Does NOT use runCommand() because the body of the email must be passed to subprocess as input
	try:
		p = subprocess.Popen(emailCommand, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
		# pass the contents of 'bodyText' to mailx as the body of the email
		(stdout, stderr) = p.communicate(bodyText)
		if stderr:
			raise ValueError, emailCommand
	except OSError, e:
		errorHandler("OSError", e, "send the status email", stderr)
	except ValueError, e:
		errorHandler("ValueError", "send the status email", stderr)
	
def syncUSBFolder():
	"""Copies the tools from the resources server to this local machine using rsync
	This will then be used to copy to each drive. Useful for updating the tools folder.
	"""
	debug("I'm starting to copy the lastest tools from the server to the local machine for faster copying to the drives.", 1)
	#We'll start with constructing the command to sync the files to the media.
	command = [ '/usr/bin/rsync', '-rtqvv8D', \
			'--delete', \				# deletes extra files/folders at the destination that don't exist at the source
										# if we remove something from tools, we won't continue to put it on the usb drives
			'-e', \
			rshArg, \
			'user@server:/tools_directory/',\
			'/local_tools/']
	action = "sync the server USB folder to a local location"
	
	#This actually executes the command.
	std_out, std_err = runCommand(command, action)

def runCommand( command, actionMsg, expectedErr = "", exitOnFail=False, debugLvl=1 ):
	"""Helper method to run a (bash) command/script and handle exceptions and 'expected' output in the
	standard error stream. This also displays helpful information about what action is being performed, etc.
	@param command - the bash command/script to be executed
						to avoid bugs, have the command split by arguments before using this method (not necessarily split on whitespace!)
	@param actionMsg - human readable message that describes what this command is doing
	@param expectedErr - (optional) expected error stream output that shouldn't trigger a failure
	@param exitOnFail - (optional) flag to specify whether this is a critical process or not
	@param debugLvl - (optional) value to specify the priority of the debug messages for this command
	
	@returns (stdout, stderr) - returns the strings that contain the standard output and error stream contents 
	"""
	#make sure command is a list split on spaces (THIS SHOULD BE SPLIT ON ARGUMENTS)
	if type(command).__name__ == 'str':
		command = command.split(' ')

	debug("Starting: " + actionMsg, debugLvl)
	debug("Using: " + ' '.join(command), debugLvl)

	command_stdout, command_stderr = "", ""
	
	try:
		p =	subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(command_stdout, command_stderr) = p.communicate()
		if command_stderr:
			if expectedErr != "" and not command_stderr.endswith(expectedErr):
				raise ValueError, command
			elif expectedErr == "":
				raise ValueError, command
	except OSError, e:
		errorhandler("OSError", e, actionMsg, command_stderr, exitOnFail)
	except ValueError, e:
		errorHandler("ValueError", e, actionMsg, command_stderr, exitOnFail)
	
	debug("Completed: " + actionMsg, debugLvl)
	return (command_stdout, command_stderr)

class media:
	"""A 'media' object is any media on which we might copy USB Tools or an image (e.g., USB Flash Drive)
	"""
	def __init__(self, name, dev, debugLvl, forceBool, emailBool):
		"""
		@param name - identifiable human-readable name for this device (e.g. usb1part2)
		@param dev - /dev/usbXpartY value for this device
		@param debugLvl - default debug level for this device
		@param forceBool - flag to determine if we should run this in 'force' mode (NOT IN USE)
		@param emailBool - flag to determine if we should send email reports for this device
		"""
		self.name = name
		self.dev = dev
		# Partition number is the last character of the device string
		self.partNum = int(self.dev[-1])
		self.debugLevel = int(debugLvl)
		self.forceOn = forceBool
		self.emailOn = emailBool
		if self.emailOn:
			#define a unique log file for this device
			self.emailFile = "/scripts/logs/" + self.name + ".log"
			#Remove the email log file for this drive if it exists
			if os.path.isfile(self.emailFile):
				os.remove(self.emailFile)
			#Open a new email log file for this drive for writing
			self.emailBody = open(self.emailFile, "w")
		# The device's mountpoint on the machine
		self.mountPoint = MEDIA_MOUNT_POINT_ROOT + '/' + self.name
		command = [ '/bin/find', self.dev, '-type', 'l', '-exec', 'readlink', '-f', '{}', ';' ]
		action = "follow symlink to determine correct /dev/sdX#"
		std_out, std_err = self.runCommand(command, action)
		self.dev_sd = std_out.strip()
		
		debug("Name: %s, dev: %s, dev_sd: %s partNum: %s, mountPoint: %s" % (self.name, self.dev, self.dev_sd, self.partNum, self.mountPoint), 3)
		
	def setPart(self, part):
		"""Set the partition number for this device"""
		self.partNum = int(part)
		
	def getName(self):
		"""Returns the name we've assigned this drive."""
		return self.name
		
	def getDev(self):
		"""Returns the location where the media is located."""
		return self.dev
		
	def getMountPoint(self):
		"""Returns the mountpoint we're using."""
		return self.mountPoint

	def getPartNum(self):
		"""Returns the partition number of this media object"""
		return self.partNum

	def getDebuggerFile(self):
		"""Returns the path of the email file for this media object"""
		return self.emailFile
		
	def errorHandler(self, ErrorType, error, job, stderr="", quit=False):#quit=True
		"""Handles possible exceptions/errors and reports them
		@param ErrorType - type of exception that occured (ValueError, OSError, etc)
		@param error - the error string
		@param job - the string describing the job that had an exception
		@param stderr - error output from the standard error stream
		@param quit - optional parameter to specify whether it should quit because of this exception
		"""
		if self.name not in failed_drives:
			failed_drives.append(self.name)
		self.debug(ErrorType + ": The following action failed: " + job, 0)
		self.debug("The command that failed: " + str(error), 0)
		if stderr != "":
			self.debug("STDERR: " + stderr, 0)
			self.debug("End STDERR.", 0)
		sys.stdout.flush()
		if quit:
			self.debug("Exiting...", 0)
			sys.exit(1)
	
	# Object-specific debugging and emailBuilders
	def debug(self, text, level):
		"""Display a debug message if it has high enough priority
		@param text	 - debug message to display
		@param level - priority level of this message
		"""
		if level <= self.debugLevel and self.debugLevel > 0:
			print "\n[debug] [" + self.dev + "] " + text
			if self.emailOn:
				self.emailBuilder("[debug] " + text)
		sys.stdout.flush()
		
	def runCommand( self, command, actionMsg, expectedErr = "", exitOnFail=False, debugLvl=1 ):
		"""Run a command using subprocess
			@param command
				List, where the command is split on spaces
				(can be string, but it will be split on spaces so beware of commands with strings with spaces....)
			@param actionMsg
				String describing what this command will do
			@param expectedErr
				String equal to the expected stderr of a specific command, if it always prints to stderr for example
			@param exitOnFail
				Boolean to determine if this process should quit on an error (DEFAULT: False)
			@param debugLvl
				Integer to specify what level to print the debug at
			@return (stdout, stderr) as a Tuple
		"""
		#make sure command is a list split on spaces
		if type(command).__name__ == 'str':
			command = command.split(' ')

		self.debug("Starting: " + actionMsg, debugLvl)
		self.debug("Using: " + ' '.join(command), debugLvl)

		command_stdout, command_stderr = "", ""
	
		try:
			p =	subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			(command_stdout, command_stderr) = p.communicate()
			if command_stderr:
				if expectedErr != "" and not command_stderr.endswith(expectedErr):
					raise ValueError, command
				elif expectedErr == "":
					raise ValueError, command
		except OSError, e:
			debug("error!",1)
			self.errorHandler("OSError", e, actionMsg, command_stderr, exitOnFail)
		except ValueError, e:
			debug("error!",1)
			self.errorHandler("ValueError", e, actionMsg, command_stderr, exitOnFail)
	
		self.debug("Completed: " + actionMsg, debugLvl)
		return (command_stdout, command_stderr)
	
	def emailBuilder(self,text):
		"""Appends 'text' to the email body
		@param text - the text to append
		"""
		self.emailBody.write("\n" + text)
		self.emailBody.flush()

	def readEmail(self):
		"""Opens and reads the log file that will be used for an email report"""
		#Make sure the file is closed first
		self.closeDebugger()
		toReturn = ""
		try:
			#Open the file readonly
			emailReader = open(self.emailFile, "r")
			for line in emailReader.readlines():
				toReturn += line
			emailReader.close()
		except IOError, e:
			toReturn += "No log file for this partition."
		return toReturn

	def closeDebugger(self):
		"""Close the email log file"""
		self.emailBody.close()
	
	def cleanMountPoint(self):
		"""Ensures that the mountpoint assigned to the drive is removed"""
		#This is the command that we will use to remove the mountpoint.
		command = [ '/bin/rm', '-rf', self.mountPoint ]
		action = "clean this (\"" + self.mountPoint + "\") mountpoint"
		std_out, std_err = self.runCommand(command, action)
	
	def getCurrentMountPoint(self):
		"""Gets the mountpoint of this media device."""
		#Let's get the results from mount.
		command = [ '/bin/mount' ]
		action = "get a list of mounted devices (using 'mount') - " + self.dev
		std_out, std_err = self.runCommand(command, action)
		
		# Separate the output into individual lines for processing
		result = std_out.split("\n")	
		
		#Now, it's time to cycle through each line and see if it's one of our media.
		for current in result:
			toCheck = string.split(current)
			#if len(toCheck) < 3:
				#return False
			if len(toCheck) > 2:
				#self.debug(str(toCheck), 4)
				#currentDev = toCheck[0]
				currentMountPoint = toCheck[2]

				#Here's the actual mountpoint
				#self.debug("Current mountpoint we're reviewing is: " + currentMountPoint, 4)
				if currentMountPoint.find(self.name) != -1:
					self.debug("We found the mountpoint!: (\"" + currentMountPoint + "\")\n", 2)
					return currentMountPoint

		#Let's try to get a list from /proc/mounts as well, just in case
		command = [ '/bin/cat', '/proc/mounts' ]
		action = "read /proc/mounts just in case 'mount' didn't get everything"
		#Run the command
		std_out, std_err = self.runCommand(command, action)		
		# Separate the output into individual lines for processing
		resultProc = std_out.split("\n")

		for current in resultProc:
			toCheck = string.split(current)
			if len(toCheck) > 1:
				#self.debug(str(toCheck), 4)
				#currentDev = toCheck[0]
				currentMountPoint = toCheck[1]
				#self.debug("Reviewing: " + currentMountPoint, 4)
				if currentMountPoint.find(self.name) != -1:
					self.debug("Found mountpoint!: (\"" + currentMountPoint + "\")\n", 2)
					return currentMountPoint

		return False
		
	def unmount(self):
		"""Unmounts the drive from its current mountpoint."""
		#If nothing is mounted at the mountpoint, we needn't go further.
		currentMountPoint = self.getCurrentMountPoint()
		self.debug("The current mount point for " + self.name + " is: " + str(currentMountPoint) + "\n", 3)

		if not currentMountPoint:
			self.debug("This drive is not mounted: " + self.dev + "\n", 3)
			return False
			
		if not os.path.exists(currentMountPoint) or not os.path.isdir(currentMountPoint):
			self.debug("This drive's mountpoint (" + currentMountPoint + ") does not exist\n", 3)
			return
			
		#Let's kill any processes that are accessing files on the drive.
		command = [ '/usr/sbin/lsof', '-t', currentMountPoint ]
		action = "check for open processes at this mountpoint: (\"" + currentMountPoint + "\")"
	
		#This actually executes the command.	
		std_out, std_err = self.runCommand(command, action)
		
		#We'll now get a string for each process to kill, but we'll need to split it into an array
		result = string.split(std_out)
	
		#Now, it's time to cycle through each process and kill it.
		for current in result:
			#Here's the actual kill command
			command = [ '/bin/kill', '-9', current ]
			action = "kill process #" + current + " (accessing \"" + currentMountPoint + "\")"
		
			#This actually executes the command.	
			std_out, std_err = self.runCommand(command, action)		
				
		#This is the command that we will use to unmount each drive.
		command = [ '/bin/umount', '-fv', self.dev ]
		action = "unmount this mountpoint: \"" + currentMountPoint +"\""
			
		#This actually executes the command.	
		std_out, std_err = self.runCommand(command, action)
		
		sleep(10) # Sleep for 3 seconds to make sure it's completed the unmount process
		
		self.cleanMountPoint()

	def mount(self):
		"""Mount the drive to the mountpoint it has been assigned."""
		#We'll need to create the mountpoint, if it doesn't already exist
		if os.path.exists( self.mountPoint ):
			self.unmount()
	
		#This is the command that we will use to create the mountpoint.
		command = [ '/bin/mkdir', '-p', self.mountPoint ]
		action = "create this mountpoint: \"" + self.mountPoint + "\""

		#This actually executes the command.
		std_out, std_err = self.runCommand(command, action)
	
		#This is the command that we will use to mount this drive.
		command = [ '/bin/mount', self.dev, self.mountPoint ]
		action = "mount '" + self.dev + "' to '" + self.mountPoint + "'"
		
		#This actually executes the command.
		std_out, std_err = self.runCommand(command, action)		
	
	def imageFedora(self, otherParts):
		"""Begin imaging the media object with live linux
		@param otherParts - a list of media objects that are the other partitions on the same drive as this partition
		"""
		#Unmount the drive if it's mounted
		self.unmount()
		for part in otherParts:
			part.unmount()
		
		#Lets start off anew! 
		self.cleanSlate(otherParts)

		self.unmount()		
		for part in otherParts:
			part.unmount()
		
		#Re-partition the drive such that partition 2 is 1GB FAT32 for WinPE/Ubuntu
		# and partition 1 is the rest of the disk fat32 for tools
		driveSize = self.partitionDrive(otherParts)

		self.unmount()		
		for part in otherParts:
			part.unmount()
		
		#Now format the newly partitioned drive
		self.formatDrive()
		
		self.unmount()
		for part in otherParts:
			part.unmount()
		
		if len(otherParts) < 2:
			part2 = media(self.name[:-1]+'2', self.getDev()[:-1]+'2', self.debugLevel, self.forceOn, self.emailOn)
			part2.setPart(2)
			part2.sync(driveSize)
		else:
		#If it's the second partiton, sync the ISO onto the drive
			for parts in otherParts:
				if parts.getPartNum() == 2:
					parts.sync(driveSize)
			
		#Setup Syslinux for the drive
		self.setupSyslinux()
		
	def setupSyslinux(self):
		"""Install syslinux on this device"""
		if re.match('[0-9]', self.dev_sd[-1]):
			dev = self.dev_sd[:-1]
		else:
			dev = self.dev_sd
		
		#Make sure we're unmounted before applying Syslinux
		self.unmount()
		
		'''command = ['parted',dev,'set','2','boot','on'] #Make partition active
		
		self.debug("Activating Partition",3)
		
		#Run the command
		command_stdout, command_stderr = "", ""
		try:
			p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			(command_stdout, command_stderr) = p.communicate()
			if command_stderr:	
				raise ValueError, command
		except OSError, e:
			self.errorHandler("OSError", e, "attempting to Activate Partition onto the drive", command_stderr)
		except ValueError, e:
			self.errorHandler("ValueError", e, "attempting to Activate Partition onto the drive", command_stderr)
		
		self.debug("Completed Activating Partition", 3)'''
		
		command = [ 'syslinux','-f','-i','-d','/',dev+'2'] #Use the syslinux command to install to the 2nd partition
		action = "install syslinux on the drive '" + str(dev) + "2'"
		
		#Run the command
		std_out, std_err = self.runCommand(command, action)
		
		# Did NOT use the runCommand method because of special expectedSTDERR handling		
		command = ['/bin/dd','bs=440','conv=notrunc','count=1','if=mbr.bin','of='+dev] #Install MBRto drive
		
		self.debug("Installing MBR",4)
		
		expectedSTDERR = "1+0 records"
		
		#Execute the command
		command_stdout, command_stderr = "", ""
		try:
			p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			(command_stdout, command_stderr) = p.communicate()
			if command_stderr.find(expectedSTDERR) == -1 or ( len(command_stderr) >= 100 ):	
				raise ValueError, command
			self.debug("dd's stderr (output):\n" + command_stderr, 2)
		except OSError, e:
			self.errorHandler("OSError", e, "attempting to install MBR onto the drive", command_stderr)
		except ValueError, e:
			self.errorHandler("ValueError", e, "attempting to install MBR onto the drive", command_stderr)
		
		self.debug("Completed installing MBR", 4)
		
	def sync(self,driveSize):
		"""Sync the contents of the live folder (for live linux, WinPE, etc) to the media device
		@param driveSize - the size of this device (to calculate whether we should include lubuntu.iso or not)
		"""
		self.unmount()
		self.mount()

		#We'll start with constructing the command to copy the files to the media.
		if driveSize < 7000:
			command = [ '/usr/bin/rsync', \
						'-rtqvv8D', \
						'/live_directory/', \
						self.mountPoint + '/' ]
		else:
			command = [ '/usr/bin/rsync', \
						'-rtqvv8D', \
						'/live_directory/', \
						self.mountPoint + '/' ]
		action = "copy the contents of the live folder"
		
		#Run the actual command		
		std_out, std_err = self.runCommand(command, action)
		
		#Unmount itself after completion
		self.unmount()

	def repairDrive(self):
		"""Checks and repairs drive"""
		if re.match('[0-9]', self.dev_sd[-1]):
			dev = self.dev_sd[:-1]
		else:
			dev = self.dev_sd
			
		command = ['dosfsck', '-a', dev+'1']
		action = "attempt to repair any disk errors on the volume"
		
		std_out, std_err = self.runCommand(command, action)

	def cleanSlate(self, otherParts):
		"""Clears partition table to one partition and formats it to fat32 to wipe out old partitions"""
		if re.match('[0-9]', self.dev_sd[-1]):
			dev = self.dev_sd[:-1]
		else:
			dev = self.dev_sd
			
			
		command = [ '/scripts/gdisk_script.sh',						#Use gdisk to wipe any possible GPT partition
					 				 dev,																		#use device 'dev' 
					 				 '/scripts/gdisk_cleanslate.commands']					#use the clean slate gdisk command file
		action = "wiping all partition tables of: " + dev
		
		expectedSTDERR = "Warning: The kernel is still using the old partition table."
		
		#Actually execute the partition command
		std_out, std_err = self.runCommand(command, action, expectedSTDERR)
		
		command = [ '/scripts/fdisk_script.sh',						#Use 'fdisk' to re-partition in bash script
					 				 dev,																		#use device 'dev' 
					 				 '/scripts/fdisk_cleanslate.commands']					#use the clean slate fdisk command file
		action = "partition this drive: " + dev
		
		expectedSTDERR = "Warning: invalid flag 0x0000 of partition table 4 will be corrected by w(rite)\n"

		#Actually execute the partition command
		std_out, std_err = self.runCommand(command, action, expectedSTDERR)
		
		#Let's just make sure we're unmounted
		self.unmount()		
		for part in otherParts:
			part.unmount()
		
		#If a drive began with only one partition, we don't have it listed as in 'otherParts' yet
		# so this will manually try to unmount every partition under the same device /dev/sdX
		command = [ '/bin/find', '/dev/' ]
		action = "getting a list of all partitions for device: " + dev
		
		std_out, std_err = self.runCommand(command, action)
		parts = std_out.split()
		for part in parts:
			if part != dev and part.startswith(dev):
				command = [ '/bin/umount', '-fv', part ]
				action = "unmount this (these?) mountpoint(s): \"" + part +"\""

				#This actually executes the command.	
				std_out, std_err = self.runCommand(command, action,expectedErr="not mounted\n")
		
				sleep(10) # Sleep for 3 seconds to make sure it's completed the unmount process
	
		# Ok, since it's a dummy let's force it to rescan the usb drive partition table
		command = [ '/sbin/hdparm', #use hdparm
								 '-z',	   #to force a partition table re-scan
								 dev ]	   #on the device
		action = "rescan the partition table for drive: " + dev
		
		std_out, std_err = self.runCommand(command, action)
		
		self.unmount()
		for part in otherParts:
			part.unmount()		

		command = [ 	'/sbin/mkdosfs',	# use 'mkfs.vfat' to format the partition as fat32 for TOOL
								'-n',		# set the name of the partition
								'CLEAN_BABY',	# name = TOOLS
								'-F',		# FAT size
								'32',		# FAT size = 32 (FAT32)	
								dev + '1'		# Format the whole thing
							]
		action = "wipe drive: " + dev
		
		#Actually execute the format command
		std_out, std_err = self.runCommand(command, action)
		
	def partitionDrive(self, otherParts):
		"""Partitions the drive this partition is located on
		@param otherParts - a list of other partitions on this drive so that we can unmount them all
		"""
		if re.match('[0-9]', self.dev_sd[-1]):
			dev = self.dev_sd[:-1]
		else:
			dev = self.dev_sd
		
		command = [ '/sbin/fdisk', 
					'-l',
					dev
				  ]
				  
		action = "get the size of drive: " + dev
		
		std_out, std_err = self.runCommand(command, action)
		if not std_out:
			self.errorHandler("ValueError", command, action, ' '.join(command), True)
		
		# split the output from fdisk by new line first, and take the second line (which contains the size info)
		line = std_out.split("\n")[1]
		# split the line by spaces " " and take the fourth item, which should contain the number of bytes
		size = line.split(" ")[4]
		# Make sure we *actually* got a number for the number of bytes
		if not size.isdigit():
			self.errorHandler("ValueError", "Size is not a number!!! [" + size + "]", "attempting to parse disk size", "", True)

		numSize = int(size) / 1000000 # turn bytes into MB
		if numSize < 7000:
			toolSize = numSize - 1536 # LINUX_PARTITION_SIZE_MB = 1536 # for 4gb drives
		else:
			toolSize = numSize - 2436 # LINUX_PARTITION_SIZE_MB = 2436 # for 8gb drives
		fdiskFile = self.buildFdiskCommandFile( toolSize )
		
		
		command = [ '/fdisk_script.sh',	#Use 'fdisk' to re-partition in bash script
					 dev,						#use device 'dev' 
					 fdiskFile]					#use the correct fdisk command file (4gb or 8gb drives)
		action = "partition drive: " + dev
		
		expectedSTDERR = "Warning: invalid flag 0x0000 of partition table 4 will be corrected by w(rite)\n"
		
		std_out, std_err = self.runCommand(command, action, expectedSTDERR)
		
		self.unmount()		
		for part in otherParts:
			part.unmount()
		
		#If a drive began with only one partition, we don't have it listed as in 'otherParts' yet
		# so this will manually try to unmount every partition under the same device /dev/sdX
		command = [ '/bin/find', '/dev/' ]
		action = "getting a list of all partitions for device: " + dev
		
		std_out, std_err = self.runCommand(command, action)
		parts = std_out.split()
		for part in parts:
			if part != dev and part.startswith(dev):
				command = [ '/bin/umount', '-fv', part ]
				action = "unmount this (these?) mountpoint(s): \"" + part +"\""

				#This actually executes the command.	
				std_out, std_err = self.runCommand(command, action,expectedErr="not mounted\n")
		
				sleep(10) # Sleep for 3 seconds to make sure it's completed the unmount process
		
		
		# Ok, since it's a dummy let's force it to rescan the usb drive partition table
		command = [ '/sbin/hdparm', #use hdparm
					 '-z',	   #to force a partition table re-scan
					 dev ]	   #on the device
		action = "rescan partition table for: " + dev
		
		std_out, std_err = self.runCommand(command, action)
		
		return numSize
		
	def buildFdiskCommandFile( self, toolSize ):
		try:
			defaultCommands = open( '/fdisk_4gb.commands', 'r' ) # open the default fdisk commands file for readonly
			commandsFileName = '/tmp/fdiskCommands'
			try:
				commandsFile = open( commandsFileName, 'w' ) # open the new fdisk commands file for writing
				for line in defaultCommands.readlines():
					if line.startswith("+"):
						commandsFile.write("+" + str(toolSize) + "MB\n")
					else:
						commandsFile.write(line)
				commandsFile.close()
			except IOError, e:
				self.errorHandler("IOError", e, "attempting to open the custom fdisk commands file", "", True)
			
			defaultCommands.close()		
		except IOError, e:
			self.errorHandler("IOError", e, "attempting to open the default fdisk commands file", "", True)

		return commandsFileName
		
	def formatDrive(self):
		if re.match('[0-9]', self.dev_sd[-1]):
			dev = self.dev_sd[:-1]
		else:
			dev = self.dev_sd

		# Make sure it's unmounted before formatting as fat32 (just in case)
		self.debug("Unmounting before FAT format....", 2)
		self.unmount()
	
		command = [	'/sbin/mkdosfs',	# use 'mkfs.vfat' to format the partition as fat32 for TOOL
					'-n',		# set the name of the partition
					'TOOLS',	# name = TOOLS
					'-F',		# FAT size
					'32',		# FAT size = 32 (FAT32)	
					dev + '1'	# use the FIRST partition on the device (windows will only mount first partition)
					]
		action = "format partition as fat32: " + dev + '1'
		
		#Actually execute the format command
		std_out, std_err = self.runCommand(command, action)

		# Make sure it's unmounted before formatting as FAT32 (just in case)
		self.debug("Unmounting before second FAT format....", 2)
		self.unmount()

		command = [	'/sbin/mkdosfs',	# use 'mkfs.vfat' to format the partition as fat32 for WinPE, Ubuntu
					'-n',		# set the name of the partition
					'LIVE',	# name = LIVE
					'-F',		# FAT size
					'32',		# FAT size = 32 (FAT32)	
					dev + '2'	# use the second partition
					]
		action = "format partition as fat32: " + dev + '2'
		
		#Actually execute the format command
		std_out, std_err = self.runCommand(command, action)
	
	def copyTools(self):
		
		#First, let's ensure that the drives are mounted properly.
		self.unmount()
		self.mount()
		

		#We'll start with constructing the command to copy the files to the media.
		command = [ '/usr/bin/rsync', '-rtqvv8D', \
					'--delete', \
					'/local_tools/',\
					self.mountPoint ]
		action = "copy tools to this mountpoint: " + self.mountPoint
		
		#This actually executes the command.
		std_out, std_err = self.runCommand(command, action)
		
		self.unmount()
		
def enumerateDrives():
	#This is the command that we will use to get a list of the drives that the OS has mounted.
	command = [ '/bin/find', MEDIA_DEV_ROOT, '-type', 'l']
	action = "enumerate drives"

	#This actually executes the command.
	(std_out, std_err) = runCommand(command=command, actionMsg=action, expectedErr="", exitOnFail=True)
	
	#We'll now get a string of each mountpoint, each on a new line; but we'll split those lines up into an array 
	result = string.split(std_out)

	#We're going to want to name each drive so we have something logical for each drive's mountpoint.
	#We'll just call them drive0, drive1, drive[...]	
	currentDrive = 0
	for current in result:
		debug(current, 1)
		drives.append( media(current.split('/')[-1], current, DEBUG_LEVEL, force, email) )
		currentDrive += 1
	numDrives = currentDrive

def exit():
	if email:
		# Make a notice that we're sending the email report		
		debug("Sending email report because of premature exit.", 0)
		# Actually send the email with the pre-built emailBody
		sendEmail(emailBody)
	sys.exit(1)

def processDrive(current): #"current" must be an array of media devices
	if type(current).__name__ != "list":
		debug("processDrive: did not get array of media devices....", 0)
		exit()
	if IMAGE_DRIVES:
		for part in current:
			part.unmount()
		current[0].imageFedora(current)
		if len(current) == 1:
			debug("Drive only has one partition.....", 1)

	if SYNC_DRIVES:
		for part in current:
			if part.getPartNum() == 1:
				part.copyTools()
			else:
				part.debug( "Not copying to [" + part.getDev() + "] because this is not the first FAT32 partition\n", 2)

if __name__=="__main__":
	print "\n"
	print "Starting Script... " + ctime() + " \n"
	#Create the parser we'll use to get the command line arguments.
	parser = OptionParser(
		usage = "usage: %prog [options]",
		description = "USB Updater.")
		
	#Now, we'll define the possible arguments that the script will accept.
	parser.add_option("-f",
				"--force",
				action="store_true",
				dest = "force",
				default = False,
				help = "Forces the script to continue without user input.")

	parser.add_option("-d",
				"--debug",
				dest = "debug",
				default = 0,
				help = "Puts the script in debug mode.")
				
	parser.add_option("-e",
				"--email",
				action="store_true",
				dest = "email",
				default = False,
				help = "Sends an email after the execution is complete.")

	parser.add_option("-t",
				"--tools",
				action="store_true",
				dest = "copyTools",
				default = False,
				help = "Copies the latest tools found on the server to the drives.")

	parser.add_option("-i",
				"--image",
				action="store_true",
				dest = "image",
				default = False,
				help = "Images the drives with the latest image.")
					
	#Now we'll actually parse the arguments and set the proper variables.
	(options, args) = parser.parse_args()
	
	#This will build an email to send at execution termination
	if options.email == True: #technically shouldn't be checking if a boolean equals "True" but for readability will leave it in
		email = True
		#Remove the temporary email file if it already exists
		if os.path.isfile(emailFile):
			os.remove(emailFile)
		#Open a new temporary email file for writing
		emailBody = open(emailFile, "w")
		# Write the start time at the beginning
		emailBody.write("Started: " + ctime() + "\n\n")
		
	#This will turn on debug mode, which will give more verbose output.
	if options.debug == True:
		DEBUG_MODE = True
		
	if options.debug != 0:
		DEBUG_LEVEL = options.debug
		
	#This will enumerate the drives that we can work with	
	debug("Attempting to find the drives.", 1)
	enumerateDrives()

	#First, let's see if we should run silently with the force optionte
	if options.force == True:
		debug("I will run in \"Force\" mode, which is to say that I'm not going to ask for input.", 1)
		force = True

	#Now, we'll want to see if the drives should be imaged.
	if options.image == True:
		debug("Drives will be IMAGED!", 1)
		IMAGE_DRIVES = True

	#Now, we can see if we want to copy over the USB tools folders.
	if options.copyTools == True:
		debug("I'm going to copy the latest USB tools to the drives.", 1)
		syncUSBFolder()
		SYNC_DRIVES = True

	addDevs = [] # A list of devices (not partitions) to use for the keys of 'devices'
	for current in drives:
		devToAdd = current.getDev()[:-1]
		if devToAdd not in addDevs:
			addDevs.append(devToAdd)

	devices = {} # A dictionary of the form "device : [device_part1, device_part2, etc]"
	# Let's build the dictionary items
	for dev in addDevs:
		devices[dev] = []
	
	# Now let's populate the dictionary
	for current in drives:
		devToAdd = current.getDev()[:-1]
		devices[devToAdd].append(current)

	# Store a list of the processes so we know when they're complete
	processes = []

	# Go go gadget!
	for dev in devices:
		p = Process(target=processDrive, args=(devices[dev],))
		p.start()
		processes.append(p)

	#While there's still a process running...sleep a second
	for p in processes:
		while p.is_alive() and p.exitcode == None:
			sleep(1)
	
	#Now that all processes are done....
	for dev in devices:
		for part in devices[dev]:
			part.unmount()

	#Should we send an email?
	if email:
		# Make a notice that we're sending the email report		
		debug("Sending email report.", 1)
		# Actually send the email with the pre-built emailBody
		sendEmail(emailBody)
	
