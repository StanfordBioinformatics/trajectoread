#!/usr/bin/env python

"""
Release a lane to a customer by copying the contents of the current
project into a new project and transferring the project to the
specified user.

Dependencies:
-Expects the lims package to be in the system path. 
"""
import re
import sys
import dxpy
import time
import datetime
import subprocess
from scgpm_lims import Connection,RunInfo

#my_auth = app_utils.getSecurityAuth(conf.dashboardContributeToken) #use this for setting any properties/details on a dashboard record
token = "kKEI8Hb3g6k3gUqjD6ZrP9hoiTIUkNV7"
auth = dxpy.DXHTTPOAuth2({"auth_token_type": "Bearer", "auth_token": token})

class FlowcellLane:

	def __init__(self, project_dxid, record_dxid, dashboard_project_dxid, 
				 dx_user_id, user_first_name, user_last_name, user_email, 
				 viewers, release_note, lims_url, lims_token):
		# This is lane level stuff. Most of this info will be stored in dxrecord.
		if record_dxid and dashboard_project_dxid:
			self.record = dxpy.DXRecord(dxid=record_dxid, project=dashboard_project_dxid)
		else:
			self.record = None

		self.project_dxid = project_dxid
		self.dx_user_id = dx_user_id
		self.user_first_name = user_first_name
		self.user_last_name = user_last_name
		self.user_email = user_email
		self.viewers = viewers
		self.release_note = release_note
		self.lims_url = lims_url
		self.lims_token = lims_token

		# Values assigned during project transfer
		self.sponsored_datetime = None
		self.release_project_dxid = None
		self.clone_project_dxid = None

		# Values gotten from DXRecord
		self.properties = None
		self.details = None
		self.lane_index = None
		self.run_name = None
		self.library_name = None

		if self.record:
			self.properties = self.record.get_properties()
			self.details = self.record.get_details()

			self.parse_record_details()
			self.parse_record_properties()

	def parse_record_properties(self):
		if not self.lims_url:
			self.lims_url = self.properties['lims_url']
		if not self.lims_token:
			self.lims_token = self.properties['lims_token']

	def parse_record_details(self):
		# Get user first & last name
		if not self.user_first_name or not self.user_last_name:
			self.user = self.details['user']
			user_elements = self.user.split()
			self.user_first_name = user_elements[0]
			self.user_last_name = user_elements[1]

		if not self.project_dxid:
			self.project_dxid = self.details['laneProject']

		if not self.user_email:
			self.user_email = self.details['email']

		if not self.library_name:
			library_label = self.details['library']
			elements = library_label.split('rcvd')
			library_name = elements[0].rstrip()
			library_name = library_name.replace(' ', '-')
	        library_name = library_name.replace('_', '-')
        	library_name = library_name.replace('.', '-')
        	self.library_name = library_name

		self.lane_index = int(self.details['lane'])
		self.run_name = self.details['run']

	def sponsor_project(self, days):
		current_datetime = datetime.datetime.utcnow()
		# datetime.datetime object + datetime.timedelta object = datetime.datetime object.
		sponsored_datetime = current_datetime + datetime.timedelta(days=days)
		epoch = datetime.datetime.utcfromtimestamp(0)
		sponsored_milli_datetime = (sponsored_datetime - epoch).total_seconds() * 1000
		self.sponsored_datetime = sponsored_milli_datetime

		print 'Info: Sponsoring project %s for %d days' % (self.project_dxid, int(days))
		dx_sponsorship_input = {'sponsoredUntil': self.sponsored_datetime}
		dxpy.api.project_update_sponsorship(self.project_dxid, dx_sponsorship_input)
		# Specifying null (or any time in the past) terminates the sponsorship effective immediately.
		# Specifying a different number of (positive) days will update the time the sponsorship terminates.

	def update_project_description(self, text):
		dxpy.api.project_update(self.release_project_dxid, {"description": text})

	def clone_project(self, clone_project_name):
		clone_properties = {'library_name': self.library_name}
		clone_dx_project = dxpy.api.project_new({
												 "name": clone_project_name,
												 "properties": clone_properties
												}) 
		self.clone_project_dxid = clone_dx_project["id"]
		print 'Created project %s: %s' % (clone_project_name, self.clone_project_dxid)

		print 'Cloning root folder from %s into %s' % (self.project_dxid, 
													   self.clone_project_dxid)
		dxpy.api.project_clone(self.project_dxid, {
												   "folders": ["/"], 
												   "project": self.clone_project_dxid, 
												   "destination": "/"
												  })

	def transfer_clone_project(self, email):
		print 'Transferring project %s to user %s' % (self.clone_project_dxid, email)
		dxpy.api.project_transfer(self.clone_project_dxid, {"invitee": email, "suppressEmailNotification": False})
		
		if self.record:
			# Get UTC time in milliseconds
			release_date = str(int(round(time.time() * 1000)))
			self.record.set_properties({
										'status': 'released',
										'releaseDate': release_date
									   })

	def update_run_in_lims(self):

		# Only update LIMS on last lane of run (=8 or MiSeq)
		match_spenser = re.search('SPENSER', self.run_name)
		match_M04199 = re.search('M04199', self.run_name)
		if not int(self.lane_index) == 8 and not match_spenser and not match_M04199:
			print 'Info: Skipping LIMS update'
			return None

		# Get LIMS info
		if not self.lims_url or not self.lims_token:
			#try:
			#	lims_url = self.properties['lims_url']
			#	lims_token = self.properties['lims_token']
			#except:
			#	warning = 'Warning: Could not add DX User ID to LIMS; could not get '
			#	warning += 'LIMS URL/token information from DXRecord properties.'
			#	print warning
			#	return None
			warning = 'Warning: Could not update LIMS pipeline run info.'
			print warning
			return None

		# Establish connection to LIMS
		try:
			conn = Connection(lims_url=self.lims_url,lims_token=self.lims_token)
		except:
			warning = 'Warning: Could not establish connection to LIMS with...\n' 
			warning += 'LIMS URL: %s\n' % self.lims_url
			warning += 'LIMS Token: %s\n' % self.lims_token
			warning += 'Run status will not be updated in LIMS'
			print warning
			return None 
		
		# Mark "Finished" flag on Solexa Pipeline Run
		try:
			pipeline_runs = conn.indexpipelineruns(self.run_name)
			for run_id in pipeline_runs.keys():
				param_dict = {'finished': True}
				conn.updatepipelinerun(run_id, param_dict)
		except:
			warning = 'Warning: Could not update LIMS pipeline run info.'
			print warning

		# Mark Analysis/Notification/DNAnexus Done as True for Solexa run
		try:
			solexa_runs = conn.indexsolexaruns(self.run_name)
			for run_id in solexa_runs.keys():
				param_dict = {'analysis_done': True,
							  'notification_done': True,
							  'dna_nexus_done': True
							  }
				conn.updatesolexarun(run_id, param_dict)
		except:
			warning = 'Warning: Could not update LIMS solexa run info.'
			print warning
			print self.run_name

class User:

	def __init__(self, record_dxid, dashboard_project_dxid, dx_user_id, first_name, 
				 last_name, email, sunet_id, lims_url, lims_token):

		self.dx_user_id = dx_user_id
		self.email = email
		self.sunet_id = sunet_id
		self.first_name = first_name
		self.last_name = last_name
		self.record_dxid = record_dxid
		self.dashboard_project_dxid = dashboard_project_dxid
		
		self.details = None
		self.properties = None

		if self.record_dxid and self.dashboard_project_dxid:
			self.record = dxpy.DXRecord(dxid=record_dxid, project=dashboard_project_dxid)
			self.get_record_properties_details()

		if not self.email:
			print 'Error: No email address provided. Required to transfer to user'
			sys.exit()
		if not self.dx_user_id:
			self.create_dx_user_id()
			self.set_lims_dx_user_id(self.email, 
									 self.dx_user_id, 
									 lims_url, 
									 lims_token)

	def get_record_properties_details(self):
		## DEV: Use this to get record details if creating user_id fails
		self.properties = self.record.get_properties()
		self.details = self.record.get_details()

		if not self.email:
			self.email = self.details['email']

		if not self.first_name or not self.last_name:
			user = self.details['user']
			user_elements = user.split()
			self.first_name = user_elements[0]
			self.last_name = user_elements[1]

	def create_dx_user_id(self):

		if self.sunet_id and self.email:
			# Try generating user ID using SUNet ID
			try: 
				dx_user_id = self.sunet_id
				legal_dx_user_id = self.legalize_dx_user_id(dx_user_id)
				self.dx_user_id = self.ensure_new_user(self.email,
												   )
			except NameError:
				dx_user_id = self.email.split("@")[0]
				legal_dx_user_id = self.legalize_dx_user_id(dx_user_id)
		elif self.email:
			# Try generating user ID using email
			print 'Warning: Could not get SUNet ID for user. Trying email address.'
			try:
				dx_user_id = self.email.split("@")[0]
				legal_dx_user_id = self.legalize_dx_user_id(dx_user_id)
			except:
				print 'Error: Could not get SUNet ID nor email for user.' 
				print 'Cannot transfer project.'
				print 'Check details in project record.'
				print 'Record: %s, Project: %s' % (record_dxid, dashboard_project_dxid)
				sys.exit()
		else:
			# Go get the record.
			self.get_record_properties_details()
			self.create_dx_user_id()

		if not self.first_name or not self.last_name:
			# Get user name information
			self.user = self.details['user']
			user_elements = self.user.split()
			self.first_name = user_elements[0]
			self.last_name = user_elements[1]

		# Create DX User ID
		self.dx_user_id = self.ensure_new_user(email = self.email, 
											   dx_user_id = legal_dx_user_id, 
											   first_name = self.first_name, 
											   last_name = self.last_name
											  )
		#except EmailTakenError:
		#	print 'Warning: DNAnexus user already associated with email.' 
		#	print 'Using email %s to transfer project to user' % self.email

	def legalize_dx_user_id(self, proposed_dx_user_id):
		"""
		Function : As Joe Dale from DNAnexus put it, DNAnexus user names must:

								-be at least 3 characters long
								-be at most 255 characters long
								-begin with an English alphanumeric character
								-contain English alphanumeric characters, period, and underscore only

	             This function removes illegal characters, and then makes sure that all other contstraints are met. If all constraints are met,
	             the legalized version of the input will be returned (which could be identical to the input string if all was okay). If the input can't 
	             be legalized, i.e. doesn't have at least 3 characters, or doesn't start with an alphanumeric character, then the empty string is returned.
		Args     : suggested_username - str. 
		Returns  : str. 
		"""
		reg = re.compile(r'[^\w\.]')
		dx_user_id = reg.sub("", proposed_dx_user_id)
		if len(dx_user_id) < 3 or len(dx_user_id) > 255 or not re.match(r'\w',dx_user_id):
			raise NameError('Cannot create a DNAnexus username from the string %s' % dx_user_id)
		else:
			return dx_user_id

	def ensure_new_user(self, email, dx_user_id, first_name=None, last_name=None):
		"""
		Function : Given a suggested userid name for the DNAnexus platform, creates it in DNAnexus if that userid doens't already exist, otherwise
							 finds a unique userid by appending a number to the end (beginning with '2') and increcmenting that number as needed.
		Args     : email          - str. The email address of the user.
	               suggested_name - str. The first-choice name for a user ID in DNAnexus.
	 			   first_name     - str. First name of the user. Required if not last_name.
	               last_name      - str. Last name of the user. Required if not first_name.
		Returns  : str.
		"""
		print("Info: Checking for existing account associated with email: %s" % email)
		index = 1 
		while True:
			try:
				new_user_info = {
								 'username': dx_user_id, 
								 'email': email, 
								 'first': first_name, 
								 'last': last_name
								}
				print 'Info: New user information:'
				print new_user_info
				dxpy.DXHTTPRequest('https://auth.dnanexus.com' + '/user/new', new_user_info, prepend_srv=False)
				print("DX User ID available; created under username '%s'.\n" % dx_user_id)
				break
			except dxpy.exceptions.DXAPIError as e:
				if e.name == "UsernameTakenError":
					index = index + 1 
					dx_user_id = dx_user_id + str(index)
					continue
				elif e.name == "EmailTakenError":
					print "Warning: Email '%s' already associated with DNAnexus account." % email
					print 'Info: Transferring project to user with email %s' % email
					#raise EmailTakenError("Error: User email %s already associated with DNAnexus account.\n" % (email))
					dx_user_id = None
					break
				else:
					raise e
		return dx_user_id

	def set_lims_dx_user_id(self, email, dx_user_id, lims_url, lims_token):
		"""
		Function : Updates/sets the dnanexus_userid attribute of a Person record in UHTS.
		Args     : personid - The ID of a UHTS. Person record.
							  attributeDict - dict. Keys are Person attribute names
		Returns  : A JSON hash of the person specified by personid as it exists in the database after the record update(s).
		"""
		if not lims_url or not lims_token:
			try:
				lims_url = self.properties['lims_url']
				lims_token = self.properties['lims_token']
			except:
				warning = 'Warning: Could not add DX User ID to LIMS; could not get '
				warning += 'LIMS URL/token information from DXRecord properties.'
				print warning
				return None

		try:
			conn = Connection(lims_url=lims_url,lims_token=lims_token)
			user_attributes = conn.get_person_attributes_by_email(email=email)
			user_id = user_attributes['id']
			json = conn.update_person(personid = user_id,
							attributeDict = {"dnanexus_userid": dx_user_id})
			info = 'Info: LIMS updated with DNAnexus ID: %s ' % json['dnanexus_userid']
			info += 'for user: %s %s' % (json['first_name'], json['last_name'])	
			print info
			return json
		except:
			warning = 'Warning: Could not update LIMS user info '
			warning += 'with email %s' % email
			print warning

@dxpy.entry_point('main')
def main(project_dxid=None, record_dxid=None, dx_user_id=None, user_first_name=None, 
		 user_last_name=None, user_email=None, user_sunet_id=None, viewers=None, 
		 days=30, release_note=None, lims_url=None, lims_token=None,
		 dashboard_project_dxid='project-BY82j6Q0jJxgg986V16FQzjx', 
		 qc_pdf_report=None):

	if not release_note:
		release_note = ('Thank you for using the Stanford Center for Genomics ' +
			            'and Personalized Medicine (SCGPM) for your sequencing ' +
			            'needs. Your sequencing data will be sponsored on ' +
			            'DNAnexus, by SCGPM, for one month from this date. During ' +
			            'this time, you will be able to log into DNAnexus, access ' +
			            'this project, and download data from it, free of charge. ' +
			            'You may accept transfer of the project any time within the ' +
			            'sponsorship window.'
			            'If you do not accept transfer of the project, you will not ' +
			            'be able to access the data after the sponsorship period of ' +
			            'one month. '
			            'Once you "Accept Transfer" of this project, ' +
			            'ownership of the project will transfer to you ' +
			            'and you will be responsible for any future data transfer, ' +
			            'storage, and compute charges.')

	print 'Info: Creating lane object associated with project: %s' % project_dxid
	lane = FlowcellLane(project_dxid, record_dxid, dashboard_project_dxid, 
					    dx_user_id, user_first_name, user_last_name, user_email, 
					    viewers, release_note, lims_url, lims_token)
	print 'Info: Creating user object associated with email: %s' % user_email
	user = User(record_dxid, dashboard_project_dxid, dx_user_id, user_first_name, 
				user_last_name, user_email, user_sunet_id, lims_url, lims_token)

	lane.sponsor_project(days=days)
	
	## DEV: This function is broken 4/5/2016
	#if release_note:
	#	lane.update_project_description(release_note)
	
	# Create clone of project for release to user
	dx_project = dxpy.DXProject(dxid=lane.project_dxid)
	release_project_name = '%s_%s' % (dx_project.name, lane.library_name)
	lane.clone_project(release_project_name)
	lane.transfer_clone_project(user.email)
	lane.update_run_in_lims()

dxpy.run()
