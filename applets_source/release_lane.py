#!/usr/bin/env python

"""
Release a lane to a customer by copying the contents of the current
project into a new project and transferring the project to the
specified user.

Dependencies:
-Expects the script maindrill_send.py to be in the system path. 
-Expects the lims package to be in the system path. 
"""
import re
import sys
import dxpy
import datetime
import subprocess
from scgpm_lims import Connection,RunInfo
from pipeline_utils import conf, app_utils

my_auth = app_utils.getSecurityAuth(conf.dashboardContributeToken) #use this for setting any properties/details on a dashboard record

class FlowcellLane:

	def __init__(self, project_dxid, record_dxid, dashboard_project_dxid, dx_user_id, 
				 user_first_name, user_last_name, user_email, viewers, release_note):
		# This is lane level stuff. Most of this info will be stored in dxrecord.
		self.record = dxpy.DXRecord(dxid=record_dxid, project=dashboard_record_dxid)
		self.properties = self.record.get_properties()
		self.details = self.record.get_details()

		self.project_dxid = project_dxid
		self.dx_user_id = dx_user_id
		self.user_first_name = user_first_name
		self.user_last_name = user_last_name
		self.user_email = user_email
		self.viewers = viewers
		self.release_note = release_note

		self.parse_record_details()
		self.parse_record_properties()

		self.sponsored_datetime = None
		self.release_project_dxid = None
		self.clone_project_dxid = None

	def parse_record_details(self):
		
		# Get user first & last name
		if not self.recipient_first_name or not self.recipient_last_name:
			self.user = self.details['user']
			user_elements = self.user.split()
			self.user_first_name = user_elements[0]
			self.user_last_name = user_elements[1]

		if not self.project_dxid:
			self.project_dxid = self.details['laneProject']

		if not self.user_email:
			self.user_email = self.details['email']

	def sponsor_project(self, days):
		current_datetime = datetime.datetime.utcnow()
		# datetime.datetime object + datetime.timedelta object = datetime.datetime object.
		sponsored_datetime = current_datetime + datetime.timedelta(days=days)
		epoch = datetime.datetime.utcfromtimestamp(0)
		sponsored_milli_datetime = (sponsored_datetime - epoch).total_seconds() * 1000
		self.sponsored_datetime = sponsored_milli_datetime

		dx_sponsorship_input = {'sponsoredUntil': self.sponsored_datetime}
		dxpy.api.project_update_sponsorship(self.project_dxid, dx_sponsorship_input)
		# Specifying null (or any time in the past) terminates the sponsorship effective immediately.
		# Specifying a different number of (positive) days will update the time the sponsorship terminates.

	def update_project_description(self, text):
		dxpy.api.project_update(self.release_project_dxid, {"description": text})

	def clone_project(self, dest_project_name):
		dest_dx_project = dxpy.api.project_new({"name": dest_project_name}) #returns a dict with the sole key being 'id'
		dest_project_dxid = dest_dx_project["id"]
		print 'Created project %s: %s' % (dest_project_name, dest_project_dxid)

		print 'Cloning root folder from %s into %s' % (self.project_dxid, dest_project_dxid)
		dxpy.api.project_clone(self.project_dxid, {"folders": ["/"], 
												   "project": dest_project_dxid, 
												   "destination": "/"
												  })

	def transfer_clone_project(self, email):
		print 'Transferring project %s to user %s' % (self.clone_project_dxid, email)
		try:
			dxpy.api.project_transfer(self.clone_project_dxid, {"invitee": email, "suppressEmailNotification": False})
		except:
			print 'Error: Could not transfer project %s to user email: %s' % (self.clone_project_dxid, email)

class User:

	def __init__(self, record_dxid, dashboard_project_dxid, dx_user_id, first_name, 
				 last_name, user_email):

		self.dx_user_id = dx_user_id
		self.email = email
		self.sunet_id = sunet_id
		self.first_name = first_name
		self.last_name = last_name

		# DEV: A user should not have lane record attributes.
		self.dx_record = None
		self.details = None
		self.properties = None

		if not self.dx_user_id:
			self.create_dx_user_id(record_dxid, dashboard_project_dxid)
			self.set_lims_dx_user_id(self.email, self.dx_user_id)

	def create_dx_user_id(self, record_dxid, dashboard_project_dxid):
		self.dx_record = dxpy.DXRecord(dxid=record_dxid, project=dashboard_project_dxid)
		self.details = dx_record['details']
		self.properties = dx_record['properties']
		
		# Propose new DX User ID based on user SUNet or email
		try:
			self.sunet_id = details['sunet_id']
			dx_user_id = self.sunet_id
			self.email = details['email']
		except:
			print 'Warning: Could not get SUNet ID for user. Trying email address.'
			try:
				self.email = details['email']
				dx_user_id = self.email.split("@")[0]
			except:
				print 'Error: Could not get SUNet ID nor email for user.' 
				print 'Cannot transfer project.'
				print 'Check details in project record.'
				print 'Record: %s, Project: %s' % (record_dxid, dashboard_project_dxid)
				sys.exit()

		if not self.recipient_first_name or not self.recipient_last_name:
			self.user = self.details['user']
			user_elements = self.user.split()
			self.user_first_name = user_elements[0]
			self.user_last_name = user_elements[1]

		legal_dx_user_id = self.legalize_dx_user_id(dx_user_id)
		if not legal_dx_user_id:
			raise Exception("Can't create a DNAnexus username from the string {un}".format(un=dx_user_id))

		self.dx_user_id = self.ensure_new_user(self.email, 
											   legal_dx_user_id, 
											   first_name, 
											   last_name
											  )

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
			return None
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
		print("*** Checking if there exists a user account associated with email '%s'..." % email)
		index = 1 
		while True:
			try:
				new_user_info = {
								 'username': dx_user_id, 
								 'email': email, 
								 'first': first_name, 
								 'last': last_name
								}
				dxpy.DXHTTPRequest(dxpy.get_auth_server_name() + "/user/new", new_user_info, prepend_srv=False)
				print("DX User ID available; created under username '%s'.\n" % username)
				break
			except dxpy.exceptions.DXAPIError as e:
				if e.name == "UsernameTakenError":
					index = index + 1 
					dx_user_id = dx_user_id + str(index)
					continue
				elif e.name == "EmailTakenError":
					print("Error: User email %s already associated with DNAnexus account.\n" % (email))
					break
				else:
					raise e
		return dx_user_id

	def set_lims_dx_user_id(self, email, dx_user_id):
		"""
		Function : Updates/sets the dnanexus_userid attribute of a Person record in UHTS.
		Args     : personid - The ID of a UHTS. Person record.
							  attributeDict - dict. Keys are Person attribute names
		Returns  : A JSON hash of the person specified by personid as it exists in the database after the record update(s).
		"""

		try:
			lims_url = self.properties['lims_url']
			lims_token = self.properties['lims_token']
		except:
			warning = 'Warning: Could not add DX User ID to LIMS; could not get '
			warning += 'LIMS URL/token information from DXRecord properties.'
			return None

		conn = Connection(lims_url=lims_url,lims_token=lims_token)
		user_attributes = conn.get_person_attributes_by_email(email=email)
		user_id = user_attributes['id']
		jsonResult = conn.update_person(personid=user_id,attributeDict={"dnanexus_userid": dx_user_id})	
		return jsonResult

@dxpy.entry_point('main')
def main(project_dxid, record_dxid=None, dx_user_id=None, user_first_name=None, 
		 user_last_name=None, user_email=None, viewers=None, days=30,
		 release_note=None, dashboard_project_dxid='project-BY82j6Q0jJxgg986V16FQzjx'):

	user = User(lane.record, dx_user_id, user_first_name, user_last_name, user_email)
	lane = FlowcellLane(project_dxid, record_dxid, dashboard_project_dxid, dx_user_id, 
						user_first_name, user_last_name, user_email, viewers, release_note)

	lane.sponsor_project(days=days)
	if release_note:
		lane.update_project_description(release_note)
	
	# Create clone of project for release to user
	dx_project = dxpy.DXProject(dxid=lane.project_dxid)
	release_project_name = '%s_release' % dx_project.name
	lane.clone_project(release_project_name)
	lane.transfer_clone_project

dxpy.run()
main(project_dxid="project-BjgB8Q80G7320ZJxK8pfyx8J", dx_user_id=None, user_first_name="Paul", user_last_name="Billing-Ross", user_email="billingross@gmail.com", viewers=None, release_note=None)
	
