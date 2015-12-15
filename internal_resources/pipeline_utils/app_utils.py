import re
import os
import dxpy
import conf
import subprocess
import datetime
from scgpm_lims import Connection
from gbsc_utils import gbsc_utils #module load gbsc/gbsc_utils
import json
APPLETS_PROJECT_ID = conf.appletsProjectID
PIPELINE_ATTR_TYPES = conf.attrTypes


#########################################################################################################################
###SOME UHTS WRAPPERS

def get_runinfo_dict(runName,lane=None):
	"""
	Function : Gets the runinfo dict provided by the scgpm_utils package for a given run. Returns the entire runinfo dict, unless 'lane' is
             provided, in which case the sub-lane dict is returned.
	Args     : runName - str. Name of sequencing run.
						 lane - int or str. Number of the lane (i.e. 1,2,3,4,5,6,7,8).
	Returns  : dict.
	"""
	lims_url,lims_token = get_lims_credentials()
	conn = Connection(lims_url=lims_url,lims_token=lims_token)
	ri = conn.getruninfo(run=runName)['run_info']
	if lane:
		return ri['lanes'][str(lane)]
	else:
		return ri

def get_mapping_reference(runName,lane):
	"""
	Function : Finds the mapping reference name (will be a name from UHTS shown in the "Name" columna at https://uhts.stanford.edu/admin/reference_sequences.
	Returns  : str.
	"""
	reference = None
	lane_info = get_runinfo_dict(runName=runName,lane=lane)	
	if 'mapping_requests' in lane_info:
		reference  = lane_info['mapping_requests'][0]['reference_sequence_name']
	return reference

def get_mapping_program(runName,lane):
	"""
	Fetches the mapping program from UHTS for the given lane.
	The list of possible mapping programs in UHTS is defined in the helper module app/helpers/mapping_program.rb.
	"""
	mapper = None
	lane_info = get_runinfo_dict(runName=runName,lane=lane)
	if "mapping_requests" in lane_info:
		mapper = lane_info['lanes'][str(lane)]['mapping_requests'][0]['mapping_program']
		#Note that I indexed mapping_requests with the 0th element b/c that is a list of dicts, where
		# each dict is a mapping request. I don't know why UHTS was built to allow multiple mapping requsts like this since we don't support it in the pipeline, and
		# we don't have a need to. So I'll just always take the 0th element. 
	return mapper

def get_sample_name(runName,lane):
	lane_info = get_runinfo_dict(runName=runName,lane=lane)	
	return lane_info['sample_name']

def get_submitter_name(runName,lane):
	lane_info = get_runinfo_dict(runName=runName,lane=lane)	
	return lane_info['submitter']

def get_submitter_email(runName,lane):
	lane_info = get_runinfo_dict(runName=runName,lane=lane)
	return lane_info['submitter_email'].strip()

def get_lab_name(runName,lane):
	lane_info = get_runinfo_dict(runName=runName,lane=lane)
	return lane_info['lab']

def get_transfer_viewers(runName,lane):
	"""
	Function : Fetches all emails addresses listed from UHTS for a given lane from the runinfo dict return from scgpm_lims.Connection.getruninfo.
   	  The emails are from the values of the keys 'notify' and 'notify_comments' that are within the lane subdict of the runinfo dict. 
  	  Currently, the 'notify' key is only used with Ghia submissions, whereas the 'notify_comments' is only used with Ziming submissions.
  	  The value of the 'notify' key is a list of dicts, where each dict has an email key. The value of the 'notify_comments' key is a string.
  	  Normally, each space-separated key word in notify_comments is an email address, but there isn't anything enforcing this and sometimes some 
  	  free text can be found there. To try and pull out only the email addresses from that string, I keep only the words that have a '@' in them.
  	
  	  The 'notify_comments' content for a lane is displayed in UHTS on the run page in the column named Notify in the table called 
  	  'Information for Returning Results'. Most of the time, this is a list of email addresses for CC'ing result emails. But
  	  there isn't anything enforcing this, and sometimes there is non-email address text there that needs to be filtered out.
  
  	  The 'notify' content for a lane is displayed in UHTS on the dna_library page for a lane in the section of the web page called "People to Notify".
	Args    : runName - str. Name of the sequencing run.
						lane    - str. or int. The number of the lane of interest on the sequencing run.
	Returns : A unique list of email addresses as strings. Each address only appears once in the list.
	"""
	lane_info = get_runinfo_dict(runName=runName,lane=lane)
	notify = lane_info['notify']
	emails = []
	emails = [x['email'].strip() for x in notify]
	try:
		notify_comments = lane_info['notify_comments'] #key only used with Ziming lanes.
		notify_comments = notify_comments.strip().split(",")
		notify_comments = [x.strip() for x in notify_comments]
		notify_comments = [x for x in notify_comments if "@" in x]
		emails.extend(notify_comments)
	except KeyError:
		pass
	set_emails = set(emails)
	return list(emails)

def getPersonAttrsByEmail(personEmail):
	"""
	Function :
	Args     : personEmail - the value of the 'email' attribute of a Person record in the UHTS.Person table.	
	Returns  : str.
	"""
	lims_url,lims_token = get_lims_credentials()
	conn = Connection(lims_url=lims_url,lims_token=lims_token)
	personAttrs = conn.get_person_attributes_by_email(email=personEmail)
	return personAttrs

def getPersonDnanexusUserid(personAttrs):
	"""
	Function :
	Args     : personAttrs - dict. as return by getPersonAttrsByEmail().
	Returns  : str.
	"""
	return personAttrs['dnanexus_userid']

def legalizeSuggestedDnanexusUsername(suggested_username):
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
	username = reg.sub("",suggested_username)
	if len(username) < 3 or len(username) > 255 or not re.match(r'\w',username):
		return ""
	else:
		return username
	
def new_user(username, email, first, last):
	return dxpy.DXHTTPRequest(dxpy.get_auth_server_name() + "/user/new", {"username": username, "email": email, "first": first, "last": last}, prepend_srv=False);

def ensure_new_user(email, suggested_username, first_name=None, last_name=None):
	"""
	Function : Given a suggested userid name for the DNAnexus platform, creates it in DNAnexus if that userid doens't already exist, otherwise
						 finds a unique userid by appending a number to the end (beginning with '2') and increcmenting that number as needed.
	Args     : email        - str. The email address of the user.
             suggested_name - str. The first-choice name for a user ID in DNAnexus.
 						 first_name   - str. First name of the user. Required if not last_name.
             last_name    - str. Last name of the usr. Required if not first_name.
	Returns  : str.
	"""
	print("*** Checking if there exists a user account associated with email '%s'..." % email)
	index = 1 
	username = suggested_username
	while True:
		try:
			new_user(username, email, first_name, last_name)
			print("no; created one under username '%s'.\n" % username)
			break
		except dxpy.exceptions.DXAPIError as e:
			if e.name == "UsernameTakenError":
				index = index + 1 
				username = suggested_username + str(index)
				continue
			elif e.name == "EmailTakenError":
				print("yes, already there.\n")
				break
			else:
				raise e
	return username

def setDnanexusUserid(personid,dnanexus_userid):
	"""
	Function : Updates/sets the dnanexus_userid attribute of a Person record in UHTS.
	Args     : personid - The ID of a UHTS.Person record.
						 attributeDict - dict. Keys are Person attribute names
	Returns  : A JSON hash of the person specified by personid as it exists in the database after the record update(s).
	"""
	lims_url,lims_token = get_lims_credentials()
	conn = Connection(lims_url=lims_url,lims_token=lims_token)
	jsonResult = conn.update_person(personid=personid,attributeDict={"dnanexus_userid": dnanexus_userid})	
	return jsonResult

def getPersonSunetid(personAttrs):
	"""
	Function :
	Args     : personAttrs - dict. as return by getPersonAttrsByEmail().
	Returns  : str.
	"""
	return personAttrs['sunetid']
	

#########################################################################################################################
###SOME dxpy WRAPPERS

class InvalidPipelineAttrException(Exception):
  pass

class InvalidScgpmrunRecordAttrException(Exception):
  pass


def find_and_download_file(projectid,folder,name):
	res = dxpy.find_data_objects(classname="file",project=projectid,folder=folder,name=name)	
	res = list(res)
	num = len(res)
	if num > 1:
		raise Exception("Found {num} files by the name of {name} that live in the project {projectid} and folder {folder}.".format(num=num,name=name,projectid=projectid,folder=folder))
	else:
		res = res[0]
		#res is a dict with the keys 'project' and 'id'
	dxfile = dxpy.DXFile(dxid=res['id'],project=res['project'])
	dxpy.download_dxfile(dxid=dxfile.id,project=dxfile.project,filename=dxfile.name)
	
	

def download_file(dx_file):
	"""
	Args    : dx_file - a file object ID on DNAnexus to the current working directory.
	Returns : str. Path to downloaded file.
	"""
	dx_file = dxpy.DXFile(dx_file)
	fn = dx_file.describe()['name']
	dxpy.download_dxfile(dx_file.get_id(), fn)
	return fn


def get_dashboard_record_object(dashboard_record_id):
	if ':' in dashboard_record_id:
		(project_id, record_id) = dashboard_record_id.split(':')
		dashboard_record = dxpy.DXRecord(dxid=record_id, project=project_id)
	else:
		dashboard_record = dxpy.DXRecord(project=conf.dashboardProjectID,dxid=dashboard_record_id)
	return dashboard_record

def set_property(dxobject,name,value,token=None):
	"""
	Function : Sets a property on a DNAnexus object
	Args     : dxobject - a DNAnexus object
						 token - str. authentication token
	"""
	auth = None
	if token:
		auth = getSecurityAuth(token)
	props = dxobject.get_properties()
	props[name] = value
	dxobject.set_properties(props,auth=auth)

def set_properties(dxobject,props,token=None):
	"""
	Function : Sets one or more properties on a DNAnexus object.
	Args     : props - a dict.
					   token - str. authentication token.
	"""
	auth = None
	if token:
		auth = getSecurityAuth(token)
	currentProps = dxobject.get_properties()
	currentProps.update(props)
	dxobject.set_properties(currentProps,auth=auth)

def record_split_project(recordid):
	"""
	Function : Given a record id that may be prefixed with the project ID and a ":", splits on the ":" to return the project ID and the record ID.
						 If there isn't a colon delimiter, the project ID value will be returned as None.
	Args     : recordid - str. A DNAnexus record ID. Must be prefixed with the project ID and a colon.
	Returns  : two-item list being the project ID and the record ID, respectively.
	"""
	try:
			proj,rec = recordid.split(":")
	except ValueError:
		proj = None
		rec = recordid
	return [proj,rec]

def set_record_properties(recordid,properties):
	"""
	Function : Does not overwrite existing properties that aren't specified in this request. 
	Args     : recordid - str. A DNAnexus record ID. Must be prefixed with the project ID and a colon.
                        Ex: project-BY82j6Q0jJxgg986V16FQzjx:record-BfZf2kQ0jJxgXQVxP5pB4131
						 properties  - dict.
	"""
	projectid,recordid = record_split_project(recordid)
	dxpy.api.record_set_properties(object_id=recordid,input_params={"project":projectid,"properties":properties})
	##If you have a record object, can say record.set_properties().
	##There isn't a corresponding dxpy.api.record_get_properties() call yet. But if you have a record object, can say record.get_properties().

def get_record_properties(recordid):
	"""
	Function : Retrieves the properties that are set on a record object.
	Args     : recordid - str. A DNAnexus record ID. Must be prefixed with the project ID and a colon.
	Returns  : dict.
	"""
	rec = get_dashboard_record_object(dashboard_record_id=recordid)
	return rec.get_properties()

def set_record_details(recordid,details):
	"""
	Function : Does not overwrite existing details that aren't specified in this request.
	Args     : recordid - str. A DNAnexus record ID, which may be prefixed with the project ID and a colon.
                        Ex: project-BY82j6Q0jJxgg986V16FQzjx:record-BfZf2kQ0jJxgXQVxP5pB4131
						 details  - dict.
	"""
	projAndRecId = recordid
	projectid,recordid = record_split_project(recordid)
	existingDetails = get_record_details(projAndRecId)
	existingDetails.update(details)
	dxpy.api.record_set_details(object_id=recordid,input_params=existingDetails) #returns the record ID
	##if it were a record object, could have used record.set_details()

def get_record_details(recordid):
	"""
	Function : Fetches the details as a dict from the specified DNAnexus record.
	Args     : recordid - str. A DNAnexus record ID, which may be prefixed with the project ID and a colon.
	Returns  : dict.
	"""
	projectid,recordid = record_split_project(recordid)
	return dxpy.api.record_get_details(object_id=recordid)
	##if it were a record object, could have used record.get_details()

def set_project_properties(projectid,properties):
	""" 
	Function  : Unfortunately, dxpy doesn't currenlty support a set_property() or get_property() calls on a dxpy.bindings.dxproject.DXProject object.
						  That's becuase DXProject isn't a descendent of the dxpy.bindings.DXDataObject class, like it should be (since most all other data-like objects are). 
              Each value of a key must either be a str. or null, or else there will be a dxpy.exceptions.InvalidInput exception.
	 Args     : projectid - str. A DNAnexus project ID.
							properties - dict. where each key is a property name and its value is the value to set the property to in in DNAnexus project.
	"""
	print("Setting properties {properties} for project {projectid}".format(projectid=projectid,properties=properties))
	dxpy.api.project_set_properties(object_id=projectid,input_params={"properties": properties}) #this call returns the ID of the project

def get_project_properties(projectid):
	""" 
	Function : Returns the properties of a propject as a dict.
	Args     : project - str. A DNAnexus project ID
	Returns  : dict.
	"""
	return dxpy.api.project_describe(object_id=projectid, input_params={"fields": {"properties": True}})["properties"]

def get_lims_credentials():
	project = dxpy.DXProject(APPLETS_PROJECT_ID)
	properties = dxpy.api.project_describe(APPLETS_PROJECT_ID, {"fields": {"properties": True}})["properties"]
	lims_url = properties['lims_url']
	lims_token = properties['lims_token']
	return (lims_url, lims_token)


def getSecurityAuth(token):
	auth = dxpy.DXHTTPOAuth2({"auth_token_type": "Bearer", "auth_token": token})
	return auth

# This function will print the full command line to be executed and also
# save the tool name (assumed to be the first portion of the command)
# and the command line.
def run_cmd(cmd, logger, shell=True):
	if shell:
		save_cmd = cmd 
	else:
		save_cmd = subprocess.list2cmdline(cmd)
	logger.append(save_cmd)
	print save_cmd
	subprocess.check_call(cmd, shell=shell)


def find_applet_by_name(applet_name,zero_ok=False,more_ok=False):
	""" 
	Looks up an applet by name.
	"""
	found = dxpy.find_one_data_object(classname="applet", name=applet_name, project=conf.appletsProjectID, folder=conf.appletsFolder, zero_ok=zero_ok, more_ok=more_ok, return_handler=True)
	print("Resolved %s to %s" % (applet_name, found.get_id()))
	return found

def find_file_with_props(projectid,folder="/",properties=None):
	found = dxpy.find_data_objects(classname="file",project=projectid,folder=folder,properties=properties)
	return found

def find_one_job(project,req_name, req_properties):
	"""
	Finds a job in the given project with the given name and properties.
	If there are multiple jobs matching these criteria, then the most recent job is returned (determined by oldes creation date).
	"""
	jobs = dxpy.find_jobs(project=project, name=req_name, properties=req_properties, describe=True) #returns a generator
	newestJob = (0,0) # (describe dict, create time)
	for i in jobs:
		#i is a two item dictionary containing keys "describe" and "id". The former key contains the dict. returned by dx describe
		describe = i["describe"]
		date = describe['created']
		if date > newestJob[1]:
			newestJob = (describe,date)
	return newestJob[0] #return the describe dict for the newest job

def getCurrentUser():
	user = dxpy.api.system_whoami()['id']
	return user

def getPendingTransfers():
	"""
	Function : Returns the pending project transfers for the logged-in user. Note that at https://wiki.dnanexus.com/API-Specification-v1.0.0/Users#API-method%3A-%2Fuser-xxxx%2Fdescribe, it says:
						 "When /user-xxxx/describe is invoked by any user other than user-xxxx, or by a non full-scope token, only the ID, class, first, middle, last, and handle fields are returned".
             Therefore, this function doesn't take an input user argument as that isn't useful.
	Args     : userid - the dx user ID.
	Returns  :
	"""
	currentUser = getCurrentUser()	
	describe = dxpy.api.user_describe(object_id=currentUser,input_params={"pendingTransfers":True})	
	pendingTransfers = describe['pendingTransfers'] #empty list if no pending transfers
	return pendingTransfers
	
def getOrgDetails(org="org-scgpm"):
	"""
	Function : Invokes the /org-xxxx/describe API method.
	Args     : org - str. The DNAnexus org id.
	Return   : dict.
	"""
	cmd = "dx api {org} describe".format(org=org)
	stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,checkRetcode=True)
	result = json.loads(stdout)
	return result

def getOrgAdmins(org="org-scgpm"):
	"""
	Function :
	Args     : org - str. The DNAnexus org id.
	Returns  : list.
	"""
	details = getOrgDetails(org=org)
	return details['admins']	

def inviteUserToOrg(username,org="org-scgpm"):
	"""
	Args : org - The DNANexus org id.
				 username - The DNANexus username of the user to add to the org.
	"""
	
	cmd = "dx add member --level MEMBER --allow-billable-activities {org} {username}".format(org=org,username=username)
	stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,checkRetcode=True)
	print(stdout)

def getMemberAccess(userid,org="org-scgpm"):
	"""
	Function : Gets the permission level of a user in an org. Invokes the /org-xxxx/getMemberAccess API method.
	Args     : org - str. The DNAnexus org ID.
						 userid - the DNAnexus user ID.
	Returns  : str.
	"""
	inputs = "{{\"user\": \"{userid}\"}".format(userid=userid)
	cmd = "dx api {org} getMemberAccess '{inputs}'".format(org=org,inputs=inputs)	
	#print(cmd)
	stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,checkRetcode=True)
	result = json.loads(stdout)
	return result['level']
	
	
def addUserToOrg(userid,level="MEMBER",org="org-scgpm"):
	"""
	Function : Invites a user to an org. Invokes the /org-xxxx/invite API method.
	Args     : org - str. The DNAnexus org id (Must have the 'user-' prefix).
						 level - Membership status that the invitee will receive (one of "MEMBER" or "ADMIN"), where the default is MEMBER.
	Returns  :
	"""
	inputs = "\\\"invitee\\\": \\\"{userid}\\\",\\\"level\\\": \\\"{level}\\\"".format(userid=userid,level=level)
	inputs = "{" + inputs + "}"
	cmd = "dx api {org} invite \"{inputs}\"".format(org=org,inputs=inputs)
	print(cmd)
	#print(cmd)
	stdout,stderr = gbsc_utils.createSubprocess(cmd=cmd,checkRetcode=True)
	result = json.loads(stdout)
	return result
	
def getProjectName(project_id):
	desc = dxpy.api.project_describe(project_id)
	return desc['name']

def isProjectSponsored(project_id):
	""" 
	Function :
	Args     :
	Returns  :
	"""
	desc = dxpy.api.project_describe(project_id)
	if "sponsoredUntil" in desc: #sponsoredUntil, if set, is a timestamp
		return True
	return False

def sponsorProject(project_id,days):
	sponsorTime = getSponsorhipTimeSinceEpoch(days)
	dxpy.api.project_update_sponsorship(project_id, {"sponsoredUntil": sponsorTime})
	# Specifying null (or any time in the past) terminates the sponsorship effective immediately.
	# Specifying a different number of (positive) days will update the time the sponsorship terminates.

def getSponsorhipTimeSinceEpoch(days):
	"""
	Function : Given a number of days to sponsor a project, calculates the number of milliseconds that exist between the start of the epoch (UTC; which is equal to 
             datetime.datetime.utcfromtimestamp(0)) until the present time plus the number of days specified (days could be positive or negative).
	Args     : days - int. Can be positive or negative. 
	Returns  : int. The number of milliseconds since the epoch until sponsorship ends.
	"""
	now_datetime = datetime.datetime.utcnow()
	sponsored_datetime = now_datetime + datetime.timedelta(days=days) # datetime.datetime object + datetime.timedelta object = datetime.datetime object.
	sponsored_millisecond_time = (sponsored_datetime - datetime.datetime.utcfromtimestamp(0)).total_seconds() * 1000
	# datetime.datetime.utcfromtimestamp(0) returns datetime.datetime(1970, 1, 1, 0, 0)
	return sponsored_millisecond_time


def deleteExistingDashboardRunRecords(run_name,lane):
	"""
	Function : Deletes a record of type SCGPMRun that exists in conf.dashboardProject.
	Args     : run_name - str. Name of the sequencing run in question.
						 lane - int. Number of the lane sequenced in question.
						 deleteExisting - bool. When True, deletes any existing record of type SCGPMRun that lives in conf.dashboardProjectID that has the same name as the newly generated one would have.
	Returns  : str. The ID of the new record.
	"""
	dashboardRecordName = createDashboardRecordName(run_name=run_name,lane=lane)
	records_found = dxpy.find_data_objects(classname="record", name=dashboardRecordName, project=conf.dashboardProjectID, return_handler=True)
	dashboard_project = dxpy.DXProject(dxid=conf.dashboardProjectID)
	dashboard_project.remove_objects(objects=[x.id for x in records_found]) #if empty list here, then nothing happens


def removeFile(projectid,fileid):
	fle  = dxpy.DXFile(dxid=fileid,project=projectid)
	fle.remove()

def createLaneProjectName(run_name,lane):
	lane = str(lane)
	return run_name + " lane " + lane

def createDashboardRecordName(run_name,lane):
	"""
	Function : Creates a name for a dashboard record following the naming convention in the pipeline.
	Args     : run_name - str. Name of the sequencing run in question.
						 lane - int. Number of the lane sequenced in question.
	Returns  : str. A name for a dashboard record.
	"""
	recName = run_name + " lane " + lane
	return recName

def createLaneProject(run_name,lane):
	projName = createLaneProjectName(run_name=run_name,lane=lane)
	existingProjects = dxpy.find_projects(name=projName,level="VIEW",return_handler=True)	
	for i in existingProjects:
		i.destroy()
	newproj = dxpy.api.project_new(input_params={"name":projName}) #returns ID of new project
	return newproj['id']
	#to do the above in BASH:
	## laneProjectsToRemove=$(dx find projects --brief --level VIEW --name "${laneProjectName}")
	## if [[ -n $laneProjectsToRemove ]]
	## then
	##  dx rmproject -y ${laneProjectsToRemove}
	## fi  
	## lane_project_id=$(dx new project "${laneProjectName}" --brief --select)

			
def createDashboardRunRecord(run_name,lane,deleteExisting=True):
	"""
	Function : Creates a new record of type SCGPMRun. The record is named according to the pipeline convention.
	Args     : run_name - str. Name of the sequencing run in question.
						 lane - int. Number of the lane sequenced in question.
						 deleteExisting - bool. When True, deletes any existing record of type SCGPMRun that lives in conf.dashboardProjectID that has the same name as the newly generated one would have.
	Returns  : str. The ID of the new record prefixed with the project ID in which it exists.
	"""
	recName = createDashboardRecordName(run_name=run_name,lane=lane)
	if deleteExisting:
		deleteExistingDashboardRunRecords(run_name,lane)
	recordid = dxpy.api.record_new(input_params={"project":conf.dashboardProjectID,"name":recName,"types":[conf.dashboardRecordTypeName]})['id']
	return conf.dashboardProjectID + ":" + recordid
	
def isProperty(name):
	try:
		val = PIPELINE_ATTR_TYPES[name]
	except KeyError:
		raise KeyError("Attribute '{name}' is not a recognized pipeline attribute.".format(name=name))
	if val == 1:
		return True
	return False


def isDetail(name):
	try:
		val = PIPELINE_ATTR_TYPES[name]
	except KeyError:
		raise KeyError("Attribute '{name}' is not a recognized pipeline attribute.".format(name=name))
	if val == 2:
		return True
	return False

def set_dashboard_record_attributes(recordid,attrs):
	"""
	Function : Sets one or more attributes on a DNAnexus record object of type SCGPMRun. An attribute is implemented as either a detail or a property.
               Details are immutable once the record has been closed; properties are not. A record cannot be re-opened once it has been closed.
               Valid attribute names are contained within the dict PipelineUtilities.pipelineAttributeNames.
	Args     : recordid - str. A DNAnexus record ID.
						 attrs    - dict. Keys are record attribute names.
	Raises   : InvalidScgpmrunRecordAttrException if an invalid attribute name is provided.
	"""
	for key in attrs:
		if isProperty(key):
			set_record_properties(recordid=recordid,properties={key:attrs[key]})
		elif isDetail(key):
			set_record_details(recordid=recordid,details={key:attrs[key]})
		else:
			errMsg = "Warning from {here} in DxPipelineUtils.set_record_attribute(): Can't set attribute '{name}' with value '{value}' on DNAnexus record '{recordid}' because '{name'} isn't an allowed property or detail of that record.".format(here=os.path.basename(__file__),name=key,value=attrs[key],recordid=recordid)
			sys.stderr.write("\n" + errMsg + "\n\n")
			raise InvalidScgpmrunRecordAttrException(msg)

def get_dashboard_record_attributes(recordid):
	"""
	Function : Returns a dict containing the union of record details and propertis.
						 Iny any detail and property share the same name, the detail value will be returned and not that of the property.
	Args     : recordid - str. A DNAnexus record ID.
	Returns  : dict.
	"""
	details = get_record_details(recordid=recordid)
	props   = get_record_properties(recordid=recordid)
	details.update(props)
	return details

def get_file_media_type(projectid,fileid):
	"""
	Args  : projectid - the DNAnexus project ID in which the file in questions exists.
					fileid    - the DNAnexus file ID/dxlink
	"""
	return dxpy.DXFile(dxid=fileid,project=projectid).describe()['media']

def get_compressor_type(media_type):
	""" 
	Function : Outputs the program and arguments to be used to decompress a file, given
						 a media type.
	Args     : media_type - str. The value of the 'media' key of the properties dict of a DNAnexus file.
	"""
	if media_type == "application/x-gzip":
		return conf.compressors["GZIP"]
	elif media_type == "application/x-bzip2":
		return conf.compressors["BZIP"]
	else:
		return None

def download_dx_file(projectid,fileid,localname):
	"""
	Args : projectid - The DNAnexus project ID of the project in which the file in question exists.
				 fileid    - A DNAnexus file ID or dxlink to a file.
				 localname - str. Name the downloaded file will have locally.
	"""
	dxpy.download_dxfile(dxid=fileid,project=projectid,filename=localname)

