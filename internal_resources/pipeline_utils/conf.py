import os
import json
import dxpy

localDir = os.path.dirname(os.path.realpath(__file__))
confFile = open(os.path.join(localDir,"conf.json"),"r")
conf = json.load(confFile)

pythonPackagesDir = conf["pythonPackagesDir"]

appletNames = conf["appletNames"]

#compression utilities

compressors = conf["compressors"]

tools = conf["tools"]

#Projects
projects = conf["projects"]
dashboardProject = projects["dashboardProject"]["name"]
dashboardProjectID = dxpy.find_one_project(name=dashboardProject, zero_ok=False, more_ok=False,level="VIEW")['id']
dashboardContributeToken = projects["dashboardProject"]["contributeAccessToken"] #token for level CONTRIBUTE to the dashboard project
genomesProject = projects["genomesProject"]["name"]
accountProject = projects['accountSettingsProject']['name']
accountProjectID = dxpy.find_one_project(name=accountProject, zero_ok=False, more_ok=False,level="VIEW")['id']
appletsProject = accountProject
appletsProjectID = accountProjectID
referenceGenomesProjectID = dxpy.find_one_project(name=genomesProject, zero_ok=False, more_ok=False,level="VIEW")['id']
#End projects


#Formats
formats = conf["formats"]

#Dashboard
dashboardRecordTypeName = projects["dashboardProject"]["recordTypeName"] #SCGPMRun
pipelineAttributeNames = conf["pipelineAttributeNames"] #dict. value of 1 means prop, 2 means detail.

#FASTQ file properties names
fastqFileProps = conf["fastqFileProps"]

#Folders
commonResourceBundleFolder = projects['accountSettingsProject']['folders']['commonResourceBundle']
appletsFolder = projects['accountSettingsProject']['folders']['applets']
appletsFolder = "/" + appletsFolder #the dx tools require that folders have a "/" prefix.
appletsArchiveFolder = projects['accountSettingsProject']['folders']['appletsArchive']
appletsArchiveFolder = "/" + appletsArchiveFolder #the dx tools require that folders have a "/" prefix.
sample_sheet_folder = conf["sample_sheet_folder"]
run_project_tar_folder = conf["run_project_tar_folder"]
fastq_files_folder = conf["fastq_files_folder"]
#End Folders

#Attributes on records, projects, ...
attrs = conf["pipelineAttributeNames"] #dict.
attrTypes = {}
for i in attrs:
	attrTypes[i] = {}
	attrTypes[attrs[i]['name']] = attrs[i]['type']
	
pairedEndAttrName = attrs["pairedEnd"]["name"]
pairedEndAttrValues = attrs["pairedEnd"]["values"]
runMetadataTarFileAttrName = attrs["runMetadataTarFile"]["name"]
interopTarFile = attrs["interopTarFile"]["name"]
laneTarFileAttrName = attrs["laneTarFile"]["name"]
uploadDateAttrName = attrs["uploadDate"]["name"]
userAttrName = attrs["user"]["name"]
emailAttrName = attrs["email"]["name"]
laneAttrName = attrs["lane"]["name"]
runNameAttrName = attrs["runName"]["name"]
laneProjectAttrName = attrs["laneProject"]["name"]
runProjectAttrName = attrs["runProject"]["name"]
mappingReferenceAttrName = attrs["mappingReference"]["name"]
libraryAttrName = attrs["library"]["name"]
dnanexusUseridAttrName = attrs["dnanexus_userid"]["name"]
environmentAttrName = attrs["environment"]["name"]
limsUrlAttrName = attrs["limsUrl"]["name"]
limsTokenAttrName = attrs["limsToken"]["name"]
pipelineStageAttrName = attrs["pipelineStage"]["name"]
pipelineStageAttrValues = attrs["pipelineStage"]["values"]
libraryAttrName = attrs["library"]["name"]
mappingReferenceAttrName = attrs["mappingReference"]["name"]
mappingProgramAttrName = attrs["mappingProgram"]["name"]
qcReportIdAttrName = attrs["qcReportID"]["name"]
dashboardRecordIdAttrName = attrs["dashboardRecordId"]["name"]
#End attribute Names

#Other
dashboardRecordPropertyConstant = conf["dashboardRecordPropertyConstant"] #a numeric constant
dashboardRecordDetailConstant = conf["dashboardRecordDetailConstant"] #a numeric constant
daysToSponsorProjects = conf["daysToSponsorProjects"]
validEnvironments = conf["validEnvironments"]
resourceBundleName = conf['resourceBundleName']
appletsSrc = conf["appletsSrc"]
supportedMappers = conf["mappingPrograms"].values()
#End Other

del localDir
del confFile
del conf
