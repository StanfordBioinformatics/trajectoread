#!/usr/bin/bash -eux

conf=conf.json
PROPERTY=$(jq -r .dashboardRecordPropertyConstant $conf)
DETAIL=$(jq -r .dashboardRecordDetailConstant $conf)

#takes four required arguments
setDashboardRecordAttr() {
  # setDashboardRecordAttr "${dashboard_record_id}" "${laneProjectAttrName}" "${lane_project_id}" "${laneProjAttrType}"
  dashboard_record_id=${1}
  attrName=${2}
  attrVal=${3}
  attrType=${4}
  if [[ ${attrType} -eq ${PROPERTY} ]]
  then
    dx set_properties "${dashboard_record_id}" "${attrName}"="${attrVal}"
  elif [[ ${attrType} -eq ${DETAIL} ]]
  then
    dx set_details "${dashboard_record_id}" "{\"${attrName}}\": \"${attrVal}\" }"
  fi  
}

