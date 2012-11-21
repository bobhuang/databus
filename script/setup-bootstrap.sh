#!/bin/bash

mysql_ver=$1
databus_ver=$2

MYSQL_RPM_ROOT="http://apachev-ld.linkedin.biz/rpms/"
MYSQL_RPM_HEAD="MySQL-bundle-Linkedin"
MYSQL_RPM_TAIL="rel4.x86_64.rpm"

mysql_rpm_installed=`rpm -qa | grep ${MYSQL_RPM_HEAD}`

mysql_lib_ver=`rpm -qa | grep mysql-libs`
if ( rpm -qa | grep mysql-libs ) ; then
  echo "${mysql_lib_ver} conflicts and will be removed."
  sudo rpm -e --nodeps mysql-libs
fi

install_mysql=Y
if [ ! -z "${mysql_rpm_installed}" ] ; then
  read -p "${mysql_rpm_installed} installed. Install a new one (Y/N)?" -n 1 install_mysql
fi

if [ "${install_mysql}" == "Y" -o "${install_mysql}" == "y" ] ; then
  if [ -z "${mysql_ver}" ] ; then
    read -p "MySQL RPM version (${MYSQL_RPM_ROOT} /${MYSQL_RPM_HEAD}-???-${MYSQL_RPM_TAIL}): " mysql_ver
  fi

  MYSQL_RPM="${MYSQL_RPM_ROOT}/${MYSQL_RPM_HEAD}-${mysql_ver}-${MYSQL_RPM_TAIL}"
  echo "Installing ${MYSQL_RPM}"
  export MYSQL_INSTALL_MODE=B
  sudo rpm -U ${MYSQL_RPM}
fi

if [ -z "${databus_ver}" ] ; then
  read -p "Databus version: " databus_ver
fi

databus_tgz_name="databus-bootstrap-utils-seeder-pkg"
databus_tgz_fullname="${databus_tgz_name}-${databus_ver}.tar.gz"
echo "Downloading ${databus_tgz_fullname} ..."
/usr/local/linkedin/bin/pull-artifact com.linkedin.databus2 ${databus_tgz_name} ${databus_ver} tar.gz $PWD
if [ ! -f ${databus_tgz_fullname} ] ; then
  echo "Downloading of ${databus_tgz_fullname} failed!"
  exit 1
fi

echo "Creating bootstrap DB ... "
mkdir bst-utils
tar -zxf ${databus_tgz_fullname} -C bst-utils 
bst-utils/bin/databus2-bootstrap/bootstrap_db_reinit.sh