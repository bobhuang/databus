#!/bin/bash
#define espresso home

SCRIPT=`readlink -f $0`
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=`dirname $SCRIPT`
echo $SCRIPTPATH
cur_dir=$SCRIPTPATH

export ESPRESSO_HOME=${cur_dir}
 
# Define packages to download
cd $ESPRESSO_HOME
rm -rf *.pkg

# Download Packages
svn export svn+ssh://svn.corp.linkedin.com/netrepo/cluster-manager/dds_test_infra/espresso_releases/espresso-storage-node-pkg-0.5.103-SNAPSHOT.tar.gz espresso-storage-node-pkg.tar.gz
svn export svn+ssh://svn.corp.linkedin.com/netrepo/cluster-manager/dds_test_infra/espresso_releases/espresso-tools-pkg-0.5.103-SNAPSHOT.tar.gz espresso-tools-pkg.tar.gz
svn export svn+ssh://svn.corp.linkedin.com/netrepo/cluster-manager/dds_test_infra/espresso_releases/espresso-router-pkg-0.5.103-SNAPSHOT.tar.gz espresso-router-pkg.tar.gz

# Create distribution directories
mkdir -p dist
mkdir -p dist-espresso-tools
mkdir -p dist-espresso-router

#untar the distributions
tar -C dist -xvf espresso-storage-node-pkg.tar.gz
tar -C dist-espresso-tools -xvf espresso-tools-pkg.tar.gz
tar -C dist-espresso-router -xvf espresso-router-pkg.tar.gz

