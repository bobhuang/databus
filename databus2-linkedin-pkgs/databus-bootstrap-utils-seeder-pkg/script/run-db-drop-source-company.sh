#!/bin/bash
#
#
# Copyright 2013 LinkedIn Corp. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

cd `dirname $0`/..

script_dir=./bin
source $script_dir/setup.inc
source $script_dir/bootstrap_setup.inc

$script_dir/run-db-drop-source.sh -s 301 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 302 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 303 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 304 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 305 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 306 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 307 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 308 -p ${conf_dir}/bootstrap-drop-source.properties
$script_dir/run-db-drop-source.sh -s 309 -p ${conf_dir}/bootstrap-drop-source.properties
