#!/usr/bin/perl
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

my $file_prefix=$ARGV[1];

if (not $file_prefix or 0 == length($file_prefix)) {
	$file_prefix="ppart_";
}


$part_ids_str =~ s/[A-Za-z_]+//g;
$part_names_str =~ s/[0-9]+//g;

my @part_ids;
my @part_names;

for (my $i=0; $i <= 64; ++$i) {
	$part_ids[$i]=$i;
	$part_names[$i]="BizProfile";
}

my @src_ids = ("BizProfile.BizCSAdmins", "BizProfile.BizCompany", "BizProfile.BizEmailDomain", "BizProfile.BizEmailTracking", "BizProfile.BizFlag", "BizProfile.BizMetadata", "BizProfile.BizProduct", "BizProfile.BizRecommendation", "BizProfile.BizRestrictedDomain");

#print "Partitions:";
#print join(',', @part_ids);
#print "\n";
#print join(',', @part_names);
#print "\n";

my $hostname=`hostname`;
chomp $hostname;

for (my $i = 0 ; $i <= $#part_ids; ++$i) {
	my $p = $part_ids[$i];
	my $name = $part_names[$i];
	my $file_name="${file_prefix}_${name}$p.json";
	print "Configuring ${name}$p --> $file_name\n";

	my $logical_sources="";
	for (my $j=0; $j <= $#src_ids; ++$j) {
		if ($j > 0) {
			$logical_sources="$logical_sources,";
		}
		my $src_name=$src_ids[$j];
		$logical_sources = $logical_sources . 
<<EOFSTR
		{"id" : $j,
                 "name" : "$src_name",
                 "uri": "$src_name",
                 "partitionFunction" : "constant:1",
                 "partition" : $p
                }
EOFSTR
	}

	open(CONF, ">$file_name") || die "unable to open $file_name for writing";
	print CONF <<EOFILE
{
    "name" : "${name}",
    "id"  : $p,
    "uri" : "${hostname}",
     "slowSourceQueryThreshold" : 2000,
     "maxSCNHandler": {
        "type": "FILE",
        "file": {"scnDir": "/export/content/data/espresso-databus-relay/i001/binlog_ofs"}
     },
     "sources" :
        [
		$logical_sources
        ]
}
EOFILE
;
	close CONF;
}


