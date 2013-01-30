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

my $part_ids_str=`mysql -u espresso -pespresso -e 'show databases;' | egrep -i 'MailboxDB|EspressoDB|BizProfile|BizFollow|CommDB|MyDB|TestDB'`;
if ($! != 0) {
	die "unable to get partition ids";
}
chomp $part_ids_str;

my $part_names_str = $part_ids_str;

$part_ids_str =~ s/[A-Za-z_]+//g;
$part_names_str =~ s/[0-9]+//g;

my @part_ids=split(/\s+/, $part_ids_str);
my @part_names=split(/\s+/, $part_names_str);

#print "Partitions:";
#print join(',', @part_ids);
#print "\n";
#print join(',', @part_names);
#print "\n";

for (my $i = 0 ; $i < $#part_ids; ++$i) {
	my $p = $part_ids[$i];
	my $name = $part_names[$i];
	my $file_name="${file_prefix}_${name}$p.json";
	print "Configuring ${name}$p --> $file_name\n";
	open(CONF, ">$file_name") || die "unable to open $file_name for writing";
	print CONF <<EOFILE
{
    "name" : "${name}$p",
    "id"  : $p,
    "uri" : "esv4-app76.stg_12101",
        "slowSourceQueryThreshold" : 2000,
        "sources" :
        [
                {"id" : 102,
                 "name" : "BizProfile.BizCompany",
                 "uri": "BizProfile/BizCompany",
                 "partitionFunction" : "constant:1",
                 "partition" : $p
                }
        ]
}
EOFILE
;
	close CONF;
}


