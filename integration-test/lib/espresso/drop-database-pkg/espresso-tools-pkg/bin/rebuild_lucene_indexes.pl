#! /usr/bin/perl

# Script to rebuild lucene indexes from MySQL
#
# Directions:
#
#   Shut down the espresso-storage-node process running on the host.
# 
#   % sudo -u app rebuild_lucene_indexes.pl
#
#   Restart the espresso-storage-node process.
# 
# The script will create a temp directory under /mnt/u001/lucene-indexes and build a complete
# set of new indexes for the specified database in that location.  When done, it moves the
# existing /mnt/u001/lucene-indexes/<database> directory aside and replaces it with the new
# one.

use strict;	# Of course

#
# Settings (should make these command line arguments, for now just edit if you need to change)
#
my $tools_ver = "0.5.105";
my $database = "BizProfile";
my $schema_root = "/schemas_registry/bizprofile";
my $mysql_port = 3306;
my $zk_address = "ela4-app0488.prod:2181";
my $index_root = "/mnt/u001/lucene-indexes";
my $part_prefix = "es_$database" . "_";

#
# The bulk indexer in the 0.5.105 release will fail if it encounters a table within
# the database that doesn't have any indexed fields.  So we need to specify all of
# the tables by name. Future releases will fix this bug and eliminate the need to
# spoon-feed the table names to the buld-indexer.
#
my @tables = ("BizCompany","BizEmailDomain","BizFlag","BizProduct","BizRecommendation");
my $tbl_arg = join " -t ", @tables;

$ENV{"JAVA_HOME"} = "/export/apps/jdk/JDK-1_6_0_21/" unless (defined $ENV{"JAVA_HOME"});

# If neecessary, create scratch directory to build
if (! -e "$index_root/temp") {
  mkdir "$index_root/temp" || die "Unable to create temp directory under /mnt/u001/lucene-indexes.  Did you run with 'sudo -u app' ?\n";
}
chdir "$index_root/temp";

# Get the appropriate espresso-tools-pkg and explode it
if (! -e "espresso-tools-pkg-$tools_ver.tar.gz") {
  system "wget artifactory.corp.linkedin.com:8081/artifactory/simple/DDS/com/linkedin/espresso/espresso-tools-pkg/$tools_ver/espresso-tools-pkg-$tools_ver.tar.gz" || die "Unable to fetch espresso-tools-pkg\n";
}

if (! -e "espresso-tools-pkg-$tools_ver") {
  mkdir "espresso-tools-pkg-$tools_ver" || die "Unable to create tools subdirectory\n";
  system "tar xz -C espresso-tools-pkg-$tools_ver -f espresso-tools-pkg-$tools_ver.tar.gz" || die "Unable to expand tools package\n";
}

# Invoke the bulk-indexer on all partitions on this host
open MYSQL, "mysql --user=espresso --password=espresso --exec 'show databases;'|" || die "Unable to fetch databases from mysql\n";
while (<MYSQL>) {
  if (/$part_prefix(\d+)/) {
    my $part = $1;
    my $cmd = "espresso-tools-pkg-$tools_ver/bin/bulk-indexer.sh -m zk -p $mysql_port -z $zk_address -r $schema_root -t $tbl_arg $database $part";
    print "$cmd\n";
    system "$cmd" || die "Unable to rebuild index for $database partition $part\n";
  }
}

# If  we made it this far, we've got a complete set of new indexes. Save the old index directory and # replace it.
my $rev = 1;
$rev++ while (-e "$index_root/$database.$rev");
rename "$index_root/$database", "$index_root/$database.$rev" || die "Unable to move old index directory\n";

print "Moving new Lucene indexes to $index_root/$database";
system "mv $database $index_root/$database" || die "Unable to move new index directory to final location\n";
print "Done.\n";

