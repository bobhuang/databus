#! /usr/bin/perl

#
# Script to move old Lucene segment files from subdirectory to parent directory.
#

@dirs = `ls -d /mnt/u001/lucene-indexes/BizProfile/*/*`;

foreach $dir (@dirs) {
  chomp $dir;

  if (! -e "$dir/0") {
     print "$dir: No subdirectory to move.\n";
     next;
  }

  # First remove any indexes that were created in the new location
  print "rm -f $dir/*\n";
  system "rm -f $dir/*";

  # Then move the content from the old subdirectory to its parent directory
  print "mv $dir/0/* $dir\n";
  system "mv $dir/0/* $dir";

  # Finally, nuke the old subdirectory
  print "rmdir $dir/0\n";
  system "rmdir $dir/0";
}
