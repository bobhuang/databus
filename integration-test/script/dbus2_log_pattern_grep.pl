#!/usr/bin/perl
#
# ./dbus2_log_pattern_grep.pl <pattern_file> <log_file>
#
# Pattern File:
#
# The pattern file will have the following format
#
#
# <Pattern1>,<NumOccurence>
# <Pattern2>,<NumOccurence>
# <Pattern3>,<NumOccurence>
# <Pattern4>,<NumOccurence>
#
# The pattern field can be any regex pattern. Any special regex character that needs to be treated as literal has to be escaped.
# The NumOccurence field can be one of
#   * - 0 or more times
#   + - 1 or more times
#   ? - atmost 1 time
#   n - exactly n times (e.g 5 -> exactly 5 times)
#

use strict;

my $usage = "$0 <pattern_file> <log_file>";

die "Usage: $usage\n" if ($#ARGV != 1 );

my $patternFile = $ARGV[0];
my $logFile = $ARGV[1];

my @patterns =  ();
my @occurences = ();

open (F, "< $patternFile") or die "Unable to open pattern file : $patternFile";
while (my $l = <F> )
{
  chomp($l);

  my @fields = split(",",$l);

  die "Could not parse line : $l" if  ( $#fields < 1 );

  my $occurence = $fields[$#fields];
  pop @fields;
  my $pattern = join(",",@fields);
 
  push @occurences, $occurence;
  push @patterns, $pattern;   
}
close(F);

for (my $i =0; $i <= $#patterns; $i++)
{
   my $p = $patterns[$i];
   my $occ = $occurences[$i];

   print "Checking for pattern ($p)\n";

   my $count = `grep -cP "$p" $logFile`;
   
   if ( $occ eq "+" )
   {
      die "Pattern match failed for pattern ($p)\n" if ( $count <= 0 );
   } elsif ($occ eq "*" ) {
   } elsif ($occ eq "?" ) {
      die "Pattern match failed for pattern ($p)\n" if ( $count > 1 );
   } elsif ($occ =~ m/\[(\d+)-(\d+)\]/) {
      my $low = $1;
      my $high = $2;
      die "Pattern match failed for pattern ($p). Not in range ($low) - ($high)\n" if ( ($count < $low)  || ($count > $high));
   } else {
      die "Pattern match failed for pattern ($p)\n" if ( $count != $occ);
   }
}

print "Patterns matched successfully !!\n"
