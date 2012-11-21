
use strict;

my $usage = "$0 <low_scn> <high_scn> <inclusive - 0|1> <positive - 0|1>";
   $usage .= "\n\t <low_scn> : Lowest SCN in the filter range";
   $usage .= "\n\t <high_scn> : Highest SCN in the filter range";
   $usage .= "\n\t <inclusive> : 1 : Include the low and high Scn to be part of the range. 0 : otherwise ";
   $usage .= "\n\t <positive> : 1 : pass through only SCNs in the range. 0 : Acts like a negative filter which filters out events within the range"; 

if ( $#ARGV != 3 )
{
  die $usage;
}

my $lowScn = $ARGV[0];
my $highScn = $ARGV[1];
my $inclusive = $ARGV[2];
my $positive = $ARGV[3];

die "LowScn cannot be higher thatn highScn !! " if ( $lowScn > $highScn );

die "Inclusive Flag can only be 0 or 1 !! " if ( ($inclusive != 0) && ($inclusive != 1 ));
die "Positive flag can only be 0 or 1 !! " if ( ($positive != 0) && ($positive != 1));


while ( my $event = <STDIN> )
{
  chomp($event);

  if ( $event =~ m/sequence\":(\d+)/ )
  {
    my $sequence = $1;
    my $in_range = isSeqInRange($sequence);
    
    if ( (($positive == 1) && ($in_range > 0 )) ||
         (($positive == 0) && ($in_range == 0 )))
    {
      print "$event\n";
    }
  } else {
    die "Event ($event) does not have sequence field";
  }
}

sub isSeqInRange()
{
   my ($seq) = @_;

   my $ret = 0;
   if ( $inclusive == 1 )
   {
      $ret = ($seq >= $lowScn) && ($seq <= $highScn);
   } else {
      $ret = ($seq > $lowScn) && ($seq < $highScn);
   }
   return $ret;
}
