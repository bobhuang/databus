use strict;

my $num = 0;

my @VALID_STATES_AFTER_PICK_SERVER = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER", "REQUEST_SOURCES" ) ;
my @VALID_STATES_AFTER_REQUEST_SOURCES = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","SOURCES_REQUEST_ERROR", "SOURCES_RESPONSE_ERROR","SOURCES_RESPONSE_SUCCESS");
my @VALID_STATES_AFTER_SOURCES_REQUEST_ERROR = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER");
my @VALID_STATES_AFTER_SOURCES_RESPONSE_ERROR = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER");
my @VALID_STATES_AFTER_SOURCES_RESPONSE_SUCCESS = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","REQUEST_REGISTER");
my @VALID_STATES_AFTER_REQUEST_REGISTER = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","REGISTER_REQUEST_ERROR", "REGISTER_RESPONSE_ERROR","REGISTER_RESPONSE_SUCCESS");
my @VALID_STATES_AFTER_REGISTER_REQUEST_ERROR = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER");
my @VALID_STATES_AFTER_REGISTER_RESPONSE_ERROR = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER");
my @VALID_STATES_AFTER_REGISTER_RESPONSE_SUCCESS = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","REQUEST_STREAM","BOOTSTRAP");
my @VALID_STATES_AFTER_REQUEST_STREAM = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","STREAM_REQUEST_ERROR", "STREAM_RESPONSE_ERROR","STREAM_REQUEST_SUCCESS");
my @VALID_STATES_AFTER_STREAM_REQUEST_ERROR = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER");
my @VALID_STATES_AFTER_STREAM_RESPONSE_ERROR = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER");
my @VALID_STATES_AFTER_STREAM_REQUEST_SUCCESS = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","STREAM_RESPONSE_DONE", "BOOTSTRAP","STREAM_RESPONSE_ERROR");
my @VALID_STATES_AFTER_STREAM_RESPONSE_DONE = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","REQUEST_STREAM");
my @VALID_STATES_AFTER_BOOTSTRAP = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","BOOTSTRAP_COMPLETE","BOOTSTRAP_FAILED");
my @VALID_STATES_AFTER_BOOTSTRAP_FAILED = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","BOOTSTRAP");
my @VALID_STATES_AFTER_BOOTSTRAP_COMPLETE = ("SHUTDOWN", "PAUSE", "SUSPEND_ON_ERROR", "RESUME", "SET_SERVERS","ADD_SERVER", "REMOVE_SERVER","PICK_SERVER","BOOTSTRAP","REQUEST_STREAM");


while ( my $line = <STDIN> )
{
  chomp($line);

  if ( $line =~ m/RelayPuller/)
  {
     if ( $line =~ m/Message Queue History/ )
     {
       $num++;
       my @tok = split(":", $line);
       my $msg = $tok[$#tok];
       $msg =~ s/\[//;
       $msg =~ s/\]//;
       my @msgArr = split(/,/,$msg);
       my $p = "";
       my $prevError = 0;
       foreach my $m (@msgArr)
       { 
          die "Duplicate message seen ($m) in Line : $line" if ($m eq $p);
          
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_PICK_SERVER) if ($p =~ m/PICK_SERVER/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_REQUEST_SOURCES) if ($p =~ m/REQUEST_SOURCES/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_SOURCES_REQUEST_ERROR) if ($p =~ m/SOURCES_REQUEST_ERROR/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_SOURCES_RESPONSE_ERROR) if ($p =~ m/SOURCES_RESPONSE_ERROR/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_SOURCES_RESPONSE_SUCCESS) if ($p =~ m/SOURCES_RESPONSE_SUCCESS/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_REQUEST_REGISTER) if ($p =~ m/REQUEST_REGISTER/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_REGISTER_REQUEST_ERROR) if ($p =~ m/REGISTER_REQUEST_ERROR/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_REGISTER_RESPONSE_ERROR) if ($p =~ m/REGISTER_RESPONSE_ERROR/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_REGISTER_RESPONSE_SUCCESS) if ($p =~ m/REGISTER_RESPONSE_SUCCESS/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_REQUEST_STREAM) if ($p =~ m/REQUEST_STREAM/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_STREAM_REQUEST_ERROR) if ($p =~ m/STREAM_REQUEST_ERROR/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_STREAM_RESPONSE_ERROR) if ($p =~ m/STREAM_RESPONSE_ERROR/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_STREAM_REQUEST_SUCCESS) if ($p =~ m/STREAM_REQUEST_SUCCESS/);
          verifyNextState($line, $p, $m, @VALID_STATES_AFTER_STREAM_RESPONSE_DONE) if ($p =~ m/STREAM_RESPONSE_DONE/);

          if ($p =~ m/BOOTSTRAP_COMPLETE/)
          {
            verifyNextState($line, $p, $m, @VALID_STATES_AFTER_BOOTSTRAP_COMPLETE) 
          } elsif($p =~ m/BOOTSTRAP_FAILED/) {
            verifyNextState($line, $p, $m, @VALID_STATES_AFTER_BOOTSTRAP_FAILED);
          } elsif ($p =~ m/BOOTSTRAP/) { 
            verifyNextState($line, $p, $m, @VALID_STATES_AFTER_BOOTSTRAP);
          }

          $p = $m;
          if ( ($m =~ /ERROR/) && ($m !~ /SUSPEND_ON_ERROR/) )
          { 
             $prevError = 1;
          } else {
             $prevError = 0;
          }
       }
     } elsif ( $line =~ m/Unkown state in RelayPullThread/ )  {
        die "Unknown state noticed in RelayPullThread. Line : $line";   
     }
  }
}
print "Num Lines validated : $num\n";

sub verifyNextState()
{
  my ($line, $currState, $nextState, @validNextStates) = @_;

  my $found = 0;

  $nextState =~ s/\s+//g;   

  foreach my $n (@validNextStates)
  {
    $n =~ s/\s+//g;
    if ($n eq $nextState)
    {
       $found = 1;
       last;
    }
  }
  
  die "Wrong next state ($nextState) for current state ($currState). Valid states are (@validNextStates). Line : $line" if ( $found == 0 );
}
