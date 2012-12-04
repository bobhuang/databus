#! /usr/bin/perl

#
# Steps to install:
# 
# 1) Install Oracle Instantclient (should be in YUM repository):
# 
#   sudo yum install oracle-instantclient11.2-basic
# 
# 2) Install the perl DBD::Oracle module and add the .so location to ldconfig
# 
#   sudo rpm -i --nodeps ./perl-DBD-Oracle-1.27-2.el6.x86_64.rpm
#   sudo bash -c 'echo /usr/lib/oracle/11.2/client64/lib > /etc/ld.so.conf.d/oracle-instantclient11.2.conf'
#   sudo ldconfig
# 

use strict;

use DBI;
use DBD::Oracle qw(:ora_types);
use Getopt::Long;

my $mysqldump = "/usr/bin/mysqldump";

# Default values for options.  All are overridable on the command line.
my $host = "ela4-db66.prod.linkedin.com";
my $port = 1521;
my $SID = "PUSCP1";
my $user = "espresso";
my $password = "m2vd1";
my $batchSize = 100;
my $debug = 0;
my $dryrun = 0;
my $verbose = 0;
my $partitions = undef;

GetOptions(
           'batch=i'      => \$batchSize,
           'debug'        => \$debug,
           'Dryrun'       => \$dryrun,
           'host=s'       => \$host,
           'partitions=s' => \$partitions,
           'password=s'   => \$password,
           'port=i'       => \$port,
           'SID=s'        => \$SID,
           'user=s'       => \$user,
           'verbose'      => \$verbose) || usage(1);

# Map of Espresso table name to SQL statement for inserts into said table
my %PreparedStatements = (
  "App"                     => "INSERT INTO ucp_App (id,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "AppFeed"                 => "INSERT INTO ucp_AppFeed (appIdMd5,nameMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "ObjectSchema"            => "INSERT INTO ucp_ObjectSchema (appIdMd5,idMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "Sequence"                => "INSERT INTO ucp_Sequence (sequenceName,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "SharedFeed"              => "INSERT INTO ucp_SharedFeed (appIdMd5,idMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "TranslatableContent"     => "INSERT INTO ucp_TranslatableContent (keyMd5,appIdMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "VerbSchema"              => "INSERT INTO ucp_VerbSchema (appIdMd5,idMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "ActivityEvent"           => "INSERT INTO ucpx_ActivityEvent (id,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "Comment"                 => "INSERT INTO ucpx_Comment (id,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "Comment2"                => "INSERT INTO ucpx_Comment2 (threadIdMd5,commentId,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "CommentThread"           => "INSERT INTO ucpx_CommentThread (id,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "CommentThread2"          => "INSERT INTO ucpx_CommentThread2 (idMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "Lyke"                    => "INSERT INTO ucpx_Lyke (objectidMd5,actorIdMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?,?)",
  "ObjectLikeSummary"       => "INSERT INTO ucpx_ObjectLikeSummary (objectIdMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)",
  "RelevancePreferences"    => "INSERT INTO ucpx_RelevancePreferences (memberMd5,timestamp,etag,val,schema_version) VALUES (?,?,?,?,?)");

# Map of Espresso table name to number of key parts in said table
my %keyParts = (
  "App"                     => 1,
  "AppFeed"                 => 2,
  "ObjectSchema"            => 2,
  "Sequence"                => 1,
  "SharedFeed"              => 2,
  "TranslatableContent"     => 2,
  "VerbSchema"              => 2,
  "ActivityEvent"           => 1,
  "Comment"                 => 1,
  "Comment2"                => 2,
  "CommentThread"           => 1,
  "CommentThread2"          => 1,
  "Lyke"                    => 2,
  "ObjectLikeSummary"       => 1,
  "RelevancePreferences"    => 1);

my $txnCount = 0;	# Number of uncommitted transactions for current table
my $txnTotal = 0;       # Total number of transactions for current partition
my $currentTable = "";	# Name of current table
my $currentStatement;	# Current prepared statement

# Connect to the database
my $oracleConnect = "DBI:Oracle:host=$host;port=$port;sid=$SID";

my $dbh = DBI->connect($oracleConnect,
                       $user, 
                       $password, 
                       {AutoCommit => 0, 
                        RowCacheSize => 1000}) || die "Unable to connect to Oracle with '$oracleConnect'\n";

$dbh->{AutoCommit} = 0;
$dbh->{RowCacheSize} = $batchSize;
$dbh->{ora_check_sql} = 0 unless ($debug);

# Are we operating on pre-created dump files, or on specified partitions
if (defined $partitions) {
  my @parts = split /,/, $partitions;
  foreach my $part (@parts) {
    my $todo = "TODO/$part";
    if (! -e $todo) {
	    print STDERR "Skipping partition $part: no longer in TODO list.\n";
            next;
    }
    print STDERR "Processing partition $part:\n";
    open my $dump, "sudo -u app $mysqldump -ntq --compact --hex-blob --user=espresso --password=espresso --databases $part|" || die "Unable to run mysqldump for partition $part\n";
    forklift ($dump);
    close $dump;
  }
} else {
  forklift(\*STDIN);
}

sub forklift($)
{
  my $fh = shift;
  $txnCount = 0;
  $txnTotal = 0;
  
  while (<$fh>) {

    if (/INSERT INTO `(\w+)`/) {  # New INSERT statement.  We may have changed tables.
      my $table = $1;
      next if ($table eq "Content");   # Ignore old Content table that is no longer in use
      die "Found insert for undefined table $table\n" unless (defined $PreparedStatements{$table});
      if ($table ne $currentTable) {
	# Is a change of table.  Commit any pending inserts and reset pending transaction count
	if ($txnCount > 0) {
	  $dbh->commit unless ($dryrun);
	  $txnCount = 0;
	}
	print STDERR "Ending $currentTable. txn total: $txnTotal\n" if ($verbose && $currentTable ne "");
	$currentStatement = $dbh->prepare_cached($PreparedStatements{$table}) || die "Couldn't prepare statement '$PreparedStatements{$table}'\n";
	$currentTable = $table;
	print STDERR "\nChanging statement to $PreparedStatements{$table}\n" if ($verbose);
	print STDERR "Changing to table $table: " if ($verbose);
      }
    }

    if ($keyParts{$currentTable} == 1) {
      while (/\('?(\w+)'?,(\d+),'?(\d+)'?(,NULL)*,(\w+),(\d+)\)/g) {
	my ($id1, $timestamp, $etag, $nulls, $hexval, $schema_version) = ($1, $2, $3, $6, $5, $6);
	my $val = pack "H*", substr($hexval, 2);
	print "Executing: INSERT INTO $currentTable ($id1, $timestamp, $etag, $hexval, $schema_version)\n" if ($debug);
	$currentStatement->bind_param(":p1", $id1,            {ora_type => ORA_VARCHAR2});
	$currentStatement->bind_param(":p2", $timestamp,      {ora_type => ORA_LONG});
	$currentStatement->bind_param(":p3", $etag,           {ora_type => ORA_VARCHAR2});
	$currentStatement->bind_param(":p4", $val,            {ora_type => ORA_BLOB});
	$currentStatement->bind_param(":p5", $schema_version);
	if (!$dryrun) {
	  $currentStatement->execute() || print "Couldn't execute statement: " . $dbh->errstr;
	}
	$txnCount++;
        $txnTotal++;

	if ($txnCount >= $batchSize) {
	  $dbh->commit unless ($dryrun);
	  $txnCount = 0;
	}
	print STDERR "$txnTotal: $id1\n" if ($verbose && ($txnTotal % 1000 == 0));
      }
    } else {
      while (/\('?(\w+)'?,'?(\w+)'?,(\d+),'?(\d+)'?(,NULL)*,(\w+),(\d+)\)/g) {
	my ($id1, $id2, $timestamp, $etag, $nulls, $hexval, $schema_version) = ($1, $2, $3, $4, $5, $6, $7);
	my $val = pack "H*", substr($hexval, 2);
	print "Executing: INSERT INTO $currentTable ($id1, $timestamp, $etag, $hexval, $schema_version)\n" if ($debug);
	$currentStatement->bind_param(":p1", $id1,            {ora_type => ORA_VARCHAR2});
	$currentStatement->bind_param(":p2", $id2,            {ora_type => ORA_VARCHAR2});
	$currentStatement->bind_param(":p3", $timestamp,      {ora_type => ORA_LONG});
	$currentStatement->bind_param(":p4", $etag,           {ora_type => ORA_VARCHAR2});
	$currentStatement->bind_param(":p5", $val,            {ora_type => ORA_BLOB});
	$currentStatement->bind_param(":p6", $schema_version);
	if (!$dryrun) {
	  $currentStatement->execute () || print "Couldn't execute statement: " . $dbh->errstr;
	}
	$txnCount++;
        $txnTotal++;

	if ($txnCount >= $batchSize) {
	  $dbh->commit unless ($dryrun);
	  $txnCount = 0;
	}
	print STDERR "$txnTotal: $id1\n" if ($verbose && ($txnTotal % 1000 == 0));
      }
    }
  }
}

$dbh->commit if ($txnCount > 0 && !$dryrun);
$dbh->disconnect();

print "Ending Forklift...\n";

sub usage($)
{
  my $exit_status = shift;
  print <<__END__;

Usage:

forklift.pl [options] {filename...}|{partition...}

Options:
	--host=<hostname>     	Oracle db host ($host)
	--port=<port>     	Oracle db port ($port)
	--user=<user>     	Oracle db user ($user)
	--password=<password> 	Oracle db password (XXXXXX)
	--SID=<dbname>      	Oracle SID ($SID)
	--batch=<#rows>    	Number of rows to commit at a time ($batchSize)
	--debug      		Check SQL and be verbose ($debug)
	--verbose    		Be Verbose ($verbose)

If one or more files are specified on the command line, forklift.pl will upload to Oracle the contents of
each file sequentially.  If no file is specified, forklift.pl reads from standard input.

Examples:

   Run mysqldump for a single partition and stream the output to the Oracle upload script

   % sudo -u app mysqldump -ntq --compact --hex-blob --user=espresso --password=espresso --databases es_ucpx_2 | ./forklift.pl

   Run mysqldump to generate a single dump file containing partitions 0 to 3

   % sudo -u app mysqldump -ntq --compact --hex-blob --user=espresso --password=espresso --result-file=es_ucpx0-3.dump  --databases es_ucpx_0 es_ucpx_1 es_ucpx_2 es_ucpx_3

   Load all mysql databases named ucp_*

   % sudo -u app mysqldump -ntq --compact --hex-blob --user=espresso --password=espresso \
     --databases `mysql -u espresso --password=espresso -e 'show databases' | grep es_ucp_` | ./forklift.pl

__END__

  exit $exit_status;
}

