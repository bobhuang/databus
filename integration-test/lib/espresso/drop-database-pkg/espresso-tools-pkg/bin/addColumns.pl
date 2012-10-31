#! /usr/bin/perl

use Getopt::Long;

# Add the flags,rstate and expires columns to every table in every es_* database:
# +----------------+--------------+------+-----+---------+-------+
# | <key_parts>    | <type>       |      |     |         |       |
# | timestamp      | bigint(20)   | NO   |     | NULL    |       |
# | etag           | varchar(10)  | NO   |     | NULL    |       |
# | flags          | bit(32)      | YES  |     | NULL    |       |
# | rstate         | varchar(256) | YES  |     | NULL    |       |
# | expires        | bigint(20)   | YES  |     | NULL    |       |
# | val            | longblob     | YES  |     | NULL    |       |
# | schema_version | smallint(6)  | NO   |     | NULL    |       |
# +----------------+--------------+------+-----+---------+-------+

# Default values for options.  All are overridable on the command line.
my $dbname = "";
my $dryrun = 0;
my $instance = "";
my $verbose = 0;

GetOptions(
           'dbname=s'     => \$dbname,
           'dryrun'       => \$dryrun,
           'instance=s'   => \$instance,
           'verbose'      => \$verbose) || usage(1);

# Get list of espresso databases
my @databases = `mysql $instance -uespresso -pespresso -ss -e 'show databases' | grep '^es_$dbname'`; 

foreach my $database (@databases) {
    chomp $database;
    print "Processing database: $database\n" if ($verbose);
    my @tables  = `mysql $instance -uespresso -pespresso -ss -e 'use $database; show tables;'`;
    foreach my $table (@tables) {
        chomp $table;
	my $ddl = "";
	my $schema = `mysql $instance -uespresso -pespresso -ss -e 'describe $database.$table;'`;
        if ($schema !~ /flags/) {
	    $ddl .= " ADD flags bit(32) AFTER etag";
        }
        if ($schema !~ /rstate/) {
            $ddl .= "," if (length($ddl));
	    $ddl .= " ADD rstate varchar(256) AFTER flags";
        }
        if ($schema !~ /expires/) {
            $ddl .= "," if (length($ddl));
	    $ddl .= " ADD expires bigint AFTER rstate";
        }

        if (length($ddl)) {
	    $ddl = "alter table $database.$table $ddl;\n";
            print $ddl if ($verbose);
            print `mysql $instance -uespresso -pespresso -ss -e 'SET sql_log_bin = 0; $ddl'` unless ($dryrun);
        } else {
            print "No fields needed for $database.$table\n" if ($verbose);
        }
    }
}

sub usage($)
{
  my $exit_status = shift;
  print <<__END__;

Usage:

addColumns.pl [options] 

Options:
        --dbname=<name>         Name of Espresso Datbase to add columns to (none)
        --dryrun                Don't actually execute ddl statements (false)
        --instance=<i00?>       Don't actually execute ddl statements (none)
        --verbose               Be Verbose ($verbose)

Executes alter-table commands to add new columns to database partitions as needed.

Examples:

   Add columns to only the MailboxDB database and be verbose

   % addColumns --database MailboxDB --verbose

   Print the commands necessary to add columns to all databases, but don't actually modify the database

   % addColumns --verbose --dryrun

__END__

  exit $exit_status;
}

