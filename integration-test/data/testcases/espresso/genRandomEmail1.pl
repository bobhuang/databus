#!/usr/bin/perl

use Encode;
my $ustring1 = "Hello \x{263A}!\n";  
my $ustring2 = <DATA>;
$ustring2 = decode_utf8( $ustring2 );
open FILE, ">EspressoDB_RandomEmail1.dat" or die $!;
my @chars=('a'..'z','A'..'Z','0'..'9','_');

for ( $count=0 ; $count < 10000; $count++ ) {
    $ustring1 = "Rand: ";
    for ( $count2=0; $count2 < rand(100); $count2++ ) {
        $ustring1 .= $chars[rand @chars];
    }
    print FILE "{\"createdDate\":$count, ";
    print FILE "\"fromMemberId\":$count, ";
    print FILE "\"subject\":\"$ustring1\", ";
    print FILE "\"body\":\"$ustring1\"";
    print FILE "}\n";
}
close(FILE);

