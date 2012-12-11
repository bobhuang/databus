#!/usr/bin/perl

use Encode;
my $ustring1 = "Hello \x{263A}!\n";  
my $ustring2 = <DATA>;
$ustring2 = decode_utf8( $ustring2 );
open FILE, ">EspressoDB_Random1.dat" or die $!;
print FILE "EspressoDB8\n";
print FILE "IdNamePair\n";
my @chars=('a'..'z','A'..'Z','0'..'9','_');

for ( $count=0 ; $count < 10000; $count++ ) {
    print FILE "{\"id\":$count, \"name\": \"";
    $ustring1 = "Rand: ";
    for ( $count2=0; $count2 < rand(100); $count2++ ) {
        $ustring1 .= $chars[rand @chars];
    }
    print FILE "$ustring1";
    print FILE "\"}\n"
}
close(FILE);
