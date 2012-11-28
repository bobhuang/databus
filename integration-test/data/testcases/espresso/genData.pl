#!/usr/bin/perl

use Encode;
my $ustring1 = "Hello \x{263A}!\n";  
my $ustring2 = <DATA>;
$ustring2 = decode_utf8( $ustring2 );
open FILE, ">EspressoDB_Unicode.dat" or die $!;
print FILE "EspressoDB8\n";
print FILE "IdNamePair\n";

for ( $count=0 ; $count < 6553; $count++ ) {
    print FILE "{\"id\":$count, \"name\": \"";
    $ustring1 = "UNI: ";
    for ( $count2=0; $count2 < 10; $count2++ ) {
        $val = $count*10+$count2;
        my $foo = chr($val);
        $ustring1 .= $foo;
    }
    print FILE "$ustring1";
    print FILE "\"}\n"
}
close(FILE);

