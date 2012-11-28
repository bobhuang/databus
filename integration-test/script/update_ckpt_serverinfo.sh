ckpt_file=$1
host=`hostname`;
ckpt=`cat $ckpt_file | perl -lane '{my $a = $_; $a =~ 's/__FIXME__/$host/'; print $a;}'`
echo $ckpt
