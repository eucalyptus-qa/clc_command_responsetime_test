#!/usr/bin/perl

$sleeptime = shift @ARGV || "5";
$ip = shift @ARGV;
#$echo = "echo";

if (!$ip) {
    print "ERROR: must supply a target ip\n";
    exit(1);
}

sleep 2;

$cmd = "ip addr show |grep 'inet $ip' -B2 | grep BROADCAST | awk '{print \$2}' | sed 's/://g'";
chomp($iface = `$cmd`);

if (!$iface || $iface eq "") {
    print "ERROR: could not find iface for ip $ip\n";
    exit(1);
}
print "halting $iface...\n";

system("$echo ip link set $iface down");
sleep $sleeptime;

print "starting $iface...\n";
system("$echo ip link set $iface up");

exit(0);
