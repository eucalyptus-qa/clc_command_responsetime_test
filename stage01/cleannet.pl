#!/usr/bin/perl

sub restore_network_state {
    my $input = shift @_ || "2b_tested.lst";
    my $subnet = $pubipstr = "";
    my $ret = 0;

    open(FH, "$input");
    while(<FH>) {
	chomp;
	my $line = $_;
	if ($line =~ /^\s*SUBNET_IP\s*(.*)/) {
	    $subnet = $1;
	} elsif ($line =~ /^\s*MANAGED_IPS\s*(.*)/) {
	    $pubipstr = $1;
	}

    }
    close(FH);


    chomp($devstr = `ls /sys/class/net/`);
    $devstr =~ s/\s+/ /g;
    @devlist = split(/\s+/, $devstr);

    $pubipstr =~ s/\s+/ /g;
    @pubips = split(/\s+/, $pubipstr);
    
    for ($k=0; $k<@devlist; $k++) {
	$dev = $devlist[$k];

	if ($subnet ne "" && $dev ne "") {
	    print "CLEARING IP_SUBNET ARTIFACTS\n";
	    print "IP_SUBNET=$subnet\n";
	    $cmd = "ip addr flush dev $dev to $subnet/20";
	    print "RUNNING CMD: $cmd\n";
	    if (system($cmd)) {print "FAILED\n";$ret++;}
	    print "FINISHED RUNNING CMD: $cmd\n";
	}
	if ($pubipstr ne "" && $dev ne "") {
	    print "CLEARING MANAGED_IPS ARTIFACTS\n";
	    print "MANAGED_IPS=$pubipstr\n";
	    for ($j=0; $j<@pubips; $j++) {
		my $ip = $pubips[$j];
		$cmd = "ip addr del $ip/32 dev $dev >/dev/null 2>&1";
		print "RUNNING CMD: $cmd\n";
		if (system($cmd)>>8 == 1) {print "FAILED\n";$ret++;}
		print "FINISHED RUNNING CMD: $cmd\n";
	    }
	}

	print "CLEARING BRIDGES\n";
	if ($dev =~ /eucabr/) {
	    $cmd = "ifconfig $dev down";
	    print "RUNNING CMD: $cmd\n";
	    if (system($cmd)) {print "FAILED\n";$ret++;}
	    print "FINISHED RUNNING CMD: $cmd\n";
	    
	    $cmd = "brctl delbr $dev";
	    print "RUNNING CMD: $cmd\n";
	    if (system($cmd)) {print "FAILED\n";$ret++;}
	    print "FINISHED RUNNING CMD: $cmd\n";
	}

	print "CLEARING VLAN TAGGED IPS\n";
	if ($dev =~ /\.\d+/) {
	    $cmd = "vconfig rem $dev";
	    print "RUNNING CMD: $cmd\n";
	    if (system($cmd)) {print "FAILED\n";$ret++;}
	    print "FINISHED RUNNING CMD: $cmd\n";
	}
	
    }

    print "CLEARING IPTABLES RULES";
    $cmd = "iptables -F >/dev/null 2>&1";
    print "RUNNING CMD: $cmd\n";
    if (system($cmd)) {print "FAILED\n";$ret++;}
    print "FINISHED RUNNING CMD: $cmd\n";
    
    $cmd = "iptables -t nat -F >/dev/null 2>&1";
    print "RUNNING CMD: $cmd\n";
    if (system($cmd)) {print "FAILED\n";$ret++;}
    print "FINISHED RUNNING CMD: $cmd\n";

    $cmd = "iptables -P FORWARD ACCEPT >/dev/null 2>&1";
    print "RUNNING CMD: $cmd\n";
    if (system($cmd)) {print "FAILED\n";$ret++;}
    print "FINISHED RUNNING CMD: $cmd\n";

    chomp($iptchainstr = `iptables -L -n | grep Chain | grep -v INPUT | grep -v OUTPUT | grep -v FORWARD | awk '{print \$2}'`);
    print "IPT: $iptchainstr\n";
    @iptchains = split(/\s+/, $iptchainstr);
    for ($i=0; $i<@iptchains; $i++) {
	my $chain = $iptchains[$i];
	$cmd = "iptables -F $chain";
	print "RUNNING CMD: $cmd\n";
	if (system($cmd)) {print "FAILED\n";$ret++;}
	print "FINISHED RUNNING CMD: $cmd\n";
	$cmd = "iptables -X $chain";
	print "RUNNING CMD: $cmd\n";
	if (system($cmd)) {print "FAILED\n";$ret++;}
	print "FINISHED RUNNING CMD: $cmd\n";
    }

    return(0);
}

$file = shift @ARGV || "/tmp/2b_tested.lst";
exit(restore_network_state("$file"));
