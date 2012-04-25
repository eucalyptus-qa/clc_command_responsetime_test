use List::Util qw[min max];

$prefix = "euca";
$runat = "runat 30";
%cleanup_artifacts, %static_artifacts, %current_artifacts;
$remote_pre = "";
$remote_post = "";
$trycount = 180;
$ofile = "ubero";
%masters, %slaves, %roles;
$use_virtio = 0;
$networkmode="";
$ismanaged = 1;
$cleanup = 1;
$library_sleep = 0;
$devname = "/dev/sdc";
$emitype = "instancestoreemi";
$keypath = ".";

sub setkeypath {
    $keypath = shift @_ || ".";
    if (! -d "$keypath") {
	$keypath = ".";
	return(1);
    }
    return(0);
}

sub setcleanup {
    $cleanup = shift @_ || "yes";
    if ($cleanup eq "no") {
	$cleanup = 0;
    } else {
	$cleanup = 1;
    }
    return(0);
}

sub setfailuretype {
    #options are script, net, reboot
    my $type = shift @_ || "script";
    if ($type ne "script" && $type ne "net" && $type ne "reboot") {
	doexit(1, "FAILED: invalid failure type $type\n");
    }

    $current_artifacts{"failtype"} = $type;
    return(0);
}

sub control_component {
    my $ftype = $current_artifacts{"failtype"} || "script";
    if ($ftype eq "script") {
	return(control_component_script(@_));
    } elsif ($ftype eq "net") {
	return(control_component_net(@_));
    } elsif ($ftype eq "reboot") {
	return(control_component_reboot(@_));
    } else {
	doexit(1, "FAILED: could not find control driver for failtype $ftype\n");
    }
    return(0);
}

sub control_component_reboot {
    my $op = shift @_ || "START";
    my $component = shift @_ || "CLC";
    my $rank = shift @_ || "MASTER";
    my $ip, $cmd, $cleancmd;

    if ($rank eq "MASTER") {
	$ip = $masters{"$component"};
    } else {
	$ip = $slaves{"$component"};
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "FAILED: could not detemine component ip ($component, $rank, $op)\n");
    }
    $current_artifacts{"controlip"} = $ip;

    if ($op ne "STOP") {
	my $done=0;
	my $i;
	for ($i=0; $i<300 && !$done; $i++) {
	    print "\ttesting network connectivity: $ip\n";
	    $cmd = "$runat ping -c 1 $ip";
	    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	    if (!$rc && !$crc) {
		$done++;
	    }
	    sleep(1);
	}
	if (!$done) {
	    doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
	}

	return(control_component_script($op, $component, $rank));
    }

    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip 'reboot -f >/dev/null 2>&1 </dev/null &'";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
    }
    
    return(0);    
}

sub control_component_net {
    my $op = shift @_ || "START";
    my $component = shift @_ || "CLC";
    my $rank = shift @_ || "MASTER";
    my $ip, $cmd, $cleancmd;

    if ($rank eq "MASTER") {
	$ip = $masters{"$component"};
    } else {
	$ip = $slaves{"$component"};
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "FAILED: could not detemine component ip ($component, $rank, $op)\n");
    }
    $current_artifacts{"controlip"} = $ip;
    
    if ($op ne "STOP") {
	my $done=0;
	my $i;
	for ($i=0; $i<30 && !$done; $i++) {
	    print "\ttesting network connectivity: $ip\n";
	    $cmd = "$runat ping -c 1 $ip";
	    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
	    if (!$rc && !$crc) {
		$done++;
	    }
	    sleep(1);
	}
	if (!$done) {
	    doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
	}

	return(0);
    }

    $cmd = "$runat scp -o StrictHostKeyChecking=no cyclenet.pl root\@$ip:/tmp/";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
    }
    
    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip '/tmp/cyclenet.pl 60 $ip >/dev/null 2>&1 </dev/null &'";
    my ($rc, $crc, $buf) = piperun($cmd, "", "ubero");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($rc, $crc, $buf) = $cmd\n");
    }
    
    return(0);
}

sub control_component_script {
    my $op = shift @_ || "START";
    my $component = shift @_ || "CLC";
    my $rank = shift @_ || "MASTER";
    my $ip, $cmd, $cleancmd;

    if ($rank eq "MASTER") {
	$ip = $masters{"$component"};
    } else {
	$ip = $slaves{"$component"};
    }
    if (! ($ip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	print "\tWARN: could not detemine component ip ($component, $rank, $op), skipping\n";
	return(0);
#	doexit(1, "FAILED: could not detemine component ip ($component, $rank, $op)\n");
    }
    $current_artifacts{"controlip"} = $ip;

    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip '";
    if ($component =~ /CC\d+/) {
	$cmd .= " $current_artifacts{eucahome}/etc/init.d/eucalyptus-cc ";
	if ($op eq "STOP") {
	    $cmd .= "cleanstop";
	    $cleancmd = "$runat scp -o StrictHostKeyChecking=no cleannet.pl root\@$ip:/tmp/";
	    my ($rc, $crc, $buf) = piperun($cleancmd, "", "ubero");
	    if ($rc || $crc) {
		doexit(1, "FAILED: ($rc, $crc, $buf) = $cleancmd\n");
	    }

	    $cleancmd = "$runat scp -o StrictHostKeyChecking=no ../input/2b_tested.lst root\@$ip:/tmp/";
	    my ($rc, $crc, $buf) = piperun($cleancmd, "", "ubero");
	    if ($rc || $crc) {
		doexit(1, "FAILED: ($rc, $crc, $buf) = $cleancmd\n");
	    }
	    $cleancmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ip /tmp/cleannet.pl /tmp/2b_tested.lst";
	} else {
	    $cmd .= "cleanstart";
	}
	if ($cc_has_broker{"$component"}) {
	    $cmd .= "; $current_artifacts{eucahome}/etc/init.d/eucalyptus-cloud ";
	    if ($op eq "STOP") {
		$cmd .= "stop";
	    } else {
		$cmd .= "start";
	    }
	}
    } elsif ($component =~ /NC\d+/) {
	$cmd .= " $current_artifacts{eucahome}/etc/init.d/eucalyptus-nc ";
	if ($op eq "STOP") {
	    $cmd .= "stop";
	} else {
	    $cmd .= "start";
	}	
    } else {
	$cmd .= " $current_artifacts{eucahome}/etc/init.d/eucalyptus-cloud ";
	if ($op eq "STOP") {
	    $cmd .= "stop";
	} else {
	    $cmd .= "start";
	}		
    }
    $cmd .= "'";

    my ($crc, $rc, $buf) = piperun($cmd, "", "ubero");
    if ($crc || $rc) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }

    if ($cleancmd) {
	my ($rc, $crc, $buf) = piperun($cleancmd, "", "ubero");
	if ($rc || $crc) {
	    doexit(1, "FAILED: ($rc, $crc, $buf) = $cleancmd\n");
	}
    }

    return(0);
}

sub setrunat {
    my $newrunat = shift @_ || "runat 30";

    $runat = $newrunat;

    return(0);
}

sub print_all_metadata {
    print "cleanup_artifacts: \n";
    foreach $key (keys(%cleanup_artifacts)) {
	$val = $cleanup_artifacts{$key};
	print "\t$key=$val\n";
    }
    print "static_artifacts: \n";
    foreach $key (keys(%static_artifacts)) {
	$val = $static_artifacts{$key};
	print "\t$key=$val\n";
    }
    print "current_artifacts: \n";
    foreach $key (keys(%current_artifacts)) {
	$val = $current_artifacts{$key};
	print "\t$key=$val\n";
    }
}

sub parse_input {
    print "BEGIN PARSING INPUT FILE\n-------------------\n";
    my $inputfile = "../input/2b_tested.lst";
    open(FH, "$inputfile") or die "failed to open $inputfile";
    while(<FH>) {
	chomp;
	my $line = $_;
	if ($line =~ /BZR_BRANCH\s+(.*)/) {
	    my $fullbranch = $1;
	    my (@tmp) = split("/", $fullbranch);
	    my $len = @tmp;
	    my $branch = $tmp[$len-1];
	    $current_artifacts{"branch"} = $branch;
	}
	my ($ip, $distro, $version, $arch, $source, @component_str) = split(/\s+/, $line);
	if ($ip =~ /\d+\.\d+\.\d+\.\d+/ && $distro && $version && ($arch eq "32" || $arch eq "64") && $source && @component_str) {
	    foreach $component (@component_str) {
		$component =~ s/\[//g;
		$component =~ s/\]//g;
		if ($masters{"$component"}) {
		    $slaves{"$component"} = "$ip";
		} else {
		    $masters{"$component"} = "$ip";
		}
		$roles{"$component"} = 1;
		if ($distro =~ /VMWARE/ && $component =~ /NC(\d+)/) {
		    $cc_has_broker{"CC$1"} = 1;
		}
		if ($component =~ /NC\d+/) {
		    if ($distro eq "FEDORA" || $distro eq "DEBIAN" || ($distro eq "RHEL" && $version =~ /^6\./)) {
			$use_virtio = 1;
		    }
		}
	    }
	}
    }
    close(FH);
    foreach $component (keys(%roles)) {
	print "Component: $component Master: $masters{$component} Slave: $slaves{$component}\n";
    }
    
    my $this_mode = `cat ../input/2b_tested.lst | grep NETWORK`;
    chomp($this_mode);
    if( $this_mode =~ /^NETWORK\s+(\S+)/ ){
	my $mode = lc($1);
	if ($mode eq "system" || $mode eq "static") {
	    $ismanaged = 0;
	} else {
	    $ismanaged = 1;
	}
	$networkmode = $mode;
    }
    print "Network Mode: $networkmode\n";
    print "Use Virtio: $use_virtio\n";

    print "END PARSING INPUT FILE\n-------------------\n";

    if ($use_virtio) {
	$devname = "/dev/vda";
	print "Using virtio device name: $devname\n";
    }


    return(0);
}

sub discover_test_state {
    my $testkey = shift @_ || "ec2ops";
    my $instance, $group, $key, $ret=0;

    $cmd = "$runat $remote_pre $prefix-describe-keypairs $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep $testkey | tail -n 1 | awk '{print \$2}'", "ubero");
    if ($rc || !$buf || $buf eq "") {
	$ret=1;
    } else {
	$key = $buf;
	print "DISCOVERED KEY: $key\n";
    }

    $cmd = "$runat $remote_pre $prefix-describe-groups $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep $testkey | tail -n 1 | awk '{print \$3}'", "ubero");
    if ($rc || !$buf || $buf eq "") {
	$ret=1;
    } else {
	$group = $buf;
	print "DISCOVERED GROUP: $group\n";
    }

    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep $testkey | grep INST | grep -v erminated | grep -v hutting | tail -n 1", "ubero");
    if ($rc || !$buf || $buf eq "") {
	$ret=1;
    } else {
	($meh, $instance, $meh, $publicip, $privateip, $state, @meh) = split(/\s+/, $buf);
	print "BUF: |$buf|\n";
	if ($instance =~ /i-.*/ && $publicip =~ /\d+\.\d+\.\d+\.\d+/ && $privateip =~ /\d+\.\d+\.\d+\.\d+/ && $state ne "") {
	    print "DISCOVERED INSTANCE: $instance, $publicip, $privateip, $state\n";
	} else {
	    $ret = 1;
	}
    }

    if (!$ret) {
	$current_artifacts{"keypair"} = "$key";
	$current_artifacts{"keypairfile"} = "$keypath/$key" . ".priv";
	$current_artifacts{"group"} = "$group";
	$current_artifacts{"instance"} = "$instance";
	$current_artifacts{"instances"} .= "$instance ";
	$current_artifacts{"instanceip"} = "$publicip";
	$current_artifacts{"instanceprivateip"} = "$privateip";
	$current_artifacts{"instancestate"} = "$state";
	$current_artifacts{"instancestates"} .= "$state ";
	
	$cleanup_artifacts{"groups"} .= "$group ";
	$cleanup_artifacts{"instances"} .= "$instance ";
	$cleanup_artifacts{"keypairs"} .= "$key ";
	$cleanup_artifacts{"keypairfiles"} .= "$keypath/$key" . ".priv ";
    }

    
    return($ret);
}

sub discover_static_info {
    discover_emis();
    print "SUCCESS: discovered loaded image: current=$current_artifacts{instancestoreemi}, all=$static_artifacts{instancestoreemis}\n";

    discover_zones();
    print "SUCCESS: discovered available zone: current=$current_artifacts{availabilityzone}, all=$static_artifacts{availabilityzones}\n";

    discover_vmtypes();
    print "SUCCESS: discovered vmtypes: m1smallmax=$static_artifacts{m1smallmax} m1smallavail=$static_artifacts{m1smallavail}\n";
    
    return(0);
}

sub discover_vmtypes {
#    $cmd = "$runat $prefix-describe-availability-zones verbose'";
    $cmd = "$runat $remote_pre $prefix-describe-availability-zones verbose $remote_post";
    ($crc, $rc, $m1scount) = piperun($cmd, "grep m1.small | awk '{print \$4}'", "ubero");
    $m1scount = int($m1scount);
    if ($rc || $m1scount < 0) {
	doexit(1, "FAILED: ($crc, $rc, $m1scount) = $cmd\n");
    }
    $static_artifacts{m1smallavail} = $m1scount;
    
    ($crc, $rc, $m1scount) = piperun($cmd, "grep m1.small | awk '{print \$6}'", "ubero");
    $m1scount = int($m1scount);
    if ($rc || $m1scount < 0) {
	doexit(1, "FAILED: ($crc, $rc, $m1scount) = $cmd\n");
    }
    $static_artifacts{m1smallmax} = $m1scount;

    return(0);
}

sub register_snapshot {
    $snap = $current_artifacts{"snapshot"};

    if ( ! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }
    
    $cmd = "$runat $remote_pre $prefix-register -n testImage --root-device-name /dev/sda1 -b /dev/sda1=$snap $remote_post";
#    $cmd = "$prefix-register -n testImage --root-device-name /dev/sda1 -b /dev/sda1=$snap";
    ($crc, $rc, $emi) = piperun($cmd, "grep IMAGE | awk '{print \$2}'", "ubero");
    if ($rc || !$emi || $emi eq "" || !($emi =~ /.*mi-.*/)) {
	doexit(1, "FAILED: $cmd\n");
    }

    $cleanup_artifacts{"emis"} .= "$emi ";
    $current_artifacts{"ebsemi"} = "$emi";

    return(0);
}

sub authorize_ssh {
    return(authorize_ssh_from_cidr(@_));
}

sub authorize_ssh_from_cidr {
    my $group = shift @_ || $current_artifacts{group} || "default";
    my $cidr = shift @_ || "0.0.0.0/0";

    if (! $group || $group eq "") {
	doexit(1, "FAILED: invalid group '$group'\n");
    }
    
    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-authorize $group -P tcp -p 22 -s $cidr $remote_post";
    
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
    }

    $cleanup_artifacts{"rules"} .= "$group -P tcp -p 22 -s $cidr,";
    
    return(0);
}

sub find_instance_volume {
    $keypairfile = $current_artifacts{keypairfile} || shift @_;
    $instanceip = $current_artifacts{instanceip} || shift @_;
    if (! -f "$keypairfile") {
	doexit(1, "ERROR: cannot find keypairfile '$keypairfile'\n");
    }
    if ( ! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/ )) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }

    sleep($library_sleep);

    if ($use_virtio) {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip ls /dev/vd\*";
    } else {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip 'ls /dev/sd\* | grep -v sda'";
    }
    $done=0; 
    my $i;
    for ($i=0; $i<10 && !$done; $i++) {
	($crc, $rc, $buf) = piperun($cmd, "grep -v RUNAT | grep dev | tail -n 1", "$ofile");
	if ($rc || ! ($buf =~ /^\/dev\/.*/) ) {
	    print "\twaiting for dev to appear...\n";
	    sleep(1);
	} else {
	    $done++;
	}
    }
    if ($rc || ! ($buf =~ /^\/dev\/.*/) ) {
	doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
    }
    $current_artifacts{instancedevice} = $buf;
    
    return(0);
}

sub run_command {
    $icmd = shift @_ || "echo HELLO WORLD";
    
    sleep($library_sleep);
    
    $cmd = "$runat $icmd";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($crc || $rc) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub run_command_not {
    $icmd = shift @_ || "echo HELLO WORLD";
    
    sleep($library_sleep);
    
    $cmd = "$runat $icmd";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if (!$crc || $rc) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    return(0);
}

sub ping_instance_from_cc {
    $instanceip = shift @_ || $current_artifacts{instanceprivateip};
    my @ccips, $ccidx=0, $key;

    foreach $key (keys(%masters)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $masters{$key};
	    $ccidx++;
	}
    }
    foreach $key (keys(%slaves)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $slaves{$key};
	    $ccidx++;
	}
    }
    
    if (! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }
    
    sleep($library_sleep);
    
    my $i=0, $j=0;
    my $done=0;
    for ($i=0; $i<30 && !$done; $i++) {
	for ($j=0; $j<$ccidx && !$done; $j++) {
	    $ccip = $ccips[$j];
	    if ($ccip =~ /\d+\.\d+\.\d+\.\d+/) {
		$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ccip 'ping -c 1 $instanceip'";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if (!$rc && !$crc) {
		    $current_artifacts{instancecc} = $ccip;
		    $done++;
		} else {
		    print "\twaiting to be able to ping instance ($instanceip) from cc ($ccip): ($i/30)\n";
		}
	    }
	}
	if (!$done) {
	    sleep(1);
	}
    }
    if (!$done) {
	doexit(1, "FAILED: could not ping instance ($instanceip) from cc ($ccip)\n");
    }

    return(0);

}

sub run_instance_command {
    $icmd = shift @_ || "echo HELLO WORLD";
    $keypairfile = shift @_ || $current_artifacts{keypairfile};
    $instanceip = shift @_ || $current_artifacts{instanceip};

    if (! -f "$keypairfile" ) {
	doexit(1, "ERROR: cannot find keypairfile '$keypairfile'\n");
    }
    if (! ($instanceip =~ /\d+\.\d+\.\d+\.\d+/) ) {
	doexit(1, "ERROR: invalid instanceip '$instanceip'\n");
    }
    
    sleep($library_sleep);

    my $i=0;
    my $done=0;
    for ($i=0; $i<30 && !$done; $i++) {
	$cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip 'echo HELLO WORLD'";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if (!$rc && !$crc) {
	    $done++;
	} else {
	    print "\twaiting to be able to ssh to instance ($i/30)\n";
	    sleep(1);
	}
    }    
    if (!$done) {
	doexit(1, "FAILED: could not ssh to instance\n");
    }
    
    $cmd = "$runat ssh -o StrictHostKeyChecking=no -i $keypairfile root\@$instanceip '$icmd'";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
	doexit(1, "ERROR: running '$cmd' = ($crc, $rc)\n");
    }

    return(0);
}

sub setemitype {
    $emitype = shift @_ || "instancestoreemi";
    return(0);
}

sub run_instances {
    my $num = shift @_ || 1;

    my $emi = $current_artifacts{"$emitype"};
    my $keypair = $current_artifacts{keypair};
    my $zone = $current_artifacts{availabilityzone};
    my $group = $current_artifacts{group};

    $zone = "";

    if (! ($emi =~ /emi-.*/) ) {
	doexit(1, "ERROR: invalid emi '$emi'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-run-instances -n $num $emi";
    if ($keypair) {
	$cmd .= " -k $keypair";
    } 
    if ($zone) {
	$cmd .= " -z $zone";
    }
    if ($group) {
	$cmd .= " -g $group";
    }
    
    $cmd .= " $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep INSTANCE | awk '{print \$2}'", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: ($rc, $inst) = $cmd\n");
    }
    
    my @insts = split(/\s+/, $buf);
    foreach $inst (@insts) {
	if (!$inst || $inst eq "" || !($inst =~ /i-.*/)) {
	    print "WARN: insts=@insts, inst=$inst\n";
	} else {
	    $cleanup_artifacts{instances} .= "$inst ";
	    $current_artifacts{instance} = $inst;
	    $current_artifacts{instances} .= "$inst ";
	}
    }
    if (!$current_artifacts{instance}) {
	doexit(1, "FAILED: could not run instance\n");
    }
    $current_artifacts{instancestate} = "pending";
    $current_artifacts{instancestates} .= "pending ";

    return(0);
}

sub add_group {
    my $ingroup = shift @_ || "mygroup";
    
    if ($ingroup eq "default") {
	$current_artifacts{"group"} = "$ingroup";
	return(0);
    }
    
    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-add-group $ingroup -d '$ingroup' $remote_post";
    ($crc, $rc, $group) = piperun($cmd, "", "$ofile");
    if ($rc || !$group || $group eq "") {
	doexit(1, "FAILED: no group\n");
    }
    
    $cleanup_artifacts{"groups"} .= "$ingroup ";    
    $current_artifacts{"group"} = "$ingroup";
    if (!$current_artifacts{group}) {
	doexit(1, "FAILED: could not add group\n");
    }

    return(0);
}

sub source_credentials {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";

    $cmd = "$runat $remote_pre ls -l /root/$account-$user-qa/eucarc $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    
    setremote();
    
    return(0);
}

sub get_credentials {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";

    sleep($library_sleep);

    $cmd = "$runat $remote_pre euca-get-credentials -a $account -u $user /root/$account-$user-qa.zip $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }

    $cmd = "$runat $remote_pre unzip -o /root/$account-$user-qa.zip -d /root/$account-$user-qa/ $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || !$buf) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }
    
    return(0);
}

sub grant_allpolicy {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";
    my $doaccount=0, $douser=0;

    if ($user eq "admin") {
	return(0);
    }

    sleep($library_sleep);

    open(OFH, ">/tmp/thepolicy.$$");
    my $thepolicy = '{
 "Version":"2011-04-01",
 "Statement":[{
   "Sid":"1",
   "Effect":"Allow",
   "Action":"*",
   "Resource":"*"
 }]
}';
    print OFH "$thepolicy";
    close(OFH);

    run_command("scp -o StrictHostKeyChecking=no /tmp/thepolicy.$$ root\@$current_artifacts{remoteip}:/tmp/thepolicy");

    $cmd = "$runat $remote_pre euare-useruploadpolicy --delegate=$account -u $user -p allpolicy -f /tmp/thepolicy $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
    if ($rc || $crc) {
	doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
    }

    return(0);
}

sub create_account_and_user {
    my $account = shift @_ || "eucalyptus";
    my $user = shift @_ || "admin";
    my $doaccount=0, $douser=0;

    if ($account eq "eucalyptus") {
	$current_artifacts{"account"} = "$account";
    } else {
	$doaccount=1;
    }
    if ($user eq "admin") {
	$current_artifacts{"user"} = "$user";
    } else {
	$douser=1;
    }
    
    sleep($library_sleep);

    if ($doaccount) {
	$cmd = "$runat $remote_pre euare-accountcreate -a $account $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "tail -n 1 | grep $account | awk '{print \$1}'", "$ofile");
	if ($rc || $buf ne "$account") {
	    doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
	}
	
	$cleanup_artifacts{"accounts"} .= "$account ";    
	$current_artifacts{"account"} = "$account";
	if (!$current_artifacts{account}) {
	    doexit(1, "FAILED: could not add account\n");
	}
    }
    if ($douser) {
	$cmd = "$runat $remote_pre euare-usercreate --delegate=$account -u $user $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if ($rc) {
	    doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
	}
	
	$cleanup_artifacts{"users"} .= "$account/$user ";    
	$current_artifacts{"user"} = "$user";
	if (!$current_artifacts{user}) {
	    doexit(1, "FAILED: could not add user\n");
	}
    }

    return(0);
}

sub add_keypair {
    my $inkey = shift @_ || "mykey";

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-add-keypair $inkey $remote_post";
    ($crc, $rc, $key) = piperun($cmd, "", "$ofile");
    if ($crc || $rc || !$key || $key eq "") {
	doexit(1, "FAILED: no key\n");
    }
    open(FH, "> $keypath/$inkey.priv") || doexit(1, "ERROR: could not write to $keypath/$inkey.priv");
    print FH "$key";
    close(FH);
    system("chmod 0600 $keypath/$inkey.priv");
    
    $cleanup_artifacts{"keypairs"} .= "$inkey ";    
    $cleanup_artifacts{"keypairfiles"} .= "$keypath/$inkey" . ".priv ";    
    $current_artifacts{"keypair"} = "$inkey";
    $current_artifacts{"keypairfile"} = "$keypath/$inkey" . ".priv";
    if (!$current_artifacts{keypair}) {
	doexit(1, "FAILED: could not add keypair\n");
    }
    
    return(0);
}

sub attach_volume {
    my $remote = $devname;
    my $inst = $current_artifacts{instance};
    my $vol = $current_artifacts{volume};
    
    if ( ! ($remote =~ /\/dev.*/) ) {
	doexit(1, "ERROR: invalid remote dev name '$devname'\n");
    }

    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }
    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-attach-volume $vol -i $inst -d $remote $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    if ( ! ($buf =~ /vol-/)) {
	doexit(1, "FAILED: could not attach volume\n");
    }
    $current_artifacts{volumestate} = "attaching";
    
    return(0);
}

sub detach_volume {
    my $vol = $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-detach-volume $vol $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    if ( ! ($buf =~ /vol-/)) {
	doexit(1, "FAILED: could not detach volume\n");
    }
    $current_artifacts{volumestate} = "available";
    
    return(0);
}


sub delete_volume {
    my $vol = $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-delete-volume $vol $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    if ( ! ($buf =~ /vol-/)) {
	doexit(1, "FAILED: could not delete volume\n");
    }
    $current_artifacts{volumestate} = "deleted";
    
    return(0);
}

sub delete_snapshot {
    my $snap = $current_artifacts{snapshot};

    if (! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-delete-snapshot $snap $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep SNAPSHOT | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "snap-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    $current_artifacts{snapshotstate} = "deleted";
    
    return(0);
}

sub create_volume {
    my $size = shift @_ || 1;
    my $zone = $current_artifacts{availabilityzone};

    if (! $zone ) {
	doexit(1, "ERROR: invalid zone '$zone'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-create-volume -z $zone -s $size $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    $cleanup_artifacts{"volumes"} .= "$buf ";    
    $current_artifacts{"volume"} = $buf;
    $current_artifacts{"volumestate"} = "UNSET";
    if (!$current_artifacts{"volume"}) {
	doexit(1, "FAILED: could not create volume\n");
    }
    
    return(0);
}


sub create_snapshot_volume {
    my $snap = $current_artifacts{snapshot};
    my $zone = $current_artifacts{availabilityzone};

    if (! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }
    if (! $zone ) {
	doexit(1, "ERROR: invalid zone ID '$zone'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-create-volume --snapshot $snap -z $zone $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep VOLUME | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "vol-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    $cleanup_artifacts{"volumes"} .= "$buf ";    
    $current_artifacts{"volume"} = $buf;
    $current_artifacts{"volumestate"} = "UNSET";
    if (!$current_artifacts{"volume"}) {
	doexit(1, "FAILED: could not create volume\n");
    }
    
    return(0);
}

sub create_snapshot {
    my $vol = $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    $cmd = "$runat $remote_pre $prefix-create-snapshot $vol $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep SNAPSHOT | awk '{print \$2}'", "$ofile");
    if ($rc || !$buf || !$buf =~ "snap-") {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }

    $cleanup_artifacts{"snapshots"} .= "$buf ";    
    $current_artifacts{"snapshot"} = $buf;
    $current_artifacts{"snapshotstate"} = "pending";
    
    return(0);
}

sub create_image {
    $instance = $current_artifacts{"instance"};

    if ( ! ($instance =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$instance'\n");
    }
    
    $cmd = "$runat $remote_pre $prefix-create-image -n testImage $instance $remote_post";
    ($crc, $rc, $emi) = piperun($cmd, "grep IMAGE | awk '{print \$2}'", "ubero");
    if ($rc || !$emi || $emi eq "" || !($emi =~ /.*mi-.*/)) {
	doexit(1, "FAILED: $cmd\n");
    }

    $cleanup_artifacts{"emis"} .= "$emi ";
    $current_artifacts{"ebsemi"} = "$emi";

    return(0);
}


sub wait_for_instance {
    my $inst = shift @_ || $current_artifacts{instance};

    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep INSTANCE | awk '{print \$6}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state eq "running") {
	    $done++;
	} elsif ($state eq "shutting-down" || $state eq "terminated") {
	    doexit(1, "FAILED: waiting for instance to run (state went to $state)\n");
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance to run\n");
    }
    $current_artifacts{instancestate} = "running";

    return(0);
}

sub wait_for_instance_ip {
    return(wait_for_instance_ip_public(@_));
}

sub wait_for_instance_ip_public {
    my $inst = shift @_ || $current_artifacts{instance};
    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $ip) = piperun($cmd, "grep INSTANCE | awk '{print \$4}' | tail -n 1", "$ofile");
	print "\tIP: $ip\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $ip) = $cmd\n");
	} elsif ($ip && $ip ne "0.0.0.0") {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance public ip\n");
    }
    $current_artifacts{instanceip} = "$ip";

    return(0);
}

sub wait_for_instance_ip_private {
    my $inst = shift @_ || $current_artifacts{instance};
    if (! ($inst =~ /i-.*/) ) {
	doexit(1, "ERROR: invalid instance ID '$inst'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $inst $remote_post";
	($crc, $rc, $ip) = piperun($cmd, "grep INSTANCE | awk '{print \$5}' | tail -n 1", "$ofile");
	print "\tIP: $ip\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $ip) = $cmd\n");
	} elsif ($ip && $ip ne "0.0.0.0") {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for instance private ip\n");
    }
    $current_artifacts{instanceprivateip} = "$ip";

    return(0);
}

sub wait_for_volume_attach {
    my $vol = shift @_ || $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    my $done=0;

    sleep($library_sleep);
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $vol $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep VOLUME | grep $vol | awk '{print \$5,\$6}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state =~ /in-use/) {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume to be attached\n");
    }
    $current_artifacts{volumestate} = "in-use";

    return(0);
}


sub wait_for_volume {
    my $vol = shift @_ || $current_artifacts{volume};

    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    my $done=0;

    sleep($library_sleep);
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $vol $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep VOLUME | grep $vol | awk '{print \$5,\$6}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state =~ /available/) {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume to be available\n");
    }
    $current_artifacts{volumestate} = "available";

    return(0);
}

sub wait_for_snapshot {
    my $snap = shift @_ || $current_artifacts{snapshot};
    if (! ($snap =~ /snap-.*/) ) {
	doexit(1, "ERROR: invalid snapshot ID '$snap'\n");
    }

    my $done=0;

    sleep($library_sleep);
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-snapshots $snap $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep SNAPSHOT | awk '{print \$4}' | tail -n 1", "$ofile");
	print "\tSTATE: $state\n";
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state eq "completed") {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for snapshot to be completed\n");
    }
    $current_artifacts{snapshotstate} = "completed";

    return(0);
}

sub wait_for_volume_detach {
    my $vol = shift @_ || $current_artifacts{volume};
    if (! ($vol =~ /vol-.*/) ) {
	doexit(1, "ERROR: invalid volume ID '$vol'\n");
    }

    sleep($library_sleep);

    my $done=0;
    my $i;
    for ($i=0; $i<$trycount && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-volumes $vol $remote_post";
	($crc, $rc, $state) = piperun($cmd, "grep VOLUME | grep $vol | awk '{print \$5,\$6}' | tail -n 1", "$ofile");
	if ($rc) {
	    doexit(1, "FAILED: ($rc, $state) = $cmd\n");
	} elsif ($state =~ /available/) {
	    $done++;
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for volume to be detached\n");
    }
    $current_artifacts{volumestate} = "available";

    return(0);
}

sub discover_zones {
    my $zone, $buf;

    sleep($library_sleep);
    $cmd = "$runat $remote_pre $prefix-describe-availability-zones $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep AVAILABILITYZONE | awk '{print \$2}'", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: no zone\n");
    }
    my @zones = split(/\s+/, $buf);
    foreach $zone (@zones) {
	if (!$zone || $zone eq "") {
	    print "WARN: zones=@zones, zone=$zone\n";
	} else {
	    $static_artifacts{"availabilityzones"} .= "$zone ";
	    $current_artifacts{"availabilityzone"} = $zone;
	}
    }

    return(0);
}

sub discover_emis {
    my $emi, $buf;

    sleep($library_sleep);
    $cmd = "$runat $remote_pre $prefix-describe-images $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep IMAGE | grep -i 'mi-' | awk '{print \$2}'", "$ofile");
    if ($rc) {
	doexit(1, "FAILED: ($rc, $buf) = $cmd\n");
    }
    my @emis = split(/\s+/, $buf);
    foreach $emi (@emis) {
	if (!$emi  || $emi eq "" || !($emi =~ /.*mi-.*/)) {
	    print "WARN: emis=@emis, emi=$emi\n";
	} else {
	    $static_artifacts{"instancestoreemis"} .= "$emi ";
	    $current_artifacts{"instancestoreemi"} = $emi;
	}
    }

    return(0);
}

sub terminate_instances {
    my $num = shift @_ || 1;
    my $count=0;
    foreach $inst (split(/\s+/, $current_artifacts{"instances"})) {
	$cmd = "$runat $prefix-terminate-instances $inst";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if ($rc) {
	    doexit(1, "FAILED: ($crc, $rc, $buf) = $cmd\n");
	}
	$count++;
	if ($count >= $num) {
	    return(0);
	}
    }
    return(0);
}

sub piperun {
    my $cmd = shift @_;
    my $pipe = shift @_;
    my $uberofile = shift @_ || "/tmp/uberofile.$$";
    my $pipestr = "";

    if ($pipe) {
	$pipestr = "| $pipe";
    }
    
    system("$cmd > /tmp/tout.$$ 2>&1");
    $retcode = ${^CHILD_ERROR_NATIVE};

    chomp(my $buf = `cat /tmp/tout.$$ $pipestr`);
    $pipecode = system("cat /tmp/tout.$$ $pipestr >/dev/null 2>&1");

    system("echo '*****' >> $uberofile");
    system("echo CMD=$cmd >> $uberofile");
    my $rc = system("cat /tmp/tout.$$ >> $uberofile");
    unlink("/tmp/tout.$$");

    sleep(1);
    return($retcode, $pipecode, $buf);
}

sub doexit {
    my $code = shift @_;
    my $msg = shift @_;

    if ($msg) {
	print "$msg";
    }

    print_all_metadata();

    if ($cleanup || $code) {
	print "BEGIN CLEANING UP TEST ARTIFACTS\n----------------\n";

	print "Instances\n\t";
	foreach $inst (split(/\s+/, $cleanup_artifacts{"instances"})) {
	    print "$inst";
	    $cmd = "$runat $remote_pre $prefix-terminate-instances $inst $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";
	
	print "Images\n\t";
	foreach $emi (split(/\s+/, $cleanup_artifacts{"emis"})) {
	    print "$emi";
	    $cmd = "$runat $remote_pre $prefix-deregister $emi $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";
	
	print "Volumes\n\t";
	foreach $vol (split(/\s+/, $cleanup_artifacts{"volumes"})) {
	    print "$vol";
	    $cmd = "$runat $remote_pre $prefix-delete-volume $vol $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";
	
	print "Snapshots\n\t";
	foreach $snap (split(/\s+/, $cleanup_artifacts{"snapshots"})) {
	    print "$snap";
	    $cmd = "$runat $remote_pre $prefix-delete-snapshot $snap $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";

	print "Rules\n\t";
	foreach $rule (split(",", $cleanup_artifacts{"rules"})) {
	    print "$rule";
	    $cmd = "$runat $remote_pre $prefix-revoke $rule $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";

	print "Groups\n\t";
	foreach $group (split(/\s+/, $cleanup_artifacts{"groups"})) {
	    print "$group";
	    $cmd = "$runat $remote_pre $prefix-delete-group $group $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";

	print "Keypairs\n\t";
	foreach $key (split(/\s+/, $cleanup_artifacts{"keypairs"})) {
	    print "$key";
	    $cmd = "$runat $remote_pre $prefix-delete-keypair $key $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";
	
	print "Keypairfiles\n\t";
	foreach $keyf (split(/\s+/, $cleanup_artifacts{"keypairfiles"})) {
	    if ( -f "./$keyf" ) {
		print "$keyf";
		$cmd = "rm -f ./$keyf";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	    }
	}
	print "\n";

	$current_artifacts{account} = "";
	$current_artifacts{user} = "";
	setremote($current_artifacts{remoteip});

	print "Users\n\t";
	foreach $acuser (split(/\s+/, $cleanup_artifacts{"users"})) {
	    my ($account, $user) = split("/", $acuser);
	    print "$account/$user";
	    $cmd = "$runat $remote_pre euare-userdel --delegate=$account -R -u $user $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";

	print "Accounts\n\t";
	foreach $account (split(/\s+/, $cleanup_artifacts{"accounts"})) {
	    print "$account";
	    $cmd = "$runat $remote_pre euare-accountdel -r -a $account $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if ($crc || $rc) { print "(failed) "; } else { print "(success) "; }
	}
	print "\n";

	print "END CLEANING UP\n---------------\n";
    }

    if ( -f "$ofile" ) {
	print "BEGIN OUTPUT TRACE\n------------\n";
	system("cat $ofile");
	print "END OUTPUT TRACE\n------------\n";
    }
    exit($code);
}

sub setprefix {
    my $in = shift @_;
    if ($in && $in ne "") {
	$prefix = $in;
    }
    return(0);
}

sub setremote {
    my $remoteip = shift @_ || $current_artifacts{remoteip};
    if ($remoteip && $remoteip ne "") {
	if ($current_artifacts{account} && $current_artifacts{user}) {
	    $remote_pre = "ssh -o StrictHostKeyChecking=no root\@$remoteip 'source /root/$current_artifacts{account}-$current_artifacts{user}-qa/eucarc; ";
	} else {
	    $remote_pre = "ssh -o StrictHostKeyChecking=no root\@$remoteip 'source /root/eucarc; ";
	}
	$remote_post = "'";
	$cmd = "$runat $remote_pre uname -a $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if ($crc || $rc) {
	    doexit(1, "FAILED: could not run remote command: ($crc, $rc, $buf) = $cmd\n");
	}

	$cmd = "$runat $remote_pre ls -l /opt/eucalyptus/usr/sbin/euca_conf $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	if (!$crc) {
	    $remote_pre .= "export PATH=/opt/eucalyptus/usr/sbin:\$PATH; export EUCALYPTUS=/opt/eucalyptus; ";
	    $current_artifacts{eucahome} = "/opt/eucalyptus";
	} else {
	    $remote_pre .= "export EUCALYPTUS=/; ";
	    $current_artifacts{eucahome} = "/";
	}
	$current_artifacts{remoteip} = "$remoteip";
    }
    return(0);
}

sub setlibsleep {
    my $insleep = shift @_;
    if ($insleep >= 0 && $insleep < 3600) {
	$library_sleep = $insleep;
    } else {
	$library_sleep = 0;
    }
}

sub build_and_deploy_fakeCC {
    my @ccips, $ccidx=0, $key;
    
    foreach $key (keys(%masters)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $masters{$key};
	    $ccidx++;
	}
    }
    foreach $key (keys(%slaves)) {
	if ($key =~ /^CC/) {
	    $ccips[$ccidx] = $slaves{$key};
	    $ccidx++;
	}
    }
    
    sleep($library_sleep);
    $done=0;
    my $j;
    for ($j=0; $j<$ccidx; $j++) {
	$ccip = $ccips[$j];
	if ($ccip =~ /\d+\.\d+\.\d+\.\d+/) {
	    $cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ccip 'cd /root/euca_builder/$current_artifacts{branch}/cluster/; make fake'";
	    ($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
	    if (!$rc && !$crc) {
		$cmd = "$runat ssh -o StrictHostKeyChecking=no root\@$ccip 'cd /root/euca_builder/$current_artifacts{branch}/cluster/; make fakedeploy; cp $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf.orig; cat $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf | grep -v '^VNET_PUBLICIPS' > /tmp/meh.conf; " . 'echo VNET_PUBLICIPS=\"1.3.0.1-1.3.0.254 1.3.1.1-1.3.1.254\" >> /tmp/meh.conf' . "; cp /tmp/meh.conf $current_artifacts{eucahome}/etc/eucalyptus/eucalyptus.conf'";
		($crc, $rc, $buf) = piperun($cmd, "", "$ofile");
		if (!$rc && !$crc) {
		    $done++;
		    print "\tbuilt reconfigured and deployed fakeCC on $ccip\n";
		}
	    } else {
		print "\tfailed to build/reconfigure/deploy fakeCC on $ccip\n";
	    }
	}
	if (!$done) {
	    doexit(1, "FAILURE: could not install fakeCC on any CC\n");
	}
    }

    $oldrunat = "$runat";
    setrunat("runat 30");
    print "SUCCESS: set command timeout to 'runat 30'\n";

    control_component("STOP", "CC00", "MASTER");
    control_component("STOP", "CC00", "SLAVE");
    control_component("STOP", "CLC", "MASTER");
    control_component("STOP", "CLC", "SLAVE");
    control_component("START", "CLC", "MASTER");
    control_component("START", "CLC", "SLAVE");
    control_component("START", "CC00", "MASTER");
    control_component("START", "CC00", "SLAVE");

    setrunat("$oldrunat");
    print "SUCCESS: set command timeout to '$oldrunat'\n";
    return(0);
}



sub confirm_fakeCC {
    my $done=0, $i;
    for ($i=0; $i<30 && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-availability-zones verbose $remote_post";
	($crc, $rc, $m1scount) = piperun($cmd, "grep m1.small | awk '{print \$4}'", "ubero");
	$m1scount = int($m1scount);
	if (!$crc && !$rc && $m1scount > 2048) {
	    $done++;
	} else {
	    print "\twaiting for CLC to come back up with fakeCC\n";
	    sleep(1);
	}
    }
    if (!$done) {
	doexit(1, "FAILURE: CLC did not come back up with fakeCC\n");
    }
    discover_vmtypes();
    return(0);
}

sub restore_realCC {
    return(0);
}

sub run_fake_instance_scale {
    my $num = shift @_ || "10";
    my $emi = $current_artifacts{"$emitype"};

    my $currnum = 0;
    while($currnum < $num) {
	$newgroup = "iscale" .  int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10));
	$cmd = "$runat $remote_pre $prefix-add-group $newgroup -d '$newgroup' $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($crc || $rc || !$buf ) {
	    doexit(1, "FAILURE: could not add new group $newgroup\n");
	}
	$cmd = "$runat $remote_pre $prefix-run-instances -n 28 $emi -g $newgroup $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($crc || $rc || !$buf ) {
	    doexit(1, "FAILED: could not run more instances (curr=$currnum, goal=$num)\n");
	}
	$currnum += 28;
    }
    
    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep INST | awk '{print \$2}'", "ubero");
    if ($crc || $rc || !$buf ) {
	print "\tfailed to describe instances after all runs\n";
    } else {
	my @insts = split(/\s+/, $buf);
	$cleanup_artifacts{instances} = join(" ", @insts);
    }

    return(0);
}

sub confirm_fake_instance_scale {
    my $num = shift @_ || "10";
    my $currnum = 0;

    my $done=0;
    my $i;
    for ($i=0; $i<160 && !$done; $i++) {
	$cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "grep running | grep INST | awk '{print \$2}' | sort | uniq | wc | awk '{print \$1}'", "ubero");
	$currnum = int($buf);
	$current_artifacts{numinsts} = $currnum;
	if ($currnum >= $num) {
	    print "\tfound all instances running (curr=$currnum goal=$num)\n";
	    $done++;
	} else {
	    print "\twaiting for all instances to go to running (curr=$currnum goal=$num)\n";
	    sleep(1);
	}
    }
    if (!$done) {
	doexit(1, "FAILED: waiting for $num instances to go to running\n");
    }
    return(0);
}

sub do_instance_churn {
    my $num = shift @_ || "10";
    my $emi = $current_artifacts{"$emitype"};
    my $inst;
    my $currnum = 0, $realruns = 0;

    while($currnum < $num) {
	$newgroup = "iresponse" .  int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10)) . int(rand(10));
	$cmd = "$runat $remote_pre $prefix-add-group $newgroup -d '$newgroup' $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	if ($crc || $rc || !$buf ) {
	    doexit(1, "FAILURE: could not add new group $newgroup\n");
	}
	$cmd = "$runat $remote_pre $prefix-run-instances -n 10 $emi -g $newgroup $remote_post";
	($crc, $rc, $buf) = piperun($cmd, "grep INST | awk '{print \$2}'", "ubero");
	my @meh = split(/\s+/, $buf);
	my $numinsts = @meh;
	my $insts = join(" ", @meh);
	if ($crc || $rc || !$insts ) {
	    print "\tWARN: could not run more instances (curr=$currnum, goal=$num)\n";
	} else {
	    print "\tran instances $insts\n";
	    $realruns+=$numinsts;
	    
	    $cmd = "$runat $remote_pre $prefix-terminate-instances $insts $remote_post";
	    ($crc, $rc, $buf) = piperun($cmd, "", "ubero");
	    if ($crc || $rc) {
		print "\tWARN: failed to terminate instance $inst\n";
	    }
	}
	$currnum+=10;
    }
    
    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep INST | awk '{print \$2}'", "ubero");
    if ($crc || $rc || !$buf ) {
	print "\tfailed to describe instances after all runs\n";
    } else {
	my @insts = split(/\s+/, $buf);
	$cleanup_artifacts{instances} = join(" ", @insts);
    }

    print "\trealruns: $realruns goal: $num attempts: $currnum\n";
    
    return(0);    
}

sub test_response_time {
    my $cmd, $count, $sum;
    my @timings;

    $oldrunat = $runat;
    setrunat("runat 300");
    my $i;
    
    $sum = $count = 0;
    $cmd = "$runat $remote_pre (for i in `seq 1 101`; do /usr/bin/time -v sh -c $prefix-describe-instances; done) $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "grep Elapsed | awk '{print \$8}'", "ubero");
    my @times = split(/\s+/, $buf);
    my $j;
    for ($j=0; $j<@times; $j++) {
	if ($times[$j] =~ /(\d+):(\d+)\.(\d+)/) {
	    my $tot = 0;
	    $tot += $1*60;
	    $tot += $2;
	    $tot += $3 / 100;
	    $timings[$count] = $tot;
	    $sum += $tot;
	    $count++;
	}
    }
    if ($count > 0) {
	$mean = $sum / $count;
	$median = $timings[int($count/2)];
    } else {
	$mean = 0;
	$median = 0;
    }
    
    print "\ttrial_timings: @timings\n";
    print "\tstatistics: totaltrials=$count average=$mean median=$median\n";
    $current_artifacts{responseavg} = $mean;
    $current_artifacts{responsemed} = $median;
    setrunat("$oldrunat");
    return(0);
}

sub record_clc_state {
    sleep(300);
    $cmd = "$runat $remote_pre $prefix-describe-instances $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "sort -n", "ubero");
    if ($crc || $rc) {
	doexit(1, "FAILED: could not describe instances\n");
    }
    $current_artifacts{clcstate} = $buf;

    $cmd = "$runat $remote_pre $prefix-describe-addresses $remote_post";
    ($crc, $rc, $buf) = piperun($cmd, "sort -n", "ubero");
    if ($crc || $rc) {
	doexit(1, "FAILED: could not describe addresses\n");
    }
    $current_artifacts{clcstate} .= $buf;

    return(0);
}

sub compare_clc_states {
    my $oldstate = shift @_;

    if ($current_artifacts{clcstate} ne "$oldstate") {
	print "\tWARN: oldstate and newstate differ:\n";
	print "\t--------OLDSTATE-------\n";
	print "$oldstate\n";
	print "\t--------NEWSTATE-------\n";
	print "$current_artifacts{clcstate}\n";
    }
    return(0);
}

1;

