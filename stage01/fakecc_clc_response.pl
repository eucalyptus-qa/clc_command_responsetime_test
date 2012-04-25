#!/usr/bin/perl

require "ec2ops.pl";

my $account = shift @ARGV || "eucalyptus";
my $user = shift @ARGV || "admin";

# need to add randomness, for now, until account/user group/keypair
# conflicts are resolved

$rando = int(rand(10)) . int(rand(10)) . int(rand(10));
if ($account ne "eucalyptus") {
    $account .= "$rando";
}
if ($user ne "admin") {
    $user .= "$rando";
}
$newgroup = "ebsgroup$rando";
$newkeyp = "ebskey$rando";

parse_input();
print "SUCCESS: parsed input\n";

setlibsleep(0);
print "SUCCESS: set sleep time for each lib call\n";

setremote($masters{"CLC"});
print "SUCCESS: set remote CLC: masterclc=$masters{CLC}\n";

discover_emis();
print "SUCCESS: discovered loaded image: current=$current_artifacts{instancestoreemi}, all=$static_artifacts{instancestoreemis}\n";

discover_zones();
print "SUCCESS: discovered available zone: current=$current_artifacts{availabilityzone}, all=$static_artifacts{availabilityzones}\n";

if ( ($account ne "eucalyptus") && ($user ne "admin") ) {
# create new account/user and get credentials
    create_account_and_user($account, $user);
    print "SUCCESS: account/user $current_artifacts{account}/$current_artifacts{user}\n";
    
    grant_allpolicy($account, $user);
    print "SUCCESS: granted $account/$user all policy permissions\n";
    
    get_credentials($account, $user);
    print "SUCCESS: downloaded and unpacked credentials\n";
    
    source_credentials($account, $user);
    print "SUCCESS: will now act as account/user $account/$user\n";
}
# moving along

setfailuretype("script");
print "SUCCESS: set failure mode to: script\n";

# build and install fake CC
build_and_deploy_fakeCC();
print "SUCCESS: built and deployed fake CC\n";

# test fakeCC availability
confirm_fakeCC();
print "SUCCESS: confirmed that fake CC is in place\n";
confirm_fakeCC();
print "SUCCESS: confirmed again that fake CC is in place\n";

my @means, @medians;
my $lastmean = -1;
my $lastmedian = -1;
my $avgupdown = $medupdown = 0;
my $i;
for ($i=0; $i<10; $i++) {
    # churn a bunch of instances through the system
    do_instance_churn(10);
    print "SUCCESS: churned a bunch of instances through the system\n";
    
    # test system response time
    test_response_time();
    print "SUCCESS: response time after instance churn mean=$current_artifacts{responseavg} median=$current_artifacts{responsemed}\n";
    $means[$i] = $current_artifacts{responseavg};
    $medians[$i] = $current_artifacts{responsemed};
    if ($lastmean != -1) {
	if ($current_artifacts{responseavg} > $lastmean) {
	    $avgupdown++;
	} else {
	    $avgupdown--;
	}
	if ($current_artifacts{responsemed} > $lastmedian) {
	    $medupdown++;
	} else {
	    $medupdown--;
	}
    }
    $lastmean = $current_artifacts{responseavg};
    $lastmedian = $current_artifacts{responsemed};
}

print "means: @means\n";
if ($avgupdown > 0) {
    print "Mean system response appears to be increasing: $avgupdown\n";
}
print "medians: @medians\n";
if ($medupdown > 0) {
    print "Median system response appears to be increasing: $medupdown\n";
}   

# restore real CC and config
#restore_realCC();
#print "SUCCESS: restored real CC\n";

doexit(0, "EXITING SUCCESS\n");
