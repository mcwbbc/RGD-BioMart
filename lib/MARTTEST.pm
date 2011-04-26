package MARTTEST;

####################
# martTest.pl
# by Andrew Vallejos
# July 2008
####################
#
# Purpose:
#	To create a suite of testing tools that will determine if a mart
#	has been created successfully
#
#####################

use lib "../lib";
use DBI;
use DBINFO;
use strict;

sub testMart
{
	my @inputs = @_;
	my $fail= 0;
	my %results;
	
	my $database = shift(@inputs) or $fail = 1;
	my $key = shift(@inputs) or $fail = 1;
	
	if($fail)
	{
		my $error = "Incorrect number of agruments!\n";
		error($error);
	}
	
	my $dbh = DBINFO::connectSQL($database)
		or $fail = 1;
		
	if ($fail)
	{
		my $error = DBI::errstr."\n";
		error($error);
	}
	
	my $sth = $dbh->prepare("SHOW TABLES;");
	$sth->execute;
	
	my (@main, @dms);
	
	while(my @results = $sth->fetchrow_array)
	{
		my $table = shift(@results);
		
		next if $table =~ /^meta/; #ignore BioMart config tables
		
		if($table =~ /__main$/)
		{	push(@main, $table);	}
		elsif($table =~ /__dm$/)
		{	push(@dms, $table);	}
		else
		{	print "Do not recognize $table\n";	}
	}
	
	while(my $mainTable = pop(@main))
	{
		while(my $dmTable = pop(@dms))
		{
			$results{$dmTable} = report($dbh, $mainTable, $dmTable, $key);
		}
	}
	
	return \%results;
	
	exit(0);
}

sub report
{
	my ($dbh, $main, $dm, $key) = @_;
	my $sql = "SELECT COUNT(*) FROM $main JOIN $dm ON $main.$key=$dm.$key;";
	my $sth = $dbh->prepare($sql);
	$sth->execute;
	return $sth->fetchrow_array;
}

sub Error
{
	print $_ . "Error in MartTest\n";
	exit(0);
}

1;
