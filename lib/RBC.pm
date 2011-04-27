package RBC;

=head1 RBC.pm

###############################################
#
# RBC.pm
# by Andrew Vallejos
#
# RGD BioMart Common Perl Module
# Library of Subroutines used by Update scripts
#
###############################################
#
# To turn off email reporting
# -please find #ERPT
# -and follow instructions
# -repeat for all update.pl scripts
#
# Subroutines:
#
#	createNew - run if this is the first time
#		a biomart database is being created
#		Arguements:
#		- database name
#		Returns:
#		- None
#
#	getTime - gets the current time and returns
#		it as a single number
#		Arguements:
#		- None
#		Returns
#		- time as YYYYMMDDHHMM (ie 200610311312)
#
#	date
#		Arguements:
#		- LWP::UserAgent head list
#		Returns:
#		- date that the file was last updated as YYYYMMDD
#
#	checkAge - checks the age of the new datafile against
#		the age of the last downloaded file
#		Arguements:
#		- age of new file
#		- data source
#		Returns:
#		- Boolean 1 if target file is newer
#
#	reportError - captures error messages and sends them
#		in email, if possible, before script dies
#		Arguements:
#		- error message
#		- subject line for email message
#		Returns:
#		- None
#
#	send_mail - sends email; should be modified to send
#		email to your people; allows script to communicate
#		with persons responsible
#		Arguements:
#		- message; email content
#		- subject line for email message
#
#	getNextMart - updates mart number
#		Arguements:
#		- data source
#		Returns:
#		- mart index
#
#	generateXML_mySql - updates the mart registry for a
#		MySql database
#		Arguements:
#		- the new database name
#		- the mart registry xml file
#		Returns:
#		- None
#
#	copyMetaData - copies the meta data tables and information
#		from the most recent mart database
#		Arguements:
#		- DBI connection for the new database (usually $dbh)
#		- the new database name
#		- the new mart index
#		- the age of the current data file
#		Returns:
#		- "No MetaData Found" if there is no previous mart db
#
#################################################################

=cut

require DBINFO;

# Add admin emails if using email alerts
my @emails = [];

sub createNew
{
	my $name = shift;
	my $namedate = $name.'date';
	my $nameindex = $name.'index';

	open(my $DATE, ">", $namedate) or reporError("cannot create $namedate");
	open(my $INDEX, ">", $nameindex) or reportError("cannot create $nameindex");
	print $DATE "0";
	print $INDEX "0";
	close $DATE;
	close $INDEX;
}

sub getTime
{
        my @time = localtime(time);
        #save time as YearMonthDayTime
        my $year = 1900+$time[5];
	  for(my $i =0; $i<5; $i++)
	  { $time[$i] = "0".$time[$i] if $time[$i] < 10; }
        return ("$year"."$time[4]"."$time[3]"."$time[2]"."$time[1]");
}

sub date
{
	my $headinfo = shift;
	my $data = $$headinfo{_headers}{'last-modified'};

	my @dateStamp = split(/\s/, $data);
	if($dateStamp[2] eq "Jan")    {$dateStamp[2] = '01'; }
	elsif($dateStamp[2] eq "Feb") {$dateStamp[2] = '02'; }
	elsif($dateStamp[2] eq "Mar") {$dateStamp[2] = '03'; }
	elsif($dateStamp[2] eq "Apr") {$dateStamp[2] = '04'; }
	elsif($dateStamp[2] eq "May") {$dateStamp[2] = '05'; }
	elsif($dateStamp[2] eq "Jun") {$dateStamp[2] = '06'; }
	elsif($dateStamp[2] eq "Jul") {$dateStamp[2] = '07'; }
	elsif($dateStamp[2] eq "Aug") {$dateStamp[2] = '08'; }
	elsif($dateStamp[2] eq "Sep") {$dateStamp[2] = '09'; }
	elsif($dateStamp[2] eq "Oct") {$dateStamp[2] = '10';}
	elsif($dateStamp[2] eq "Nov") {$dateStamp[2] = '11';}
	elsif($dateStamp[2] eq "Dec") {$dateStamp[2] = '12';}
	else {reportError ("Month not recognized: $dateStamp[2]", "RGD Update");}

	return "$dateStamp[3]"."$dateStamp[2]"."$dateStamp[1]";
}

sub checkAge
{
	my $newAge = shift;
	my $file = shift;
	$file .= "date";

	my $oldAge = -1;
	if(open(my $DATE, '<', $file))
	{
		$oldAge = <$DATE>;
		close $DATE;
	}
	if($newAge > $oldAge)
	{
		open(my $DATE, ">", $file);
		print $DATE "$newAge";
		close $DATE;
		return 1;
	}
	else
	{  	return 0;  }
}

sub reportError
{
	my $error = shift;
	my $subject = shift;
	#ERPT
	#uncomment to turn off email alerts
#	die($error);

	$error = "$error:".getTime();
	send_mail($error, $subject);
	die()
}

sub send_mail {
   	my $sendmail = "/usr/lib/sendmail -t";
	my $toAddresses = 'To: ' . join(',', @emails);
	my $fromAddresses = 'From: ' . $email[0];
	my $emailBody = shift;
	my $emailSubject = shift;
	my $email_message =  "$toAddresses\n $fromAdresses" .
			  "\nSubject: $emailSubject\n".
			  "$emailBody\n";


	if(open(MAIL, "|$sendmail"))
	{
		print MAIL $email_message;
		close MAIL;
	}
	else
	{
		print $email_message;
	}

}

sub getNextMart
{
	my $source = shift;
	my $file = $source."index";
	my $mart = 0;

	open (my $IN, '<', $file) and $mart = <$IN>;
	$mart++;
	close $IN;
	open (my $OUT, ">", $file);
	print $OUT "$mart";
	close $OUT;
	return $mart;
}

sub generateXML_mySql
{
#	use XML::XPath;
#
#	my ($newdb, $source, $xmlFile) = @_;
#
#	my $xp = XML::XPath->new(filename => $xmlFile);
#
#	my $nodeSet = $xp->findnodes_as_string(qq~//MartRegistry/child::*~);
#
#	my $newTable = qq~<?xml version="1.0" encoding="UTF-8"?>\n~.
#		qq~<!DOCTYPE MartRegistry>\n~.
#		qq~<MartRegistry>\n~.
#		qq~<MartDBLocation\n~.
#		qq~  name	= "$newdb"\n~.
#		qq~  displayName = "$source"\n~.
#		qq~  databaseType = "mysql"\n~.
#		qq~  host	= "~.DBINFO::getMySqlHost().qq~"\n~.
#		qq~  port	= "~.DBINFO::getMySqlPort().qq~"\n~.
#		qq~  database	= "$newdb"\n~.
#		qq~  schema	= "$newdb"\n~.
#		qq~  user	= "~.DBINFO::getMySqlUser().qq~"\n~.
#		qq~  password	= "~.DBINFO::getMySqlPwd().qq~"\n~.
#		qq~  visible	= "1" />\n~.
#		qq~  $nodeSet\n</MartRegistry>\n~;
#
#	open(XML, ">$xmlFile");
#	print XML "$newTable";
#
}

sub copyMetaData
{
	use DBI;
	#print "@_\n";
	my ($dbh, $newMart, $oldMart, $age) = @_;

	my @sqlMeta;

	$sqlMeta[0] = "CREATE TABLE meta_conf__dataset__main (".
		"dataset_id_key int( 11 ) NOT NULL default '0',".
		"dataset varchar( 100 ) default NULL ,".
		"display_name varchar( 100 ) default NULL ,".
		"description varchar( 200 ) default NULL ,".
		"type varchar( 20 ) default NULL ,".
		"visible int( 1 ) unsigned default NULL ,".
		"version varchar( 25 ) default NULL ,".
		"modified timestamp( 14 ) NOT NULL ,".
		"UNIQUE KEY dataset_id_key ( dataset_id_key ));";

	$sqlMeta[1] = "INSERT INTO meta_conf__dataset__main ".
		"SELECT * ".
		"FROM $oldMart.meta_conf__dataset__main ;";

	$sqlMeta[10] = "UPDATE meta_conf__dataset__main ".
		"SET display_name = '".$age."'".
		"WHERE type='TableSet';";

	$sqlMeta[2] = "CREATE TABLE meta_conf__interface__dm (".
		"dataset_id_key int( 11 ) default NULL ,".
		"interface varchar( 100 ) default NULL ,".
		"UNIQUE KEY dataset_id_key ( dataset_id_key , interface ));";

	$sqlMeta[3] = "INSERT INTO meta_conf__interface__dm ".
		"SELECT * ".
		"FROM $oldMart.meta_conf__interface__dm;";

	$sqlMeta[4] = "CREATE TABLE meta_conf__user__dm (".
		"dataset_id_key int( 11 ) default NULL ,".
		"mart_user varchar( 100 ) default NULL ,".
		"UNIQUE KEY dataset_id_key ( dataset_id_key , mart_user ));";

	$sqlMeta[5] = "INSERT INTO meta_conf__user__dm ".
		"SELECT * ".
		"FROM $oldMart.meta_conf__user__dm;";

	$sqlMeta[6] = "CREATE TABLE meta_conf__xml__dm (".
		"dataset_id_key int( 11 ) NOT NULL default '0',".
		"xml longblob,".
		"compressed_xml longblob,".
		"message_digest blob,".
		"UNIQUE KEY dataset_id_key ( dataset_id_key ));";

	$sqlMeta[7] = "INSERT INTO meta_conf__xml__dm ".
		"SELECT * ".
		"FROM $oldMart.meta_conf__xml__dm;";

	$sqlMeta[8] = "CREATE TABLE meta_version__version__main (".
		"version varchar( 10 ) default NULL);";

	$sqlMeta[9] = "INSERT INTO meta_version__version__main ".
		"SELECT * ".
		"FROM $oldMart.meta_version__version__main;";

	foreach (@sqlMeta)
	{  $dbh->do($_) or reportError("$DBI::errstr\n"); }

	return "";
}
1;
