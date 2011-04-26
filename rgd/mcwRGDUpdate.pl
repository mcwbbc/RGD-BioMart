#!/usr/local/bin/perl

=head1 mcwRGDUpdate.pl

#################################
#
# Perl script to parse the RGD data files
#  and load into a Biomart repository
#
# Original Script by Dr. Simon Twigger
# Updated by Andrew Vallejos - Feb 2007
#
# Updated by Andrew Vallejos - July 2007
# -added disease ontology annotation
#
# Updated by Andrew Vallejos - June 2007
# -column indexes dynamically generated to ensure that FTP file
#  and script do not fall out of sync.
#
# Updated by Andrew Valleos - Mar 2007
# -split Uniprot Accessions into seperated dm table
#
#
# Requires the following modules:
# available from CPAN
# -LWP::UserAgent
# -DBI
# -DBD for your database
# Custom
# -RBC
# -DBINFO configure for your database
#
# To turn off email reporting
# -please find #ERPT
# -and follow instructions
# -repeat for RBC.pm
#
#
#################################

=cut

#################################
# Edit to change database names #
#################################

#the name for your biomart database
my $tempdb = "rgd_temp__mart";
my $dbName = "rgd__mart";
my $key = "rgd_id_key"; #used to test mart later

#name of any preexisting database for DBI connection
my $db = $dbName;

#location of Accessory files
my $location = '/scratch3/biomart-web/updateScripts/rgd/';
my $slimFile = $location.'rgd_goslim.txt';
my $prettyBiomart = $location.'rgd_clean_update.sql';
my $doFile = $location.'rgd_do_obo.txt';
#################################
# Edit to point to location of  #
# Perl modules                  #
#################################

use lib "/scratch3/biomart-web/updateScripts/lib";


##End of Required Editing#######

my $source = $locataion.'RGD';

use LWP::UserAgent;
use DBI;
use DBINFO;
use RBC;
use MARTTEST;
use strict;
####Setting up initial values##

my @testData; #used to query the db at the end


my $baseDir = "ftp://rgd.mcw.edu/pub/data_release";
my $genefile = '/GENES_RAT'; #modified for test set

my $geneUrl = "$baseDir/$genefile";

my $annotUrl = "http://www.geneontology.org/cgi-bin/downloadGOGA.pl/gene_association.rgd.gz";
my $goUrl = "http://www.geneontology.org/doc/GO.terms_and_ids";

my $doAnnotUrl = "$baseDir/annotated_rgd_objects_by_ontology/rattus_genes_do";

my $mpAnnotUrl = "$baseDir/annotated_rgd_objects_by_ontology/rattus_genes_mp";
my $mpUrl = "http://obo.cvs.sourceforge.net/*checkout*/obo/obo/ontology/phenotype/mammalian_phenotype.obo";

my $pwAnnotUrl = "$baseDir/annotated_rgd_objects_by_ontology/rattus_genes_pw";
my $pwUrl = "$baseDir/pathway.obo";

my $new = shift(@ARGV);
if ($new eq 'new')
{
	RBC::createNew($source);
}

####Connect to FTP sites and download files#

my $localLog = "Connecting to FTP Site: ".RBC::getTime()."\n";

my @urlList = ($geneUrl, $annotUrl, $goUrl, 
				$doAnnotUrl, $mpAnnotUrl, $mpUrl, 
				$pwAnnotUrl, $pwUrl);
my @results;
my $age;

for(my $i = 0; $i<@urlList; $i++)
{
	my $ua = LWP::UserAgent->new;
	if($i == 0)
	{
		my $headinfo = $ua->head($urlList[$i]);
		
		$age = RBC::date($headinfo);

		if(!RBC::checkAge($age, $source))
		{
			$localLog .= "No new RGD Gene Data: ".RBC::getTime()."\n";
			RBC::reportError($localLog, "RGD BioMart: No Change");	
			exit(0);
		}#END if

		$localLog .= "New RGD Gene Data Found: ".RBC::getTime()."\n";
	}#END if
	
	my $request = $ua->get($urlList[$i]) or
			RBC::reportError("$localLog\n$!", "RGD Update: Cannot get $urlList[$i]");
			
	$results[$i] = $request->content;		
}#END for


my ($genes, $annot, $go, $doAnnot, $mpAnnot, $mp, $pwAnnot, $pw)
	= @results; #Assign results to named variables

@results = undef; #Empty array
print @results;
####End fetching data######################

####Create database connection#############
my $dbtype = "mysql";
# use DBINFO.pm to store information
my $username = DBINFO::getMySqlUser();
my $passwd = DBINFO::getMySqlPwd();
my $port = DBINFO::getMySqlPort();
my $host = DBINFO::getMySqlHost();
my $dsn = "dbi:$dbtype:$db:$host:$port";
my $dbh = DBI->connect( $dsn, $username, $passwd )
   or RBC::reportError( "$localLog\nCould not connect to mysql database: $DBI::errstr\n", 'RGD Update' );

###Cleans existing databases################
#$dbh->do("drop database $tempdb;");

#my $createdb = "CREATE DATABASE $tempdb;";
#$dbh->do($createdb) or RBC::reportError("$localLog\n$DBI::errstr\n");

#$localLog .= "New database created: $tempdb: ".RBC::getTime()."\n\n";

$dbh->do("USE $tempdb;");
dropTables($dbh);
$localLog .= "Looking for Existing MetaData: ".RBC::getTime()."\n\n";
$new or $localLog .= RBC::copyMetaData($dbh,$tempdb, $dbName, $age);
$localLog .= "Meta data copied: ".RBC::getTime()."\n\n";
####Creat necessary tables#####
my $tables = tableList();

$localLog .= "Tables Created and Being Populated: ".RBC::getTime()."\n\n";

foreach (@$tables)
{  $dbh->do($_);  }

########End of Addition####################

my @lines = split(/\n/, $genes);

my %index;

foreach(@lines)
{
   if($_ =~ /GENE_RGD_ID/)
   {
        #Looks for header line and parses out column information
        my @header = split(/\t/, $_);
        
        for(my $i = 0; $i < @header; $i++)
        {
        	$index{$header[$i]} = $i;
        }
   }
   else
   {
		my @info = split(/\t/, $_);
        push(@testData, $info[0]);
        my @sql;

        $sql[0] = qq~INSERT INTO rgd_genes__genetable__main ~;
        # rgd_id, symbol, name, entrez_gene_id, description 
        $sql[0] .= qq~VALUES("$info[$index{GENE_RGD_ID}]","$info[$index{SYMBOL}]",
        			"$info[$index{NAME}]","$info[$index{ENTREZ_GENE}]",~; 
        $sql[0] .= qq~"$info[$index{GENE_DESC}]");~;

		# UniProt Accession
		my @uniprot = split(/,/,$info[$index{UNIPROT_ID}]);
		foreach my $entry (@uniprot)
		{
			my $x = qq~INSERT INTO rgd_genes__uniprot__dm ~;
			$x .= qq~VALUES("$info[$index{GENE_RGD_ID}]","$entry");~;
			push(@sql, $x);
			
		}
		
		# genbank_nucleotide
        my @nucleotide = split(/,/, $info[$index{GENBANK_NUCLEOTIDE}]);
        foreach my $entry (@nucleotide)
        {
        	my $x = qq~INSERT INTO rgd_genes__accessions__dm ~;
            $x .= qq~VALUES("$info[$index{GENE_RGD_ID}]","$entry","GENBANK_NUCLEOTIDE");~;
            push(@sql, $x);
        }

		# genbank_protein
        my @protein = split(/,/, $info[$index{GENBANK_PROTEIN}]);
        foreach my $entry (@protein)
        {
        	my $x = qq~INSERT INTO rgd_genes__accessions__dm ~;
            $x .= qq~VALUES("$info[$index{GENE_RGD_ID}]","$entry","GENBANK_PROTEIN");~;
            push(@sql, $x);
        }

		# rgd_id, chromosome, mapped_repseq, gene_type, start, stop, strand, version
		my $q = qq~INSERT INTO rgd_genes__map31__dm ~;
		$q .= qq~VALUES("$info[$index{GENE_RGD_ID}]","$info[$index{CHROMOSOME_31}]","",~;
		$q .= qq~"$info[$index{GENE_TYPE}]","$info[$index{START_POS_31}]","$info[$index{STOP_POS_31}]",~;
		$q .= qq~"$info[$index{STRAND_31}]","v3.1");~;
		
		my $q1 = qq~INSERT INTO rgd_genes__map34__dm ~;
		$q1 .= qq~VALUES("$info[$index{GENE_RGD_ID}]","$info[$index{CHROMOSOME_34}]","",~;
		$q1 .= qq~"$info[$index{GENE_TYPE}]","$info[$index{START_POS_34}]","$info[$index{STOP_POS_34}]",~;
		$q1 .= qq~"$info[$index{STRAND_34}]","v3.4");~;
		
		push(@sql, $q, $q1);
		
        foreach my $i (@sql)
        {  $dbh->do($i)
        	or RBC::reportError($localLog.$_.$DBI::errstr, "RGD Update");
		}
   }
}

###GO Annotation##############################
my $annotFile = "annotation";

open(ZIP, ">$annotFile.gz");
print ZIP "$annot";
close ZIP;

qx(gzip -d $annotFile.gz);

open(IN, "$annotFile");

my %idToTerm;
while(<IN>)
{
	chomp;
    my $line = $_;
    my $sql;
    if($line =~ /^!/)
    {    	#skip this line
    }
    else
    {
        my @data = split(/\t/, $line);
        my $rgdId = $data[1];
        my $qual = $data[3];
        my $GO_id = $data[4];
        my @temp = split(/\|/, $data[5]);
        my $ref = substr($temp[0], 4);
        my $evid = $data[6];
        my $wf = $data[7];
        my $aspect = $data[8];

        if(!exists($idToTerm{$GO_id}))
        {
        	$go =~ m|$GO_id\t([\w, \(\)\-]+)|;
        	$idToTerm{$GO_id} = $1;
        }
        my $goTerm = $idToTerm{$GO_id};

        $sql = qq~INSERT INTO rgd_genes__geneontology__dm ~;
        $sql .= qq~VALUES("$rgdId","$qual","$GO_id","$goTerm","$ref","$evid",~;
        $sql .= qq~"$wf","$aspect");~;

        $dbh->do($sql)
        	or RBC::reportError($localLog.$sql."\n\n".DBI::errstr, "RGD Update");

    }
}
close IN;
qx(gzip $annotFile);
###End GO Annotation#####################

open(IN, "$slimFile");

foreach my $key (keys(%idToTerm))
{	delete($idToTerm{$key});	}

###GO Slim Annotation####################
while(<IN>)
{
	chomp;
    my $line = $_;
    my $sql;
    if($line =~ /^!/)
    {    	#skip this line
    }
    else
    {
        my @data = split(/\t/, $line);
        my $rgdId = substr($data[1], 4);
        my $GO_id = $data[4];

        if(!exists($idToTerm{$GO_id}))
        {
        	$go =~ m|$GO_id\t([\w, \(\)\-]+)|;
        	$idToTerm{$GO_id} = $1;
        }
        my $goTerm = $idToTerm{$GO_id};

        $sql = qq~INSERT INTO rgd_genes__geneontology_slim__dm ~;
        $sql .= qq~VALUES("$rgdId","$GO_id","$goTerm");~;

        $dbh->do($sql)
        	or RBC::reportError($localLog.$sql."\n\n".DBI::errstr, "RGD Update");

    }
}
close IN;

###End GO Slim Annotation################

foreach my $key (keys(%idToTerm))
{	delete($idToTerm{$key});	}

###Disease Annotation####################
my $doAnnotFile = "$location/diseaseAnnotation.txt";

open(OUT, ">$doAnnotFile");
print OUT $doAnnot;
close OUT;

my $doInfo = '';

open(IN, "$doFile");
while(<IN>) { $doInfo .= $_; }
close IN;

open(IN, "$doAnnotFile");

while(<IN>)
{
	chomp;
    my $line = $_;
    my $sql;
    if($line =~ /^!/)
    {    	#skip this line
    }
    else
    {
        my @data = split(/\t/, $line);
        my $rgdId = $data[1];
        my $qual = $data[3];
        my $DO_id = $data[4];
        my @temp = split(/\|/, $data[5]);
        my $ref = substr($temp[0], 4);
        my $evid = $data[6];
        my $wf = $data[7];
        my $aspect = $data[8];

        if(!exists($idToTerm{$DO_id}))
        {
        	$doInfo =~ m|$DO_id\s+name: ([\w ,\-]+)|;
        	$idToTerm{$DO_id} = $1;
        }
        my $doTerm = $idToTerm{$DO_id};

        $sql = qq~INSERT INTO rgd_genes__diseaseontology__dm ~;
        $sql .= qq~VALUES("$rgdId","$qual","$DO_id","$doTerm","$ref","$evid",~;
        $sql .= qq~"$wf","$aspect");~;

        $dbh->do($sql)
        	or RBC::reportError($localLog.$sql."\n\n".DBI::errstr, "RGD Update");

    }
}
close IN;
###End Disease Annotation##################

foreach my $key (keys(%idToTerm))
{	delete($idToTerm{$key});	}

###Phenotype Annotation####################
my $mpAnnotFile = "$location/phenotypeAnnotation.txt";

open(OUT, ">$mpAnnotFile");
print OUT $mpAnnot;
close OUT;

open(IN, "$mpAnnotFile");

while(<IN>)
{
	chomp;
    my $line = $_;
    my $sql;
    if($line =~ /^!/)
    {    	#skip this line
    }
    else
    {
        my @data = split(/\t/, $line);
        my $rgdId = $data[1];
        my $qual = $data[3];
        my $MP_id = $data[4];
        my @temp = split(/\|/, $data[5]);
        my $ref = substr($temp[0], 4);
        my $evid = $data[6];
        my $wf = $data[7];
        my $aspect = $data[8];

        if(!exists($idToTerm{$MP_id}))
        {
        	$mp =~ m|$MP_id\s+name: ([\w ,\-]+)|;
        	$idToTerm{$MP_id} = $1;
        }
        my $mpTerm = $idToTerm{$MP_id};

        $sql = qq~INSERT INTO rgd_genes__mpontology__dm ~;
        $sql .= qq~VALUES("$rgdId","$qual","$MP_id","$mpTerm","$ref","$evid",~;
        $sql .= qq~"$wf","$aspect");~;

        $dbh->do($sql)
        	or RBC::reportError($localLog.$sql."\n\n".DBI::errstr, "RGD Update");

    }
}
close IN;
###End Phenotype Annotation##################

foreach my $key (keys(%idToTerm))
{	delete($idToTerm{$key});	}

###Pathway Annotation####################
my $pwAnnotFile = "$location/pathwayAnnotation.txt";

open(OUT, ">$pwAnnotFile");
print OUT $pwAnnot;
close OUT;

open(IN, "$pwAnnotFile");

while(<IN>)
{
	chomp;
    my $line = $_;
    my $sql;
    if($line =~ /^!/)
    {    	#skip this line
    }
    else
    {
        my @data = split(/\t/, $line);
        my $rgdId = $data[1];
        my $qual = $data[3];
        my $PW_id = $data[4];
        my @temp = split(/\|/, $data[5]);
        my $ref = substr($temp[0], 4);
        my $evid = $data[6];
        my $wf = $data[7];
        my $aspect = $data[8];

        if(!exists($idToTerm{$PW_id}))
        {
        	$pw =~ m|$PW_id\s+name: ([\w ,\-]+)|;
        	$idToTerm{$PW_id} = $1;
        }
        my $pwTerm = $idToTerm{$PW_id};

        $sql = qq~INSERT INTO rgd_genes__pwontology__dm ~;
        $sql .= qq~VALUES("$rgdId","$qual","$PW_id","$pwTerm","$ref","$evid",~;
        $sql .= qq~"$wf","$aspect");~;

        $dbh->do($sql)
        	or RBC::reportError($localLog.$sql."\n\n".DBI::errstr, "RGD Update");

    }
}
close IN;
###End Pathway Annotation##################

###########################################
#
# Test the temp mart, do not destroy old mart
# if data is bad.
#
###########################################

$localLog .= "Testing Temp Mart, no tables should have a value of 0\n\n";

my $test = MARTTEST::testMart($tempdb, $key);

foreach my $table (keys(%$test)) 
{
	if($$test{$table} == 0)
	{
		RBC::reportError($localLog."\nNull Table Error!\n$table : $$test{$table}\n", "RGD Update");
	}
	else
	{
		$localLog .= "$table : $$test{$table}\n";
	}
}

###Makes a pretty BioMart database#########
#$dbh->do("DROP DATABASE $dbName;");
#$dbh->do("CREATE DATABASE $dbName;");
$dbh->do("USE $dbName");
dropTables($dbh);
open(IN, $prettyBiomart);
while(<IN>)
{
	chomp;
	$dbh->do("$_");
}
close IN;

#######Slim GO SLIM Table##################
my @slim;

$slim[0] = "CREATE TABLE temp ";
$slim[0] .= "SELECT DISTINCT * FROM rgd_genes__geneontology_slim__dm;";

$slim[1] = "DROP TABLE rgd_genes__geneontology_slim__dm;";

$slim[2] = "CREATE TABLE rgd_genes__geneontology_slim__dm ";
$slim[2] .= "SELECT * FROM temp;";

$slim[3] = "DROP TABLE temp;";

foreach(@slim)
{
	$dbh->do("$_") or warn "$!";
}
#######End of Work Area####################

$new eq "new" and $localLog .= "New biomart db created please configure new biomart\n\n";

$localLog .= "Looking for MetaData: ".RBC::getTime()."\n\n";
$new or $localLog .= RBC::copyMetaData($dbh, $dbName, $tempdb, $age);
$localLog .= "Meta data copied: ".RBC::getTime()."\n\n";

$dbh->disconnect;
$localLog .= "Finished: ".RBC::getTime()."\n\n";

#ERPT
#Comment out send_mail and uncomment print for debugging
RBC::send_mail("$localLog", 'RGD Update');
#print "$localLog";

######Subroutines###########################################


sub tableList
{
		my @create;
		$create[0] = "CREATE TABLE rgd_genes__genetable__main";
		$create[0] .= "(rgd_id_key int(10), symbol varchar(20), ";
		$create[0] .= "name varchar(255), entrez_gene_id varchar(10), ";
        $create[0] .= "description text, ";
        $create[0] .= "PRIMARY KEY(rgd_id_key));";

        $create[1] = "CREATE TABLE rgd_genes__map31__dm";
        $create[1] .= "(rgd_id_key int(10), chromosome varchar(5), ";
        $create[1] .= "mapped_repseq varchar(20), score varchar(20), ";
        $create[1] .= "start int(10), stop int(10), ";
        $create[1] .= "strand char(2), version varchar(20), INDEX(rgd_id_key));";

        $create[2] = "CREATE TABLE rgd_genes__accessions__dm";
        $create[2] .= "(rgd_id_key int(10), accession varchar(30), ";
        $create[2] .= "accession_type varchar(30), INDEX(rgd_id_key));";

        $create[3] = "CREATE TABLE rgd_genes__geneontology__dm";
        $create[3] .= "(rgd_id_key int(10), qualifier varchar(10), ";
        $create[3] .= "GO_id varchar(10), GO_term varchar(100), ";
        $create[3] .= "DB_reference varchar(100), evidence char(3), ";
        $create[3] .= "with_from varchar(100), aspect char(3), ";
        $create[3] .= "INDEX(rgd_id_key));";

        $create[4] = "CREATE TABLE rgd_genes__geneontology_slim__dm";
        $create[4] .= "(rgd_id_key int(10), slim_id varchar(10), ";
        $create[4] .= "slim_term varchar(100), INDEX(rgd_id_key));";
        
        $create[5] = "CREATE TABLE rgd_genes__map34__dm";
        $create[5] .= "(rgd_id_key int(10), chromosome varchar(5), ";
        $create[5] .= "mapped_repseq varchar(20), score varchar(20), ";
        $create[5] .= "start int(10), stop int(10), ";
        $create[5] .= "strand char(2), version varchar(20), INDEX(rgd_id_key));";
        
        $create[6] = "CREATE TABLE rgd_genes__uniprot__dm";
        $create[6] .= "(rgd_id_key int(10), uniprot_acc varchar(10),";
        $create[6] .= "INDEX(rgd_id_key));";
        
        $create[7] = "CREATE TABLE rgd_genes__diseaseontology__dm";
        $create[7] .= "(rgd_id_key int(10), qualifier varchar(20), ";
        $create[7] .= "DO_id varchar(10), DO_term varchar(100), ";
        $create[7] .= "DB_reference varchar(100), evidence char(3), ";
        $create[7] .= "with_from varchar(100), aspect char(3), ";
        $create[7] .= "INDEX(rgd_id_key));";
        
        $create[8] = "CREATE TABLE rgd_genes__mpontology__dm";
        $create[8] .= "(rgd_id_key int(10), qualifier varchar(20), ";
        $create[8] .= "MP_id varchar(10), MP_term varchar(100), ";
        $create[8] .= "DB_reference varchar(100), evidence char(3), ";
        $create[8] .= "with_from varchar(100), aspect char(3), ";
        $create[8] .= "INDEX(rgd_id_key));";

        $create[9] = "CREATE TABLE rgd_genes__pwontology__dm";
        $create[9] .= "(rgd_id_key int(10), qualifier varchar(20), ";
        $create[9] .= "PW_id varchar(10), PW_term varchar(100), ";
        $create[9] .= "DB_reference varchar(100), evidence char(3), ";
        $create[9] .= "with_from varchar(100), aspect char(3), ";
        $create[9] .= "INDEX(rgd_id_key));";
        
	return \@create;
}

sub dropTables
{
		my $dbh = shift;
		
		my @drop;
		$drop[0] = "DROP TABLE IF EXISTS rgd_genes__genetable__main;";

        $drop[1] = "DROP TABLE IF EXISTS rgd_genes__map31__dm;";

        $drop[2] = "DROP TABLE IF EXISTS rgd_genes__accessions__dm";

        $drop[3] = "DROP TABLE IF EXISTS rgd_genes__geneontology__dm;";

        $drop[4] = "DROP TABLE IF EXISTS rgd_genes__geneontology_slim__dm;";
        
        $drop[5] = "DROP TABLE IF EXISTS rgd_genes__map34__dm;";
        
        $drop[6] = "DROP TABLE IF EXISTS rgd_genes__uniprot__dm;";
        
        $drop[7] = "DROP TABLE IF EXISTS rgd_genes__diseaseontology__dm;";
        
        $drop[8] = "DROP TABLE IF EXISTS rgd_genes__mpontology__dm;";

        $drop[9] = "DROP TABLE IF EXISTS rgd_genes__pwontology__dm;";
        
        $drop[10] = "DROP TABLE IF EXISTS meta_conf__dataset__main;";
        
        $drop[11] = "DROP TABLE IF EXISTS meta_conf__interface__dm;";
        
        $drop[12] = "DROP TABLE IF EXISTS meta_conf__user__dm;";
        
        $drop[13] = "DROP TABLE IF EXISTS meta_conf__xml__dm;";
        
        $drop[14] = "DROP TABLE IF EXISTS meta_version__version__main;";
        
        foreach my $x (@drop)
        {  $dbh->do($x);	}
	
}
sub testDB
{
	my $results = '';
	my $testSql = "SELECT * FROM rgd_genes__genetable__main WHERE rgd_id_key='$testData[0]';";
	my $sth = $dbh->prepare("$testSql");
	$sth->execute;
	while(my @row = $sth->fetchrow_array)
	{  $results .= "@row\n\n";  }

	my $lastEntry = pop(@testData);
	$testSql = "SELECT * FROM rgd_genes__genetable__main WHERE rgd_id_key='$lastEntry';";
	my $sth2 = $dbh->prepare("$testSql");
	$sth2->execute;
	while(my @row = $sth2->fetchrow_array)
	{  $results .= "@row\n\n";  }

	return $results;
}

__END__

sub process_ID
