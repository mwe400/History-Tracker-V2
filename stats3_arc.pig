--SET DEFAULT_PARALLEL 200;
REGISTER /home/hdn11/Scripts/hc-archive-common.jar;
--REGISTER /home/hdn11/Scripts/hcpig.jar;
REGISTER /home/hdn11/Scripts/hcpig_no_white_list.jar;
REGISTER /home/hdn11/Scripts/commons-validator-1.4.0.jar;


titles = LOAD '/nsfia/input/ows/NSF-OWS-2010-201304-EXTRACTION-PART*' USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata','Envelope.ARC-Header-Metadata.Target-URI','Envelope.ARC-Header-Metadata.Date','Envelope.ARC-Header-Metadata.Content-Type','Envelope.ARC-Header-Metadata.Content-Length') AS (links:chararray,target:chararray,date:chararray,contenttype:chararray,contentlength:chararray);


nonnulls = filter titles by links is not null;

--store titles  INTO '/home/hai/aProjects/HistoryCrawl/Data/IA/2_26_2014/titles' using PigStorage();

paths = foreach nonnulls generate org.sci.historycrawl.parser($0,$1,$2),$2,$3,$4;

--store paths  INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/hainguyen' using PigStorage();


i6 = foreach paths generate bagwati.url,$1,$2,$3;        
--store i6  INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/myoutput1' using PigStorage();


i7 = foreach i6 generate flatten($0) as words,org.sci.historycrawl.formatdate(SUBSTRING($1,0,10)),$2,$3;
--store i7  INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/myoutput2' using PigStorage();

i8 = foreach i7 generate org.sci.historycrawl.getsourceURL($0),org.sci.historycrawl.getdstURL($0),org.sci.historycrawl.getText($0),$1,$2,(long)$3;
--store i8 INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/myoutput3' using PigStorage();

i9 = group i8 by ($0,$1,$3);
--store i9 INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/myoutput4' using PigStorage();

i10 = foreach i9 generate FLATTEN(group),FLATTEN(TOP(1,0,i8.$2)),COUNT(i8),FLATTEN(TOP(1,0,i8.$4)),SUM(i8.$5);
--store i10 INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/myoutput5' using PigStorage();

i11 = filter i10 by $0 is not null;
--store i11 INTO '/home/hai/Projects/HistoryCrawl/Data/IA/2_26_2014/myoutput6' using PigStorage();

i12 = filter i11 by $1 is not null;
store i12 INTO '/nsfia/output/ows_fullURL' using PigStorage();
