package sci.rutgers.edu;

import java.io.*;
import java.io.IOException;
import java.util.*;
import java.text.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
//import org.archive.format.json.JSONView;
//import org.json.JSONException;
//import org.json.JSONObject;

public class DashBoard extends Configured implements Tool {
	public static long distinct_counter = 0;
	public static long total_counter = 0;
	public static long total_TLD = 0;
	public static long total_TML = 0;
	

	public static class PreProcessingDataMap extends Mapper<Object, Text, Text, NullWritable> {
        private Text jsonLine = new Text();
		//private String[] jsonArrayString;
		private String jsonString = new String("");
		private String tempString = new String("");
		//private JSONView view;
		static enum Counters { TOTAL_RECORD, TOTAL_RECORD_ERROR}
		int lastIndex = -1;
		int bgIndex = -1;
		int edIndex = -1;
		int json = 0;

		private boolean isIP(String sIP)
		{/*
			if(sIP.isEmpty())
			{
				return true;
			}
*/
			char [] cN = sIP.toCharArray();
			for(int i =0 ; i<cN.length ;i++)
			{
				if(cN[i] != '.' && cN[i] != '0' && cN[i] != '1' && cN[i] != '2' && cN[i] != '3' && cN[i] != '4' && cN[i] != '5' && 
						cN[i] != '6' && cN[i] != '7' && cN[i] != '8' && cN[i] != '9')
				{
					return false;
				}
			}
			return true;
		}

		private void UpdateListData(Dictionary d, String url, Long c){
			Long KInDict = null;
			if (url != null){
				KInDict = (Long) d.get(url);
				if (KInDict != null){
					KInDict = KInDict + c;
					d.put(url, KInDict);
				} else {
					KInDict = c;
					d.put(url, KInDict);
				}
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
		        String line = value.toString();
				String Linksl;
				String ptSearch;
				String vTargetURI = new String("");
				String vDate= new String("");
				String vContentLength= new String("");
				String vContentType= new String("");
				String vDestURL= new String("");
				String vPart = new String("");
				String vKey = new String("");

				Dictionary InternalURLList = new Hashtable<String, Long>();
				Dictionary ExternalURLList = new Hashtable<String, Long>();

				int vInternalURL = 0;
				int vCount = 0;
			

				//Configuration conf = context.getConfiguration();
				//String fieldArgs = conf.get("fieldArgs");
			
				lastIndex = line.indexOf("{\"Envelope\":");
			

				if (lastIndex >=0) {

					if (json == 0) {
						//Text output version
						jsonString = new String("");

						ptSearch = new String("Header-Metadata\":");
						bgIndex = line.indexOf(ptSearch) ;
						edIndex = line.indexOf("Header-Length\"", bgIndex + ptSearch.length());
						if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
							tempString = line.substring(bgIndex + ptSearch.length(),edIndex);		
						
							vTargetURI = new String("");
							ptSearch = new String("Target-URI\":\"");
							bgIndex = tempString.indexOf(ptSearch) ;
							edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
							if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
								vTargetURI = tempString.substring(bgIndex + ptSearch.length(), edIndex);
								vTargetURI = vTargetURI.toLowerCase();
								String vTargetURI_Org = vTargetURI;

								bgIndex = vTargetURI.indexOf("https://");
								if (bgIndex > 0 ) {
									vTargetURI = vTargetURI.substring(bgIndex + 8);								
								} else { 
									bgIndex = vTargetURI.indexOf("http://");
									if ( bgIndex >= 0) {
										vTargetURI = vTargetURI.substring(bgIndex + 7);
									}
								}

								bgIndex = vTargetURI.indexOf("www");
								if (bgIndex >=0) {
									bgIndex = vTargetURI.indexOf(".");
									if (bgIndex >=0) {
										vTargetURI = vTargetURI.substring(bgIndex+1);
									} else {
										bgIndex = vTargetURI.indexOf("www");
										vTargetURI = vTargetURI.substring(bgIndex + 3);
									}
								}
								
								bgIndex = vTargetURI.indexOf("#");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								} 
								bgIndex = vTargetURI.indexOf("..");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								} 
								bgIndex = vTargetURI.indexOf("|");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								} 
								bgIndex = vTargetURI.indexOf("/");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}
								bgIndex = vTargetURI.indexOf(":");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}
								bgIndex = vDestURL.indexOf("&");
								if ( bgIndex >= 0) {			
									vDestURL = vDestURL.substring(0, bgIndex);
								}
								bgIndex = vDestURL.indexOf("%");
								if ( bgIndex >= 0) {			
									vDestURL = vDestURL.substring(0, bgIndex);
								}
								bgIndex = vDestURL.indexOf("?");
								if ( bgIndex >= 0) {			
									vDestURL = vDestURL.substring(0, bgIndex);
								}
								bgIndex = vDestURL.indexOf("\\");
								if ( bgIndex >= 0) {			
									vDestURL = vDestURL.substring(0, bgIndex);
								}
								bgIndex = vDestURL.indexOf("+");
								if ( bgIndex >= 0) {			
									vDestURL = vDestURL.substring(0, bgIndex);
								}
							}
						
						
							if (!isIP(vTargetURI) && vTargetURI.length() > 0) {

								ptSearch = new String("Date\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vDate = tempString.substring(bgIndex + ptSearch.length(), edIndex);	
									if (vDate.length() >= 8) {
										String vTD = vDate.replaceAll("-","");
										vDate = vTD.substring(0,8);
									}	
										
								}

								ptSearch = new String("\"Content-Type\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentType = tempString.substring(bgIndex + ptSearch.length(), edIndex);
									int vCheck_warc = vContentType.indexOf(";");
									if (vCheck_warc > 0 ) {
										vContentType = vContentType.substring(0, vCheck_warc);
									}							
								}

								ptSearch = new String("\"Content-Length\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentLength = tempString.substring(bgIndex + ptSearch.length(), edIndex);							
								}	
							
								jsonString = new String("");
								ptSearch = new String("\"HTML-Metadata\":");
								bgIndex = line.indexOf(ptSearch) ;
								edIndex = line.indexOf("\"}],\"", bgIndex + ptSearch.length());

								String listRef = new String("");
								String vDestURLTemp = new String("");

								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									tempString = line.substring(bgIndex + ptSearch.length(), edIndex);
									ptSearch = new String("\"url\":\"");
									bgIndex = tempString.indexOf(ptSearch) ;

									while (bgIndex >=0) {
										edIndex = tempString.indexOf("\"},", bgIndex + ptSearch.length());
										int bgIndex1 = -1;
										int edIndex1 = -1;
										if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
											vPart = tempString.substring(tempString.lastIndexOf(":", bgIndex)+1, bgIndex - 1);
											vPart = vPart.toLowerCase();
											if (vPart.indexOf("href")>0){
												vPart = "\"text/html\"";
											}else if (vPart.indexOf("img")>0){
												vPart = "\"image\"";
											} else if (vPart.indexOf("get")>0 || vPart.indexOf("post")>0 || vPart.indexOf("embed")>0 ){
												vPart = new String("\"application\"");
											}

											vDestURL = tempString.substring(bgIndex + ptSearch.length(), edIndex);
											vDestURLTemp = vDestURL;
											vDestURL = vDestURL.toLowerCase();
											tempString = tempString.substring(edIndex);	

											if (!isIP(vDestURL)) {
												bgIndex1 = vDestURL.indexOf("https://");
												if (bgIndex1 < 0 ) {
													bgIndex1 = vDestURL.indexOf("http://");
													if ( bgIndex1 >= 0) {
														vDestURL = vDestURL.substring(bgIndex1 + 7);
													}
												} else {
													vDestURL = vDestURL.substring(bgIndex1 + 8);
												}

												if ( bgIndex1 >= 0) {
										
													bgIndex1 = vDestURL.indexOf("www");
													if (bgIndex1 >=0) {
														bgIndex1 = vDestURL.indexOf(".");
														if (bgIndex1 >=0) {
															vDestURL = vDestURL.substring(bgIndex1+1);
														} else {
															bgIndex1 = vDestURL.indexOf("www");
															vDestURL = vDestURL.substring(bgIndex1 + 3);
														}

														bgIndex1 = vDestURL.indexOf("/");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														} 
														bgIndex1 = vDestURL.indexOf(":");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("|");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("&");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("%");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("?");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("\\");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("+");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0) {
															if (!isIP(vDestURL)) {
																vInternalURL = vInternalURL + 1;
																vKey = vPart + ":\"" + vDestURL + "\"";
																UpdateListData(InternalURLList, vKey, new Long(1));
															}
														} else {
															if (!isIP(vDestURL)) {
																vCount = vCount + 1;
																vKey =  vPart + ":\"" + vDestURL + "\"";
																UpdateListData(ExternalURLList, vKey, new Long(1));	
															}
														}
													} else {
														
														String firstChar = vDestURL.substring(0,1);
														if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0 || vDestURL.indexOf(".html")>=0|| vDestURL.indexOf(".jpg")>=0||  firstChar.equalsIgnoreCase(".") || firstChar.equalsIgnoreCase("/") ||firstChar.equalsIgnoreCase("#") || firstChar.equalsIgnoreCase("$") || firstChar.equalsIgnoreCase("@") || firstChar.equalsIgnoreCase("%") ||
															vDestURL.indexOf("?")>=0 || vDestURL.indexOf(".gif")>=0 || vDestURL.indexOf(".jpeg")>=0 || vDestURL.indexOf("(")>=0 || vDestURL.indexOf(")")>=0 || vDestURL.indexOf(".")< 0  || vDestURL.indexOf(",")>=0 || vDestURL.indexOf(".htm")>=0 ||vDestURL.indexOf(".exe")>=0) {
															if (!isIP(vDestURL)) {
																vInternalURL = vInternalURL + 1;
																vKey = vPart + ":\"" + vDestURL + "\"";
																UpdateListData(InternalURLList, vKey, new Long(1));
															}
														} else {
															bgIndex1 = vDestURL.indexOf("/");
															if (vDestURL.indexOf("/", bgIndex1 + 1)>=0) {
																if (!isIP(vDestURL)) {
																	vInternalURL = vInternalURL + 1;
																	vKey = vPart + ":\"" + vDestURL + "\"";
																	UpdateListData(InternalURLList, vKey, new Long(1));
																}
															} else {
																bgIndex1 = vDestURL.indexOf("/");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																} 
																bgIndex1 = vDestURL.indexOf(":");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																bgIndex1 = vDestURL.indexOf("|");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																bgIndex1 = vDestURL.indexOf("&");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																bgIndex1 = vDestURL.indexOf("%");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																bgIndex1 = vDestURL.indexOf("?");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																bgIndex1 = vDestURL.indexOf("\\");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																bgIndex1 = vDestURL.indexOf("+");
																if ( bgIndex1 >= 0) {			
																	vDestURL = vDestURL.substring(0, bgIndex1);
																}
																if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0) {
																	if (!isIP(vDestURL)) {
																		vInternalURL = vInternalURL + 1;
																		vKey = vPart + ":\"" + vDestURL + "\"";
																		UpdateListData(InternalURLList, vKey, new Long(1));
																	}
																} else {
																	if (!isIP(vDestURL)) {
																		vCount = vCount + 1;
																		vKey =  vPart + ":\"" + vDestURL + "\"";
																		UpdateListData(ExternalURLList, vKey, new Long(1));	
																	}
																}
															}
														}
														
													}
												} else {
													if (!isIP(vDestURL)) {
														vInternalURL = vInternalURL + 1;
														vKey = vPart + ":\"" + vDestURL + "\"";
														UpdateListData(InternalURLList, vKey, new Long(1));
													}
													String firstChar = vDestURL.substring(0,1);
													if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0 || vDestURL.indexOf("../")==0 || vDestURL.indexOf("#") >=0 || vDestURL.indexOf(".html")>=0|| vDestURL.indexOf("%")>=0||  vDestURL.indexOf(".jpg")>=0||  vDestURL.indexOf("@")>=0||  vDestURL.indexOf(":")>=0||  vDestURL.indexOf("/")>=0||  vDestURL.indexOf("./")>=0||  vDestURL.indexOf("//")>=0|| firstChar.equalsIgnoreCase(".") || firstChar.equalsIgnoreCase("/") ||firstChar.equalsIgnoreCase("#") || firstChar.equalsIgnoreCase("$") || firstChar.equalsIgnoreCase("@") ||
															vDestURL.indexOf("?")>=0 || vDestURL.indexOf(".gif")>=0 || vDestURL.indexOf(".jpeg")>=0 || vDestURL.indexOf("(")>=0 || vDestURL.indexOf(")")>=0 || vDestURL.indexOf(".")< 0  || vDestURL.indexOf(",")>=0 || vDestURL.indexOf(".htm")>=0 ||vDestURL.indexOf(".exe")>=0) {

														vInternalURL = vInternalURL + 1;
														vKey = vPart + ":\"" + vDestURL + "\"";
														UpdateListData(InternalURLList, vKey, new Long(1));
													} else {
														bgIndex1 = vDestURL.indexOf("/");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														} 
														bgIndex1 = vDestURL.indexOf(":");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.indexOf("|");
														if ( bgIndex1 >= 0) {			
															vDestURL = vDestURL.substring(0, bgIndex1);
														}
														bgIndex1 = vDestURL.lastIndexOf(".");
														if ( bgIndex1 >= 0) {			
															edIndex1 = vDestURL.lastIndexOf(".", bgIndex1);
															String v1;
															String v2;
															if (edIndex1 >=0 && edIndex1 <  bgIndex1) {
																v1 = vDestURL.substring(edIndex1);
																if (vTargetURI.length() >= (vDestURL.length() - edIndex1)) {
																	v2 = vTargetURI.substring(edIndex1);
																} else {
																	v2 = vTargetURI;
																}
																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																	vKey =  vPart + ":\"" + vDestURL + "\"";
																	UpdateListData(InternalURLList, vKey , new Long(1));
																} else {
																	vCount = vCount + 1;
																	vKey =  vPart + ":\"" + vDestURL + "\"";
																	UpdateListData(ExternalURLList, vKey, new Long(1));																	
																}

															} else {
																v1 = vDestURL;
																v2 = vTargetURI;

																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																	vKey =  vPart + ":\"" + vDestURL + "\"";
																	UpdateListData(InternalURLList, vKey , new Long(1));
																} else {
																	vCount = vCount + 1;
																	vKey = vPart + ":\"" + vDestURL + "\"";
																	UpdateListData(ExternalURLList, vKey, new Long(1));																	
																}											
															}
														}
													}													
												}
											}
										} else {
											tempString = tempString.substring(bgIndex + 7);			
										}
										bgIndex = tempString.indexOf(ptSearch) ;
									}
								}
							
								String sPattern = new String("");
								Long lCount = null;
								String sInternalList = new String("");
								String sExternalList = new String("");
								
								if (vTargetURI.length() > 0) {
									jsonString = vTargetURI + "\t" + vDate + "\t" + vContentType + "\t" + vContentLength ;
									vInternalURL = 0;
									if (vInternalURL>0)
									{
										for (Enumeration e = InternalURLList.keys(); e.hasMoreElements();) {
											sPattern = (String) e.nextElement();
											lCount = (Long) InternalURLList.get(sPattern);
											if (sInternalList.length() == 0)
											{
												sInternalList = sPattern + ":\"" + lCount+"\"";
											} else {
												sInternalList = sInternalList + "," + sPattern + ":\"" + lCount+"\"";
											}		
										}
										jsonString = jsonString + "\t" + Integer.toString(vInternalURL) + "\t{" + sInternalList + "}";
									} else {
										jsonString = jsonString + "\t" + Integer.toString(vInternalURL) + "\t{}";
									}
	
									if (vCount>0)
									{
										for (Enumeration e = ExternalURLList.keys(); e.hasMoreElements();) {
											sPattern = (String) e.nextElement();
											lCount = (Long) ExternalURLList.get(sPattern);
											if (sExternalList.length() == 0)
											{
												sExternalList = sPattern + ":\"" + lCount+"\"";
											} else {
												sExternalList = sExternalList + "," + sPattern + ":\"" + lCount+"\"";
											}
										}
										jsonString = jsonString + "\t" + Integer.toString(vCount) + "\t{" + sExternalList + "}";
									} else {
										jsonString = jsonString + "\t" + Integer.toString(vCount) + "\t{}";
									}


									jsonLine.set(jsonString);
									context.write(jsonLine, NullWritable.get());
									context.getCounter(Counters.TOTAL_RECORD).increment((vCount + vInternalURL));
								}
							} 
						}
					}


					if (json == 1) {
						//Json output verion
						ptSearch = new String("\"ARC-Header-Metadata\":");
						bgIndex = line.indexOf(ptSearch) ;
						edIndex = line.indexOf(",\"ARC-Header-Length\"", bgIndex + ptSearch.length());
						if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
							jsonString = "{\"ARC-Header-Metadata\":" + line.substring(bgIndex + ptSearch.length(),edIndex);						
						}

				
						ptSearch = new String("\"HTML-Metadata\":");
						bgIndex = line.indexOf(ptSearch) ;
						edIndex = line.indexOf("\"}],\"", bgIndex + ptSearch.length());
						if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
							jsonString = jsonString + ",\"HTML-Metadata\":" + line.substring(bgIndex + ptSearch.length(), edIndex+3) + "}}";
						} else {
							jsonString = jsonString + "}";
						}
				
									
						if (jsonString.length() > 0) {
							jsonString = jsonString  + "\n";
							jsonLine.set(jsonString);
							context.write(jsonLine, NullWritable.get());
							context.getCounter(Counters.TOTAL_RECORD).increment(1);
						}
					
					} 

					if (json == 2) {
						//Text output version
						jsonString = new String("");

						ptSearch = new String("Header-Metadata\":");
						bgIndex = line.indexOf(ptSearch) ;
						edIndex = line.indexOf("Header-Length\"", bgIndex + ptSearch.length());
						if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
							tempString = line.substring(bgIndex + ptSearch.length(),edIndex);		
						
							vTargetURI = new String("");
							ptSearch = new String("Target-URI\":\"");
							bgIndex = tempString.indexOf(ptSearch) ;
							edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
							if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
								vTargetURI = tempString.substring(bgIndex + ptSearch.length(), edIndex);
								vTargetURI = vTargetURI.toLowerCase();	

								bgIndex = vTargetURI.indexOf("https://");
								if (bgIndex > 0 ) {
									vTargetURI = vTargetURI.substring(bgIndex + 8);								
								} else { 
									bgIndex = vTargetURI.indexOf("http://");
									if ( bgIndex >= 0) {
										vTargetURI = vTargetURI.substring(bgIndex + 7);
									}
								}

								bgIndex = vTargetURI.indexOf("www");
								if (bgIndex >=0) {
									bgIndex = vTargetURI.indexOf(".");
									if (bgIndex >=0) {
										vTargetURI = vTargetURI.substring(bgIndex+1);
									} else {
										bgIndex = vTargetURI.indexOf("www");
										vTargetURI = vTargetURI.substring(bgIndex + 3);
									}
								}
							
	
								bgIndex = vTargetURI.indexOf("/");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}

								bgIndex = vTargetURI.indexOf(":");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}
							
							}
						
						
							if (!isIP(vTargetURI)) {

								ptSearch = new String("Date\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vDate = tempString.substring(bgIndex + ptSearch.length(), edIndex);	
									if (vDate.length() >= 8) {
										String vTD = vDate.replaceAll("-","");
										vDate = vTD.substring(0,8);
									}	
										
								}

								ptSearch = new String("\"Content-Type\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentType = tempString.substring(bgIndex + ptSearch.length(), edIndex);
									int vCheck_warc = vContentType.indexOf(";");
									if (vCheck_warc > 0 ) {
										vContentType = vContentType.substring(0, vCheck_warc);
									}							
								}

								ptSearch = new String("\"Content-Length\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentLength = tempString.substring(bgIndex + ptSearch.length(), edIndex);							
								}	
							
								jsonString = new String("");
								ptSearch = new String("\"HTML-Metadata\":");
								bgIndex = line.indexOf(ptSearch) ;
								edIndex = line.indexOf("\"}],\"", bgIndex + ptSearch.length());

								String listRef = new String("");

								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									tempString = line.substring(bgIndex + ptSearch.length(), edIndex);
									ptSearch = new String("\"url\":\"");
									bgIndex = tempString.indexOf(ptSearch) ;

									while (bgIndex >=0) {
										edIndex = tempString.indexOf("\"},", bgIndex + ptSearch.length());
										int bgIndex1 = -1;
										int edIndex1 = -1;
										if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
											vPart = tempString.substring(tempString.lastIndexOf(":", bgIndex)+1, bgIndex - 1);
											vPart = vPart.toLowerCase();
											if (vPart.indexOf("href")>0){
												vPart = "\"text/html\"";
											}else if (vPart.indexOf("img")>0){
												vPart = "\"image\"";
											} else if (vPart.indexOf("get")>0 || vPart.indexOf("post")>0 || vPart.indexOf("embed")>0 ){
												vPart = new String("\"app\"");
											}

											vDestURL = tempString.substring(bgIndex + ptSearch.length(), edIndex);
											vDestURL = vDestURL.toLowerCase();
											tempString = tempString.substring(edIndex);	
		
											bgIndex1 = vDestURL.indexOf("https://");
											if (bgIndex1 < 0 ) {
												bgIndex1 = vDestURL.indexOf("http://");
												if ( bgIndex1 >= 0) {
													vDestURL = vDestURL.substring(bgIndex1 + 7);
												}
											} else {
												vDestURL = vDestURL.substring(bgIndex1 + 8);
											}

											if ( bgIndex1 >= 0) {
									
												bgIndex1 = vDestURL.indexOf("www");
												if (bgIndex1 >=0) {
													bgIndex1 = vDestURL.indexOf(".");
													if (bgIndex1 >=0) {
														vDestURL = vDestURL.substring(bgIndex1+1);
													} else {
														bgIndex1 = vDestURL.indexOf("www");
														vDestURL = vDestURL.substring(bgIndex1 + 3);
													}
												}
												bgIndex1 = vDestURL.indexOf("/");
												if ( bgIndex1 >= 0) {			
													vDestURL = vDestURL.substring(0, bgIndex1);
												}

												bgIndex1 = vDestURL.indexOf(":");
												if ( bgIndex1 >= 0) {			
													vDestURL = vDestURL.substring(0, bgIndex1);
												}

												if (!isIP(vDestURL)) {
													if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0 || vDestURL.indexOf("/")>=0 || vDestURL.indexOf(".html")>=0|| vDestURL.indexOf("%")>=0|| 
														vDestURL.indexOf("?")>=0 || vDestURL.indexOf(".gif")>=0 || vDestURL.indexOf(".htm")>=0 || vDestURL.indexOf(".GIF")>=0 ||vDestURL.indexOf(".exe")>=0) {
														vInternalURL = vInternalURL + 1;
													} else {
														bgIndex1 = vDestURL.lastIndexOf(".");
														if ( bgIndex1 >= 0) {			
															edIndex1 = vDestURL.lastIndexOf(".", bgIndex1);
															String v1;
															String v2;
															if (edIndex1 >=0 && edIndex1 <  bgIndex1) {
																v1 = vDestURL.substring(edIndex1);
																if (vTargetURI.length() >= (vDestURL.length() - edIndex1)) {
																	v2 = vTargetURI.substring(edIndex1);
																} else {
																	v2 = vTargetURI;
																}
																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																} else {
																	vCount = vCount + 1;
																	if (listRef.length() > 0 ) {
																		if (listRef.indexOf(vDestURL) < 0) {
																			//listRef = listRef + ",\"" + vDestURL + "\"";
																			listRef = listRef + "," + vPart + ":\"" + vDestURL + "\"";
																		}
																	} else {
																		//listRef = "{\"" + vDestURL + "\"";
																		listRef = "{" + vPart + ":\"" + vDestURL + "\"";
																	}
																}

															} else {
																v1 = vDestURL;
																v2 = vTargetURI;

																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																} else {
																	vCount = vCount + 1;
																	if (listRef.length() > 0 ) {
																		if (listRef.indexOf(vDestURL) < 0) {
																			//listRef = listRef + ",\"" + vDestURL + "\"";
																			listRef = listRef + "," + vPart + ":\"" + vDestURL + "\"";
																		}
																	} else {
																		//listRef = "{\"" + vDestURL + "\"";
																		listRef = "{" + vPart + ":\"" + vDestURL + "\"";
																	}
																}											
															}
														}
													}
												}
											} else {	
												vInternalURL = vInternalURL + 1;
											}			
										} else {
											tempString = tempString.substring(bgIndex + 7);			
										}
										bgIndex = tempString.indexOf(ptSearch) ;
									}
								}
							
								if (vInternalURL > 0 ) {
									if (listRef.length() > 0 ) {
										jsonString = vTargetURI + "\t" + vDate + "\t" + Integer.toString(vInternalURL) + "\t" + vContentType + "\t" + vContentLength + "\t" + listRef + "}";
									} else {
										jsonString = vTargetURI + "\t" + vDate + "\t" + Integer.toString(vInternalURL) + "\t" + vContentType + "\t" + vContentLength + "\t{}";
									}
								} else {
									if (listRef.length() > 0 ) {
										jsonString = vTargetURI + "\t" + vDate + "\t0\t" + vContentType + "\t" + vContentLength + "\t" + listRef + "}";
									} else {
										jsonString = vTargetURI + "\t" + vDate + "\t0\t" + vContentType + "\t" + vContentLength + "\t{}";
									}
									
								}
							
								if (jsonString.length() > 0) {
									jsonLine.set(jsonString);
									context.write(jsonLine, NullWritable.get());
									context.getCounter(Counters.TOTAL_RECORD).increment((vCount + vInternalURL));
								}
							} 
						}
					}

					if (json == 3) {
						//Text output version
						jsonString = new String("");

						ptSearch = new String("\"ARC-Header-Metadata\":");
						bgIndex = line.indexOf(ptSearch) ;
						edIndex = line.indexOf(",\"ARC-Header-Length\"", bgIndex + ptSearch.length());
						if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
							tempString = line.substring(bgIndex + ptSearch.length(),edIndex);		
						
							vTargetURI = new String("");
							ptSearch = new String("\"Target-URI\":\"");
							bgIndex = tempString.indexOf(ptSearch) ;
							edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
							if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
								vTargetURI = tempString.substring(bgIndex + ptSearch.length(), edIndex);
								vTargetURI = vTargetURI.toLowerCase();	

								bgIndex = vTargetURI.indexOf("https://");
								if (bgIndex > 0 ) {
									vTargetURI = vTargetURI.substring(bgIndex + 8);								
								} else { 
									bgIndex = vTargetURI.indexOf("http://");
									if ( bgIndex >= 0) {
										vTargetURI = vTargetURI.substring(bgIndex + 7);
									}
								}

								bgIndex = vTargetURI.indexOf("www");
								if (bgIndex >=0) {
									bgIndex = vTargetURI.indexOf(".");
									if (bgIndex >=0) {
										vTargetURI = vTargetURI.substring(bgIndex+1);
									} else {
										bgIndex = vTargetURI.indexOf("www");
										vTargetURI = vTargetURI.substring(bgIndex + 3);
									}
								}
							
	
								bgIndex = vTargetURI.indexOf("/");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}

								bgIndex = vTargetURI.indexOf(":");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}
							
							}
						
						
							if (!isIP(vTargetURI)) {

								ptSearch = new String("\"Date\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vDate = tempString.substring(bgIndex + ptSearch.length(), edIndex);	
									if (vDate.length() >= 8) {
										vDate = vDate.substring(0,8);
									}	
										
								}

								ptSearch = new String("\"Content-Type\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentType = tempString.substring(bgIndex + ptSearch.length(), edIndex);							
								}

								ptSearch = new String("\"Content-Length\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentLength = tempString.substring(bgIndex + ptSearch.length(), edIndex);							
								}	
							
								jsonString = new String("");
								ptSearch = new String("\"HTML-Metadata\":");
								bgIndex = line.indexOf(ptSearch) ;
								edIndex = line.indexOf("\"}],\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									tempString = line.substring(bgIndex + ptSearch.length(), edIndex);
									ptSearch = new String("\"url\":\"");
									bgIndex = tempString.indexOf(ptSearch) ;

									while (bgIndex >=0) {
										edIndex = tempString.indexOf("\"},", bgIndex + ptSearch.length());
										int bgIndex1 = -1;
										int edIndex1 = -1;
										if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
											vDestURL = tempString.substring(bgIndex + ptSearch.length(), edIndex);
											vDestURL = vDestURL.toLowerCase();
											tempString = tempString.substring(edIndex);	
		
											bgIndex1 = vDestURL.indexOf("https://");
											if (bgIndex1 < 0 ) {
												bgIndex1 = vDestURL.indexOf("http://");
												if ( bgIndex1 >= 0) {
													vDestURL = vDestURL.substring(bgIndex1 + 7);
												}
											} else {
												vDestURL = vDestURL.substring(bgIndex1 + 8);
											}

											if ( bgIndex1 >= 0) {
									
												bgIndex1 = vDestURL.indexOf("www");
												if (bgIndex1 >=0) {
													bgIndex1 = vDestURL.indexOf(".");
													if (bgIndex1 >=0) {
														vDestURL = vDestURL.substring(bgIndex1+1);
													} else {
														bgIndex1 = vDestURL.indexOf("www");
														vDestURL = vDestURL.substring(bgIndex1 + 3);
													}
												}
												bgIndex1 = vDestURL.indexOf("/");
												if ( bgIndex1 >= 0) {			
													vDestURL = vDestURL.substring(0, bgIndex1);
												}

												bgIndex1 = vDestURL.indexOf(":");
												if ( bgIndex1 >= 0) {			
													vDestURL = vDestURL.substring(0, bgIndex1);
												}

												if (!isIP(vDestURL)) {
													if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0 || vDestURL.indexOf("/")>=0 || vDestURL.indexOf(".html")>=0|| vDestURL.indexOf("%")>=0|| 
														vDestURL.indexOf("?")>=0 || vDestURL.indexOf(".gif")>=0 || vDestURL.indexOf(".htm")>=0 || vDestURL.indexOf(".GIF")>=0 ||vDestURL.indexOf(".exe")>=0) {
														vInternalURL = vInternalURL + 1;
													} else {
														bgIndex1 = vDestURL.lastIndexOf(".");
														if ( bgIndex1 >= 0) {			
															edIndex1 = vDestURL.lastIndexOf(".", bgIndex1);
															String v1;
															String v2;
															if (edIndex1 >=0 && edIndex1 <  bgIndex1) {
																v1 = vDestURL.substring(edIndex1);
																if (vTargetURI.length() >= (vDestURL.length() - edIndex1)) {
																	v2 = vTargetURI.substring(edIndex1);
																} else {
																	v2 = vTargetURI;
																}
																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																} else {
																	vCount = vCount + 1;
																	if (jsonString.length() > 0 ) {
																		jsonString = jsonString + "\n" + vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	} else {
																		jsonString = vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	}
																}

															} else {
																v1 = vDestURL;
																v2 = vTargetURI;

																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																} else {
																	vCount = vCount + 1;
																	if (jsonString.length() > 0 ) {
																		jsonString = jsonString + "\n" + vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	} else {
																		jsonString = vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	}
																}											
															}
														}
													}
												}
											} else {	
												vInternalURL = vInternalURL + 1;
											}			
										} else {
											tempString = tempString.substring(bgIndex + 7);			
										}
										bgIndex = tempString.indexOf(ptSearch) ;
									}
								}
							
								if (vInternalURL > 0 ) {
									if (jsonString.length() > 0 ) {
										jsonString = jsonString + "\n" + vTargetURI + "\t" + vTargetURI + "\t" + vDate + "\t" + Integer.toString(vInternalURL) + "\t" + vContentType + "\t" + vContentLength;
									} else {
										jsonString = vTargetURI + "\t" + vTargetURI + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
									}
								} else {
									jsonString = vTargetURI + "\t" + vTargetURI + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
								}
							
								if (jsonString.length() > 0) {
									jsonLine.set(jsonString);
									context.write(jsonLine, NullWritable.get());
									context.getCounter(Counters.TOTAL_RECORD).increment((vCount + vInternalURL));
								}
							} 
						}
					} 
					if (json == 4) {
						//Text output version without analysing
						int refList = -1;
						jsonString = new String("");

						ptSearch = new String("\"ARC-Header-Metadata\":");
						bgIndex = line.indexOf(ptSearch) ;
						edIndex = line.indexOf(",\"ARC-Header-Length\"", bgIndex + ptSearch.length());
						if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
							tempString = line.substring(bgIndex + ptSearch.length(),edIndex);		
						
							vTargetURI = new String("");
							ptSearch = new String("\"Target-URI\":\"");
							bgIndex = tempString.indexOf(ptSearch) ;
							edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
							if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
								vTargetURI = tempString.substring(bgIndex + ptSearch.length(), edIndex);
								vTargetURI = vTargetURI.toLowerCase();	

								bgIndex = vTargetURI.indexOf("https://");
								if (bgIndex > 0 ) {
									vTargetURI = vTargetURI.substring(bgIndex + 8);								
								} else { 
									bgIndex = vTargetURI.indexOf("http://");
									if ( bgIndex >= 0) {
										vTargetURI = vTargetURI.substring(bgIndex + 7);
									}
								}

								bgIndex = vTargetURI.indexOf("www");
								if (bgIndex >=0) {
									bgIndex = vTargetURI.indexOf(".");
									if (bgIndex >=0) {
										vTargetURI = vTargetURI.substring(bgIndex+1);
									} else {
										bgIndex = vTargetURI.indexOf("www");
										vTargetURI = vTargetURI.substring(bgIndex + 3);
									}
								}
							
	
								bgIndex = vTargetURI.indexOf("/");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}

								bgIndex = vTargetURI.indexOf(":");
								if ( bgIndex >= 0) {			
									vTargetURI = vTargetURI.substring(0, bgIndex);
								}
							
							}
						
						
							if (!isIP(vTargetURI)) {

								ptSearch = new String("\"Date\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vDate = tempString.substring(bgIndex + ptSearch.length(), edIndex);	
									if (vDate.length() >= 8) {
										vDate = vDate.substring(0,8);
									}	
										
								}

								ptSearch = new String("\"Content-Type\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentType = tempString.substring(bgIndex + ptSearch.length(), edIndex);							
								}

								ptSearch = new String("\"Content-Length\":\"");
								bgIndex = tempString.indexOf(ptSearch) ;
								edIndex = tempString.indexOf("\",\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									vContentLength = tempString.substring(bgIndex + ptSearch.length(), edIndex);							
								}	
							
								jsonString = new String("");
								ptSearch = new String("\"HTML-Metadata\":");
								bgIndex = line.indexOf(ptSearch) ;
								edIndex = line.indexOf("\"}],\"", bgIndex + ptSearch.length());
								if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
									tempString = line.substring(bgIndex + ptSearch.length(), edIndex);
									ptSearch = new String("\"url\":\"");
									bgIndex = tempString.indexOf(ptSearch) ;
								
									while (bgIndex >=0) {
										edIndex = tempString.indexOf("\"},", bgIndex + ptSearch.length());
										//int bgIndex1 = -1;
										//int edIndex1 = -1;
										if ((bgIndex > 0) && (edIndex > bgIndex + ptSearch.length())) {
											vDestURL = tempString.substring(bgIndex + ptSearch.length(), edIndex);
											vDestURL = vDestURL.toLowerCase();
											tempString = tempString.substring(edIndex);	
											
											bgIndex1 = vDestURL.indexOf("https://");
											if (bgIndex1 < 0 ) {
												bgIndex1 = vDestURL.indexOf("http://");
												if ( bgIndex1 >= 0) {
													vDestURL = vDestURL.substring(bgIndex1 + 7);
												}
											} else {
												vDestURL = vDestURL.substring(bgIndex1 + 8);
											}

											if ( bgIndex1 >= 0) {
									
												bgIndex1 = vDestURL.indexOf("www");
												if (bgIndex1 >=0) {
													bgIndex1 = vDestURL.indexOf(".");
													if (bgIndex1 >=0) {
														vDestURL = vDestURL.substring(bgIndex1+1);
													} else {
														bgIndex1 = vDestURL.indexOf("www");
														vDestURL = vDestURL.substring(bgIndex1 + 3);
													}
												}
												bgIndex1 = vDestURL.indexOf("/");
												if ( bgIndex1 >= 0) {			
													vDestURL = vDestURL.substring(0, bgIndex1);
												}

												bgIndex1 = vDestURL.indexOf(":");
												if ( bgIndex1 >= 0) {			
													vDestURL = vDestURL.substring(0, bgIndex1);
												}

												if (!isIP(vDestURL)) {
													if (vDestURL.indexOf(vTargetURI)>=0 || vTargetURI.indexOf(vDestURL)>=0 || vDestURL.indexOf("/")>=0 || vDestURL.indexOf(".html")>=0|| vDestURL.indexOf("%")>=0|| 
														vDestURL.indexOf("?")>=0 || vDestURL.indexOf(".gif")>=0 || vDestURL.indexOf(".htm")>=0 || vDestURL.indexOf(".GIF")>=0 ||vDestURL.indexOf(".exe")>=0) {
														vInternalURL = vInternalURL + 1;
													} else {
														bgIndex1 = vDestURL.lastIndexOf(".");
														if ( bgIndex1 >= 0) {			
															edIndex1 = vDestURL.lastIndexOf(".", bgIndex1);
															String v1;
															String v2;
															if (edIndex1 >=0 && edIndex1 <  bgIndex1) {
																v1 = vDestURL.substring(edIndex1);
																if (vTargetURI.length() >= (vDestURL.length() - edIndex1)) {
																	v2 = vTargetURI.substring(edIndex1);
																} else {
																	v2 = vTargetURI;
																}
																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																} else {
																	vCount = vCount + 1;
																	if (jsonString.length() > 0 ) {
																		jsonString = jsonString + "\n" + vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	} else {
																		jsonString = vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	}
																}

															} else {
																v1 = vDestURL;
																v2 = vTargetURI;

																if (v1.equals(v2)) {
																	vInternalURL = vInternalURL + 1;
																} else {
																	vCount = vCount + 1;
																	if (jsonString.length() > 0 ) {
																		jsonString = jsonString + "\n" + vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	} else {
																		jsonString = vTargetURI + "\t" + vDestURL + "\t" + vDate + "\t1\t" + vContentType + "\t" + vContentLength;
																	}
																}											
															}
														}
													}
												}
											} else {	
												vInternalURL = vInternalURL + 1;
											} 			
											refList = 0;
											if (jsonString.length() > 0 ) {
												jsonString = jsonString + ",\"" + vDestURL + "\"" ;
											} else {
												jsonString = vTargetURI + "\t" + vDate + "\t" + vContentType + "\t" + vContentLength + "\t{\"" + vDestURL + "\"";
											}

										} else {
											tempString = tempString.substring(bgIndex + 7);			
										}
										bgIndex = tempString.indexOf(ptSearch) ;
									}
								}
							
								if (refList == 0 ) {
									if (jsonString.length() > 0 ) {
										jsonString = jsonString + "}";
									} 
								} 
							
								if (jsonString.length() > 0) {
									jsonLine.set(jsonString);
									context.write(jsonLine, NullWritable.get());
									context.getCounter(Counters.TOTAL_RECORD).increment((vCount + vInternalURL));
								}
							} 
						}


					}
				}
			} catch (Exception ex){
				context.getCounter(Counters.TOTAL_RECORD_ERROR).increment(1);
				//System.err.println("GENERIC_INPUT_ERROR_MESSAGE	" + key + "," + value); // also log the payoad which resulted in the exception
				//ex.printStackTrace();
			}
        }
    }

    public static class DistinctURLMap extends 
	Mapper<Object, Text, Text, NullWritable> {
		private Text outURL1 = new Text();
		private Text outURL2 = new Text();
		static enum Counters { TOTAL_URL}

		private String dbCaseInputData;
		private String dbCase;
		private String startingTime;
		private String endingTime;
		private Text inputTime = new Text();
		String newLine = System.getProperty( "line.separator" );
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokenizer = (value.toString().replaceAll("\\s+","\t")).split("\t");

            if (tokenizer.length > 3) {
                Configuration conf = context.getConfiguration();

				dbCase = conf.get("dbCase");
				dbCaseInputData = conf.get("dbCaseInputData");
                startingTime = conf.get("startingTime");
                endingTime = conf.get("endingTime");

                //startingTime = "1997-01-01";
                //endingTime = "1997-02-01";
				               
                switch (Integer.parseInt(dbCase)) {
					case 1: //Dash Board with the 1st question
						if (dbCaseInputData.equals("ALL")) {
							String url1 = tokenizer[0];
							String url2 = tokenizer[1];
							outURL1.set(url1);
							outURL2.set(url2);

						    context.write(outURL1, NullWritable.get());
						    context.write(outURL2, NullWritable.get());
							context.getCounter(Counters.TOTAL_URL).increment(2);
						} else {
							inputTime.set(tokenizer[2]);
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date inputDate = new Date();                                          
							Date startingDate = new Date();
							Date endingDate = new Date();

							try {
							      inputDate = sdf.parse(inputTime.toString());                                            
							      startingDate = sdf.parse(startingTime);
							      endingDate = sdf.parse(endingTime);
							} catch (ParseException e) {
							   e.printStackTrace();
							}

							if (inputDate.after(startingDate) && inputDate.before(endingDate)) {
								String url1 = tokenizer[0];
								String url2 = tokenizer[1];
								outURL1.set(url1);
								outURL2.set(url2);

							    context.write(outURL1, NullWritable.get());
							    context.write(outURL2, NullWritable.get());
								context.getCounter(Counters.TOTAL_URL).increment(2);
							} 
						}

						if (dbCaseInputData.equals("ALL")) {
							String url1 = tokenizer[0];
							String url2 = tokenizer[7];
							String vURL = new String("");

							outURL1.set(url1);
							context.write(outURL1, NullWritable.get());
							context.getCounter(Counters.TOTAL_URL).increment(1);

							int bgIndex = 0;
							int edIndex = 0;
							int vIncreaseCount = 0;
							
							if (url2.length()> 2)
							{
								bgIndex = url2.indexOf("\":\"");
							}

							while (bgIndex > 0)
							{
								edIndex =url2.indexOf("\":\"", bgIndex + 3);
								if (edIndex > 0) 
								{
									if (vURL.length() > 0 ) {
										vURL = vURL + newLine + url2.substring(bgIndex+3, edIndex);
									} else {
										vURL = url2.substring(bgIndex+3, edIndex);
									}
									edIndex =url2.indexOf("\",\"",edIndex + 3);
									if (edIndex > 0) {	
										bgIndex = url2.indexOf("\":\"", edIndex + 3);
									} else {
										bgIndex = -1;
									}
									vIncreaseCount++;
								} else {
									bgIndex = -1;
								}
							
							}
							outURL2.set(vURL);
							context.write(outURL2, NullWritable.get());
							context.getCounter(Counters.TOTAL_URL).increment(vIncreaseCount);
							
						} else {
							inputTime.set(tokenizer[2]);
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date inputDate = new Date();                                          
							Date startingDate = new Date();
							Date endingDate = new Date();

							try {
							      inputDate = sdf.parse(inputTime.toString());                                            
							      startingDate = sdf.parse(startingTime);
							      endingDate = sdf.parse(endingTime);
							} catch (ParseException e) {
							   e.printStackTrace();
							}

							if (inputDate.after(startingDate) && inputDate.before(endingDate)) {
								String url1 = tokenizer[0];
								String url2 = tokenizer[1];
								outURL1.set(url1);
								outURL2.set(url2);

							    context.write(outURL1, NullWritable.get());
							    context.write(outURL2, NullWritable.get());
								context.getCounter(Counters.TOTAL_URL).increment(2);
							} 
						}								
						break;
					case 2: //Dash Board with the 2nd question
						
						break;
				}
				
                                            
            }
		}
    }

	public static class MatchingURLListMap extends 
	Mapper<Object, Text, Text, NullWritable> {
		private Text outURL1 = new Text();
		private Text outURL2 = new Text();
		static enum Counters { TOTAL_MATCH}

		private String dbCaseInputData;
		private String dbCase;
		private String startingTime;
		private String endingTime;
		private String vSeedList;
		private Text inputTime = new Text();
		String newLine = System.getProperty( "line.separator" );
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokenizer = (value.toString().replaceAll("\\s+","\t")).split("\t");
			//String vSeedList = new String("https://www.diigo.com      https://www.freespeech.org       https://www.adbusters.org/       https://twitter.com/OccupyThestreet       https://twitter.com/anonops/status/99592101733466112       https://twitter.com/       https://mises.org/       https://itunes.apple.com/       https://crowdvoice.org/       https://audioboo.fm/       http://youngadultfinances.com/       http://www.zazzle.com/       http://www.zazzle.com.au       http://www.zazzle.ca/       http://www.yesmagazine.org/       http://www.workers.org       http://www.wired.com/       http://www.weeklystandard.com/       http://www.washingtonpost.com/       http://www.videorolls.com/       http://www.vice.com/       http://www.veteransforpeace.org/       http://www.vanityfair.com/       http://www.usnews.com/       http://www.urbandictionary.com/       http://www.underpaidgenius.com       http://www.uaw.org/       http://www.truthdig.com/       http://www.truth-out.org/       http://www.trunity.net/       http://www.torontolife.com/       http://www.time.com/       http://www.theroot.com/       http://www.thereformedbroker.com/       http://www.thenation.com/       http://www.theknowingway.com/       http://www.thehealersjournal.com/       http://www.thedailybeast.com       http://www.thebaffler.com/       http://www.theatlantic.com/       http://www.thearkansasproject.com/       http://www.termpaperwarehouse.com/       http://www.technologyreview.com/       http://www.streetartutopia.com       http://www.statisticbrain.com/       http://www.staresattheworld.com/       http://www.spreadshirt.com/       http://www.spinner.com/       http://www.smartmoney.com/       http://www.slideshare.net/       http://www.slate.com/       http://www.slate.com/       http://www.shortnews.com/       http://www.sfbg.com/       http://www.servicespace.org/       http://www.searchquotes.com/       http://www.salon.com/       http://www.rushkoff.com/       http://www.rpvnetwork.org/       http://www.rosalux-nyc.org/       http://www.rollingstone.com/       http://www.reuters.com/       http://www.reddit.com/       http://www.redbubble.com/       http://www.reallypolitical.com/       http://www.realitysandwich.com/       http://www.rawstory.com       http://www.racialicious.com/       http://www.publicpolicypolling.com/       http://www.politifact.com/       http://www.politico.com/       http://www.pbs.org/       http://www.patriotactionnetwork.com/       http://www.parentsforoccupywallst.com/       http://www.orbooks.com/       http://www.occupywallstwest.org/       http://www.occupytogether.org/       http://www.occupymynews.com/       http://www.occupylv.org/       http://www.nytimes.com       http://www.nypost.com/       http://www.nydailynews.com       http://www.nycga.net/       http://www.npr.org/       http://www.newyorker.com       http://www.newrepublic.com       http://www.nationalreview.com/       http://www.naomiklein.org/       http://www.nakedcapitalism.com/       http://www.myspace.com/       http://www.motherjones.com/       http://www.mothering.com/       http://www.meetup.com/       http://www.mediaite.com/       http://www.lookhuman.com/       http://www.livestream.com/       http://www.liveleak.com/       http://www.linkedin.com/       http://www.legitgov.org/       http://www.jamendo.com/       http://www.iwallerstein.com/       http://www.infowars.com/       http://www.infobarrel.com       http://www.inflexwetrust.com       http://www.huffingtonpost.com/       http://www.hollywoodreporter.com/       http://www.highsnobiety.com       http://www.guardian.co.uk/       http://www.goldmansachs666.com/       http://www.globalpost.com/       http://www.glennbeck.com/       http://www.funnyordie.com/       http://www.foxnews.com/       http://www.foreignaffairs.com/       http://www.forbes.com/       http://www.flickr.com/       http://www.firstgameworld.com/       http://www.fastcompany.com/       http://www.facebook.com/       http://www.examiner.com/       http://www.etsy.com/       http://www.esletc.com/       http://www.entrepreneur.com/       http://www.egs.edu/       http://www.ebay.com/       http://www.dreamstime.com/       http://www.docstoc.com/       http://www.discoverthenetworks.org/       http://www.democracynow.org/       http://www.deathandtaxesmag.com/       http://www.dccc.org       http://www.daveramsey.com/       http://www.dailykos.com/       http://www.dailyfinance.com/       http://www.cta.org/       http://www.csmonitor.com/       http://www.crainsnewyork.com       http://www.cracked.com       http://www.conservapedia.com/       http://www.cnn.com/       http://www.cnbc.com/       http://www.cduniverse.com/       http://www.cbsnews.com/       http://www.causes.com/       http://www.care2.com/       http://www.cafepress.com/       http://www.buzzfeed.com/       http://www.businessweek.com/       http://www.businessinsider.com/       http://www.breakingcopy.com/       http://www.boston.com/       http://www.bloomberg.com/       http://www.bigstockphoto.com/       http://www.benjerry.com/       http://www.behance.net/       http://www.avclub.com//       http://www.artflakes.com/       http://www.amazon.co.uk/       http://www.alternet.org/       http://www.aljazeera.com/       http://www.adbusters.org/       http://www.academia.edu       http://www.99percentfilm.com/       http://world.time.com/       http://wearethe99percent.tumblr.com/       http://wallstreetjobreport.com/       http://wagingnonviolence.org       http://voices.yahoo.com/       http://voiceofdetroit.net/       http://visualeconomics.creditloan.com       http://videosift.com/       http://usnews.nbcnews.com/       http://usahitman.com/       http://urbantimes.co/       http://unitednature.wordpress.com/       http://uk.news.yahoo.com       http://twitpic.com/       http://truththeory.com/       http://truthandtraditionsparty.org/       http://truth-out.org/       http://townhall.com/       http://townhall.com       http://times-up.org/       http://thisishell.net/       http://thirdbranch.crooksandliars.com/       http://thinkprogress.org       http://theweek.com       http://thephilandrist.wordpress.com       http://thenewcivilrightsmovement.com/       http://thehpalliance.org/       http://theconservativemonster.com/       http://technorati.com/       http://technoccult.net/       http://techland.time.com/       http://tcavey.blogspot.com       http://takingnote.blogs.nytimes.com/       http://takethesquare.net/       http://sweetlibertine.com/       http://swarmcreativity.tumblr.com       http://swampland.time.com/       http://strikedebt.org/       http://stream.aljazeera.com/       http://stpeteforpeace.org       http://soundcloud.com/       http://soundcloud.com/       http://socialmediaclub.org       http://shiftshaper.org/       http://scaredmonkeys.com/       http://sb.christogenea.org/       http://savedhistory.org       http://savedhistory.net/       http://rt.com       http://rendezvous.blogs.nytimes.com/       http://reason.com/       http://rainbowcamouflage.wordpress.com       http://prospect.org       http://pressabout.us       http://politicalhumor.about.com       http://politicalhumor.about.com       http://pinterest.com/       http://peterschiffchannel.blogspot.com       http://peopleslibrary.wordpress.com/       http://papers.ssrn.com/       http://outfront.blogs.cnn.com       http://ouishare.net       http://orgtheory.wordpress.com/       http://opinionator.blogs.nytimes.com/       http://online.wsj.com/       http://occupywriters.com/       http://occupywallstreet.tumblr.com/       http://occupywallstreet.newsvine.com/       http://occupywallstreet.net/       http://occupywallstreet.com/       http://occupywallst.org/       http://occupywallst.org/       http://occupyvideos.org/       http://occupystreams.org/       http://occupyseattle.org/       http://occupyposters.tumblr.com/       http://occupylosangeles.org/       http://occupyeducated.org/       http://occupydenver.org/       http://occupydallas.org/       http://occupyaustin.org/       http://occupyamerica.crooksandliars.com/       http://occupy-together.blogspot.com/       http://occupiedwallstjournal.com/       http://occupiedmedia.us/       http://observer.com/       http://observer.com/       http://nymag.com       http://news.yahoo.com/       http://nemo235.wordpress.com/       http://naughtthought.wordpress.com/       http://moonbattery.com/       http://money.cnn.com/       http://merih-news.com/       http://melaniehamlett.com/       http://mediadecoder.blogs.nytimes.com/       http://mathbabe.org/       http://mashable.com/       http://marchone.com.au/       http://map.occupy.net/       http://management.fortune.cnn.com/       http://m.free-press-release.com/       http://live.nydailynews.com       http://knowyourmeme.com/       http://jess3.com/       http://ireport.cnn.com/       http://io9.com       http://interoccupy.net/       http://inagist.com       http://ifanboy.com       http://holycuteness.com       http://gulagbound.com/       http://gothamist.com/       http://gawker.com/       http://gawker.com/       http://gawker.com/5850054       http://gatesofpower.com       http://fundly.com/       http://front.moveon.org/       http://freefrombroke.com       http://forum.randirhodes.com       http://fivethirtyeight.blogs.nytimes.com       http://firedoglake.com/       http://fineartamerica.com       http://failblog.cheezburger.com/       http://europebusines.blogspot.com/       http://ethicsalarms.com/       http://endoftheamericandream.com/       http://en.wikipedia.org/       http://en-gb.facebook.com       http://edition.cnn.com       http://edition.cnn.com/       http://dprogram.net/       http://doctorbulldog.wordpress.com/category/occupy-wallstreet/       http://dissenter.firedoglake.com/       http://dealbreaker.com/       http://darkroom.baltimoresun.com/       http://dailycaller.com/       http://current.com/       http://crooksandliars.com/       http://criticaled.wordpress.com       http://counterpsyops.com       http://concreteloop.com/       http://chime.in       http://business.time.com       http://brajeshwar.com/       http://bobchapman.blogspot.com/       http://blogs.wsj.com/       http://blogs.villagevoice.com/       http://blogs.hbr.org/       http://bloggers.com/       http://blog.tonyblei.com       http://blog.thezeitgeistmovement.com/       http://blog.heritage.org/       http://blog.giveemthis.com       http://blog.flickr.net/       http://blackagendareport.com       http://atlasshrugs2000.typepad.com       http://artsbeat.blogs.nytimes.com/       http://articles.washingtonpost.com       http://articles.marketwatch.com       http://articles.latimes.com       http://article.wn.com       http://aroccupywallstreet.wordpress.com/       http://appfinder.lisisoft.com/       http://apocalypse-how.com/       http://anarchistnews.org/       http://americablog.com       http://act.credoaction.com/       http://abcnews.go.com/       http://500px.com       http://2012election.procon.org/");
            
			int vIncreaseCount = 0;
			if (tokenizer.length > 3) {
                Configuration conf = context.getConfiguration();

				dbCase = conf.get("dbCase");
				dbCaseInputData = conf.get("dbCaseInputData");
                startingTime = conf.get("startingTime");
                endingTime = conf.get("endingTime");
				vSeedList = conf.get("seedList");
				vSeedList = vSeedList.toLowerCase();

                //startingTime = "1997-01-01";
                //endingTime = "1997-02-01";

				if (dbCaseInputData.equals("ALL")) {
					String url1 = tokenizer[0];
					String dt1 = tokenizer[1];
					String url2 = tokenizer[7];
					String vURL = new String("");
					String vType = new String("");
					String vRef = new String("");
					int idTemp = 0;
					if (url1.length() > 0)
					{
						if (vSeedList.indexOf(url1) >= 0)
						{
							
							int bgIndex = 0;
							int edIndex = 0;
							vIncreaseCount = 0;
							String vURL2Finding = new String("");
							int edTemp = 0;
							if (url2.length()> 2)
							{
								bgIndex = url2.indexOf("\":\"");
							}

							while (bgIndex > 0)
							{
								edIndex =url2.indexOf("\":\"", bgIndex + 3);
								if (edIndex > 0) 
								{
									vURL2Finding = url2.substring(bgIndex+3, edIndex);

									if (url2.lastIndexOf("\",\"", bgIndex)>0)
									{
										vType = url2.substring(url2.lastIndexOf("\",\"", bgIndex)+3, bgIndex );
									} else {
										vType = url2.substring(2, bgIndex );
									}

									
									edTemp =url2.indexOf("\",\"", edIndex + 2);
									if (edTemp > 0)
									{
										vRef = url2.substring(edIndex +3, edTemp);
									} else
									{
										vRef = url2.substring(edIndex +3, url2.length() - 3);
									}
									
									vIncreaseCount++;

									if (vURL.length() > 0 ) {
										vURL = vURL + newLine + url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
									} else {
										vURL =  url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
									}
									
									edIndex =url2.indexOf("\",\"",edIndex + 3);
									if (edIndex > 0) {	
										bgIndex = url2.indexOf("\":\"", edIndex + 3);
									} else {
										bgIndex = -1;
									}
									
								} else {
									bgIndex = -1;
								}
							
							}

							if (vURL.length() > 0)
							{
								outURL2.set(vURL);
								context.write(outURL2, NullWritable.get());
								context.getCounter(Counters.TOTAL_MATCH).increment(vIncreaseCount);
							}		


						} else {
							
							int bgIndex = 0;
							int edIndex = 0;
							vIncreaseCount = 0;
							String vURL2Finding = new String("");
							int edTemp = 0;
							if (url2.length()> 2)
							{
								bgIndex = url2.indexOf("\":\"");
							}

							while (bgIndex > 0)
							{
								edIndex =url2.indexOf("\":\"", bgIndex + 3);
								if (edIndex > 0) 
								{
									vURL2Finding = url2.substring(bgIndex+3, edIndex);
									if (vSeedList.indexOf(vURL2Finding) >= 0)
									{
										if (url2.lastIndexOf("\",\"", bgIndex)>0)
										{
											vType = url2.substring(url2.lastIndexOf("\",\"", bgIndex)+3, bgIndex );
										} else {
											vType = url2.substring(2, bgIndex );
										}
										edTemp =url2.indexOf("\",\"", edIndex + 2);
										if (edTemp > 0)
										{
											vRef = url2.substring(edIndex +3, edTemp);
										} else
										{
											vRef = url2.substring(edIndex +3, url2.length() - 3);
										}
										
										vIncreaseCount++;

										if (vURL.length() > 0 ) {
											vURL = vURL + newLine + url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
										} else {
											vURL =  url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
										}
										
									}
									
									edIndex =url2.indexOf("\",\"",edIndex + 3);
									if (edIndex > 0) {	
										bgIndex = url2.indexOf("\":\"", edIndex + 3);
									} else {
										bgIndex = -1;
									}
									
								} else {
									bgIndex = -1;
								}
							
							}

							if (vURL.length() > 0)
							{
								outURL2.set(vURL);
								context.write(outURL2, NullWritable.get());
								context.getCounter(Counters.TOTAL_MATCH).increment(vIncreaseCount);
							}
						}
					}
					
				} else if (dbCaseInputData.equals("BM")) {
					//Both Match
					String url1 = tokenizer[0];
					String dt1 = tokenizer[1];
					String url2 = tokenizer[7];
					String vURL = new String("");
					String vType = new String("");
					String vRef = new String("");
					int idTemp = 0;
					if (url1.length() > 0)
					{
						if (vSeedList.indexOf(url1) >= 0)
						{
							int bgIndex = 0;
							int edIndex = 0;
							vIncreaseCount = 0;
							String vURL2Finding = new String("");
							int edTemp = 0;
							if (url2.length()> 2)
							{
								bgIndex = url2.indexOf("\":\"");
							}

							while (bgIndex > 0)
							{
								edIndex =url2.indexOf("\":\"", bgIndex + 3);
								if (edIndex > 0) 
								{
									vURL2Finding = url2.substring(bgIndex+3, edIndex);
									if (vSeedList.indexOf(vURL2Finding) >= 0)
									{
										if (url2.lastIndexOf("\",\"", bgIndex)>0)
										{
											vType = url2.substring(url2.lastIndexOf("\",\"", bgIndex)+3, bgIndex );
										} else {
											vType = url2.substring(2, bgIndex );
										}
										edTemp =url2.indexOf("\",\"", edIndex + 2);
										if (edTemp > 0)
										{
											vRef = url2.substring(edIndex +3, edTemp);
										} else
										{
											vRef = url2.substring(edIndex +3, url2.length() - 3);
										}
										
										vIncreaseCount++;

										if (vURL.length() > 0 ) {
											vURL = vURL + newLine + url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
										} else {
											vURL =  url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
										}
										
									}
									
									edIndex =url2.indexOf("\",\"",edIndex + 3);
									if (edIndex > 0) {	
										bgIndex = url2.indexOf("\":\"", edIndex + 3);
									} else {
										bgIndex = -1;
									}
									
								} else {
									bgIndex = -1;
								}
							
							}
						}
					
						if (vURL.length() > 0)
						{
							outURL2.set(vURL);
							context.write(outURL2, NullWritable.get());
							context.getCounter(Counters.TOTAL_MATCH).increment(vIncreaseCount);
						}		
					}
				} else if (dbCaseInputData.equals("DR")) {
					//Date Time Range
					
				} else if (dbCaseInputData.equals("MS")) {
					//Mapping Seperated Row
					String url1 = tokenizer[0];
					String dt1 = tokenizer[1];
					String url2 = tokenizer[7];
					String vURL = new String("");
					String vType = new String("");
					String vRef = new String("");
					int idTemp = 0;
					if (url1.length() > 0)
					{
						int bgIndex = 0;
						int edIndex = 0;
						vIncreaseCount = 0;
						String vURL2Finding = new String("");
						int edTemp = 0;
						if (url2.length()> 2)
						{
							bgIndex = url2.indexOf("\":\"");
						}

						while (bgIndex > 0)
						{
							edIndex =url2.indexOf("\":\"", bgIndex + 3);
							if (edIndex > 0) 
							{
								vURL2Finding = url2.substring(bgIndex+3, edIndex);

								if (url2.lastIndexOf("\",\"", bgIndex)>0)
								{
									vType = url2.substring(url2.lastIndexOf("\",\"", bgIndex)+3, bgIndex );
								} else {
									vType = url2.substring(2, bgIndex );
								}

								
								edTemp =url2.indexOf("\",\"", edIndex + 2);
								if (edTemp > 0)
								{
									vRef = url2.substring(edIndex +3, edTemp);
								} else
								{
									vRef = url2.substring(edIndex +3, url2.length() - 3);
								}
								
								vIncreaseCount++;

								if (vURL.length() > 0 ) {
									vURL = vURL + newLine + url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
								} else {
									vURL =  url1 + "\t" + vURL2Finding   + "\t" + dt1 + "\t" + vRef + "\t" + vType;
								}
								
								edIndex =url2.indexOf("\",\"",edIndex + 3);
								if (edIndex > 0) {	
									bgIndex = url2.indexOf("\":\"", edIndex + 3);
								} else {
									bgIndex = -1;
								}
								
							} else {
								bgIndex = -1;
							}
						
						}

						if (vURL.length() > 0)
						{
							outURL2.set(vURL);
							context.write(outURL2, NullWritable.get());
							context.getCounter(Counters.TOTAL_MATCH).increment(vIncreaseCount);
						}		
					}
				}


				//--------------------
                                       
            }
		}
    }

	

	public static class MapTLD extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text domain = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
			String dm = new String();
			int lastIndex = line.lastIndexOf('.');
			if (lastIndex >=0 && lastIndex < line.length()) {
				dm = line.substring(lastIndex+1);
				lastIndex = dm.indexOf('/',0);
				if (lastIndex >= 0) {
					dm = dm.substring(0, lastIndex);
				} else {
					lastIndex = dm.indexOf('#',0);
					if (lastIndex >= 0) {
						dm = dm.substring(0, lastIndex);
					} else {
						lastIndex = dm.indexOf('?',0);
						if (lastIndex >= 0) {
							dm = dm.substring(0, lastIndex);
						} else {
							lastIndex = dm.indexOf(';',0);
							if (lastIndex >= 0) {
								dm = dm.substring(0, lastIndex);
							} else {
								lastIndex = dm.indexOf(':',0);
								if (lastIndex >= 0) {
									dm = dm.substring(0, lastIndex);
								} else {
									lastIndex = dm.indexOf('&',0);
									if (lastIndex >= 0) {
										dm = dm.substring(0, lastIndex);
									} else {
										lastIndex = dm.indexOf('>',0);
										if (lastIndex >= 0) {
											dm = dm.substring(0, lastIndex);
										} else {
											lastIndex = dm.indexOf('+',0);
											if (lastIndex >= 0) {
												dm = dm.substring(0, lastIndex);
											} else {
												lastIndex = dm.indexOf('-',0);
												if (lastIndex >= 0) {
													dm = dm.substring(0, lastIndex);
												} else {
													lastIndex = dm.indexOf('%',0);
													if (lastIndex >= 0) {
														dm = dm.substring(0, lastIndex);
													} else {
														lastIndex = dm.indexOf('<',0);
														if (lastIndex >= 0) {
															dm = dm.substring(0, lastIndex);
														} else {
															
																//feature cases
															
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			if (dm.length() > 0 ) {
				domain.set(dm.toLowerCase());
				context.write(domain, one);
			}
        }
    }

	public static class MapCountingContentType extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text contentType = new Text();

		private String dbCaseInputData;
		private String dbCase;
		private String startingTime;
		private String endingTime;
		private Text inputTime = new Text();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokenizer = (value.toString().replaceAll("\\s+","\t")).split("\t");
			String sCT = new String();

            if (tokenizer.length >= 6) {
				Configuration conf = context.getConfiguration();

				dbCase = conf.get("dbCase");
				dbCaseInputData = conf.get("dbCaseInputData");
	            startingTime = conf.get("startingTime");
	            endingTime = conf.get("endingTime");

        		if (dbCaseInputData.equals("ALL")) {
					sCT = tokenizer[5];
					sCT = sCT.toLowerCase();
					if (sCT.length() > 0 ) {
						int lastIndex = sCT.indexOf("html");
						if (lastIndex >=0) {
							contentType.set("html");
							context.write(contentType, one);
						} else {
							lastIndex = sCT.indexOf("application");
							if (lastIndex >=0) {
								contentType.set("application");
								context.write(contentType, one);
							}  else {
								lastIndex = sCT.indexOf("plain");
								if (lastIndex >=0) {
									contentType.set("plain");
									context.write(contentType, one);
								}  else {
									
										contentType.set(sCT);
										context.write(contentType, one);
									
								}
							}
						}

						
					}
				} else {
					inputTime.set(tokenizer[2]);
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date inputDate = new Date();                                          
					Date startingDate = new Date();
					Date endingDate = new Date();

					try {
						  inputDate = sdf.parse(inputTime.toString());                                            
						  startingDate = sdf.parse(startingTime);
						  endingDate = sdf.parse(endingTime);
					} catch (ParseException e) {
					   e.printStackTrace();
					}

					if (inputDate.after(startingDate) && inputDate.before(endingDate)) {
						sCT = tokenizer[5];
						if (sCT.length() > 0 ) {
							contentType.set(sCT.toLowerCase());
							context.write(contentType, one);
						}
					}

				} 
			}
        }

    }


	public static class PreProcessingDataReduce extends 
	Reducer<Text, NullWritable, Text, NullWritable> {
		static enum Counters { DIST_RECORD}
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
			context.getCounter(Counters.DIST_RECORD).increment(1);
			//counter = context.getCounter(Counters.DIST_URL).getValue();
		}
    }


    public static class DistinctURLReduce extends 
	Reducer<Text, NullWritable, Text, NullWritable> {
		static enum Counters { DIST_URL}
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
			context.getCounter(Counters.DIST_URL).increment(1);
			//counter = context.getCounter(Counters.DIST_URL).getValue();
		}
    }

	public static class MatchingURLListReduce extends 
	Reducer<Text, NullWritable, Text, NullWritable> {
		static enum Counters { MATCH_DIST_URL_LIST}
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			if (key != null)
			{
				context.write(key, NullWritable.get());
				context.getCounter(Counters.MATCH_DIST_URL_LIST).increment(1);
			}			
		}
    }

	//public static class ReduceTLD extends Reducer<Text, IntWritable, Text, IntWritable> {
	public static class ReduceTLD extends Reducer<Text, IntWritable, Text, NullWritable> {
		static enum Counters { TOTAL_TLD}
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
			String kV = new String();
			Text jsonKey = new Text();
            for (IntWritable val : values) {
                sum += val.get();
            }
			context.getCounter(Counters.TOTAL_TLD).increment(1);
            //context.write(key, new IntWritable(sum));
			//kV = "{{\"Domain\":\"" + key.toString() + "\"},{\"CountingURL\":\"" + Integer.toString(sum) + "\"}}";
			kV = key.toString() + "\t" + Integer.toString(sum);
			jsonKey.set(kV);
			context.write(jsonKey, NullWritable.get());
        }
    }


	//public static class Reduce Counting the Content Type extends Reducer<Text, IntWritable, Text, IntWritable> {
	public static class ReduceCountingContentType extends Reducer<Text, IntWritable, Text, NullWritable> {
		static enum Counters { TOTAL_CTT}
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
			String kV = new String();
			Text jsonKey = new Text();
            for (IntWritable val : values) {
                sum += val.get();
            }
			context.getCounter(Counters.TOTAL_CTT).increment(1);
            //context.write(key, new IntWritable(sum));
			//kV = "{{\"Domain\":\"" + key.toString() + "\"},{\"CountingURL\":\"" + Integer.toString(sum) + "\"}}";
			kV = key.toString() + "\t" + Integer.toString(sum);
			jsonKey.set(kV);
			context.write(jsonKey, NullWritable.get());
        }
    }



	public static void main(String[] args) throws Exception {
		System.err.println("SYSTEM IS STARTING\n");
		switch (Integer.parseInt(args[2])) {
			case 1: //Processing DashBoard with the 1st question
				//System.err.println("(Distinct URL)/(Total URL) = " + distinct_counter + "/" + total_counter);
				break;
			case 2: //Processing DashBoard with the 2nd question
				//System.err.println("Total Top Level Domain = " + total_TLD);
				break;
			case 3: //Processing DashBoard with the 3nd question
				//System.err.println("Total Content Type = " + total_TLD);
				break;
			case 4: //Processing DashBoard with the Matching List question
				try {
					BufferedReader in = new BufferedReader(new FileReader(args[6]));
					String str;
					String SeedList = new String("");
					int n = 0;
					while ((str = in.readLine()) != null) {
						if (SeedList.length() > 0)
						{
							SeedList = SeedList + "\t" + str;
						} else {
							SeedList = str;
						}
						n++;
					}
					System.err.println("-------------------------------------------------------------------------------------" );
					System.err.println("SYSTEM HAS JUST LOADED " + n + " URLs FOM THE DATA SEED FILE " + args[6] + " DONE!" );
					System.err.println("-------------------------------------------------------------------------------------" );
					args[6] = SeedList;
					in.close();
				} catch (IOException e) {
				}
				break;
		}

		int ret = ToolRunner.run(new DashBoard(), args);
		if (ret==0) {
			System.err.println("SYSTEM RUNS SUCCESSFULLY!");

			switch (Integer.parseInt(args[2])) {
				case 1: //Processing DashBoard with the 1st question
					System.err.println("(Distinct URL)/(Total URL) = " + distinct_counter + "/" + total_counter);
					break;
				case 2: //Processing DashBoard with the 2nd question
					System.err.println("Total Top Level Domain = " + total_TLD);
					break;
				case 3: //Processing DashBoard with the 3nd question
					System.err.println("Total Content Type = " + total_TLD);
					break;
				case 4: //Processing DashBoard with the Matching List question
					System.err.println("Total Matching List URL = " + total_TML);
					break;
			}
		} else {
			System.err.println("SYSTEM IS FAIL!");
		}
		System.exit(ret);
    }
	
}
