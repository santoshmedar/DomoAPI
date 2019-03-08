package com.domo;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;

import com.domo.client.DomoClient;
import com.domo.client.Proxy;
import com.domo.client.auth.DomoApiTokenAuth;
import com.domo.client.manager.DatasetManager;
import com.domo.client.manager.UploadProgressListener;

public class UpsertData {

	//String domoURL;
	Map<String, String> domoApiTokens = new HashMap<>();
	String ProxyURL ;
	String ProxyUser ;
	String ProxyPass ;

	Map<String, String> dataSetIds = new HashMap<>();
	String loadDirectory ;
	String moveDirectory;
	int threadPoolSize = 10;
	boolean appendToDomoDataset;
	boolean filesAreGzipped = false;

	// not needed??
	int initialUploadDelay = 0;
	int uploadTimeout = 1440;
	int indexTimeout = 1440;

	static SimpleDateFormat dirDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat timeFormat = new SimpleDateFormat("HH-mm-ss");
	
	ConfigService config = new ConfigService();

	@SuppressWarnings("deprecation")
	void upload(String domoURL, String dataSetId) throws Exception {

		//ConfigService config = new ConfigService();

		//domoURL = config.prop.getProperty("domoURL");
		ProxyURL = config.prop.getProperty("proxyURL");

		domoApiTokens.put("nbcu-clientdashboard.domo.com",config.prop.getProperty("nbcu-clientdashboard.domo.com"));
		domoApiTokens.put("nbcuni.domo.com",config.prop.getProperty("nbcuni.domo.com"));
		//ProxyUser = config.prop.getProperty("proxyUser");
		//ProxyPass = config.prop.getProperty("proxyPass");

		//dataSetId = config.prop.getProperty("dataSetId");
		//loadDirectory = config.prop.getProperty("loadDirectory");

		//int uploadType = userdata.uploadTypeCombo.getSelectedIndex();

		//appendToDomoDataset = userdata.doUpsert.isSelected();

		DomoClient client = new DomoClient(new DomoApiTokenAuth(domoApiTokens.get(domoURL)), domoURL,
				new Proxy(ProxyURL));

		DatasetManager manager = new DatasetManager(client);

		if (appendToDomoDataset == true) {
			DomoSchema domo = new DomoSchema();
			domo.setkeyColumns(manager, dataSetId);
			//System.out.println("Upserting delta to dataset");
			//userdata.logTextArea.append("Upserting data to dataset\n");

		} else {
			//System.out.println("Overwriting dataset");
			//userdata.logTextArea.append("Overwriting dataset\n");
		}
		//userdata.logTextArea.setCaretPosition(userdata.logTextArea.getDocument().getLength());

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("EST"));

		SimpleDateFormat dateFormat2 = new SimpleDateFormat("HH:mm:ss");
		dateFormat2.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		long startTime = new Date().getTime();

		manager.uploadFromDirectory(dataSetId, new File(loadDirectory), appendToDomoDataset, filesAreGzipped,
				threadPoolSize, initialUploadDelay, uploadTimeout, indexTimeout, new UploadProgressListener() {

			public void printMsg(String msg) {
				System.out.println(new Date().toString() + " " + msg);
				//userdata.logTextArea.append(new Date().toString() + " " + msg+"\n");
				//userdata.logTextArea.setCaretPosition(userdata.logTextArea.getDocument().getLength());
			}

			@Override
			public void uploadStarted(String dataSetId, int totalParts) {
				long elapsedTime = new Date().getTime() - startTime;
				String timetaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Started upload for dataset %s. %d part files. Elapsed time: %S",
						dataSetId, totalParts, timetaken));
			}

			@Override
			public void uploadFinished(String dataSetId, int totalParts) {
				long elapsedTime = new Date().getTime() - startTime;
				String timetaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Finished Domo process for dataset %s. %d part files. Elapsed time: %s",
						dataSetId, totalParts, timetaken));
			}

			@Override
			public void uploadFailed(String dataSetId, String message) {
				printMsg(String.format("Data upload failed for dataset %s: %s", dataSetId, message));
			}

			@Override
			public void uploadPartStarted(String dataSetId, int filePart, File dataFile) {
				printMsg(String.format("Started file part %d upload for dataset %s", filePart, dataSetId));
			}

			public void uploadPartFinished(String dataSetId, int filePart, File dataFile) {
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Finished file part %d upload for dataset %s. Elapsed time: %s",
						filePart, dataSetId, timeTaken));
			}

			@Override

			public void uploadPartError(String dataSetId, int filePart, File dataFile, int statusCode,
					String statusMessage, int currentAttempt, int maxAttempts) {
				Throwable err = null;
				printMsg(String.format(" An error occurred uploading file part %d for dataset %s."
						+ "Current attempt %d, max attempts %d",
						filePart, dataSetId, currentAttempt, maxAttempts));
			}

			@Override
			public void indexStarted(String dataSetId) {
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Finished uploading data. Elapsed time: %s", timeTaken));
				printMsg(String.format("Started indexing for dataset: %s Elapsed time: %s", dataSetId, timeTaken));
			}

			/*
					@Override
					public void indexProgress(String dataSetId, int progress) {
						long elapsedTime = new Date().getTime() - startTime;
						String timetaken = dateFormat2.format(new Date(elapsedTime));
						printMsg(String.format("Current indexing progress for dataset %s - %d%,  Time taken is %s",
										dataSetId, progress, timetaken));
					}
			 */

			@Override
			public void indexWarning(String dataSetId, String warning) {
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Index progress warning for dataset %s. %s,  Time taken is %s",
						dataSetId, warning, timeTaken));
			}

			@Override
			public void indexError(String dataSetId, String error) {
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Index progress error for dataset %s. %s,  Time taken is %s",
						dataSetId, error, timeTaken));
			}
			
			public void indexFinished(String dataSetId) {
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Finished indexing for dataset %s,  Time taken is %s", dataSetId, timeTaken));
				printMsg(String.format("Finished indexing dataset %s", dataSetId));
			}

			@Override
			public void indexProgress(String dataSetId, String status, int percentComplete) {
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Index progress: %s %s %d. Elapsed time: %s", dataSetId, status, percentComplete, timeTaken));

			}

			@Override
			public void indexFinished(String dataSetId, String status) {
				// TODO Auto-generated method stub
				long elapsedTime = new Date().getTime() - startTime;
				String timeTaken = dateFormat2.format(new Date(elapsedTime));
				printMsg(String.format("Finished indexing for dataset %s,  Time taken is %s, Status is %s", dataSetId, timeTaken,status));
				}

			public void uploadPartFinished(String arg0, int arg1, File arg2, Long arg3) {
				// TODO Auto-generated method stub
				
			}

			
		});

		long elapsedTime = new Date().getTime() - startTime;

		System.out.println(new Date().toString() + " Total time elapsed: " + dateFormat2.format(new Date(elapsedTime)));
		//userdata.logTextArea.append("Total Time: " + dateFormat2.format(new Date(elapsedTime))+"\n");
		//userdata.logTextArea.setCaretPosition(userdata.logTextArea.getDocument().getLength());
		
		
		
	}

	public static void main(String[] args) {


		System.setProperty("https.protocols", "SSLv1.2");
		UpsertData uploadData = new UpsertData();

		Map<String, String> arguments;
		String line = System.getProperty("arguments");
		if(line != null) {

			arguments = new HashMap<>();

			String str[] = line.split(",");
			for(int i=0;i<str.length;i++){
				String arr[] = str[i].split("=");
				arguments.put(arr[0].trim(), arr[1].trim());
			}

			if(arguments.get("datasetIds")==null)
				{
				System.out.println(new Date().toString() + " datasetId is missing from the arguments list");
				System.exit(1);
				}
			else
			{
				
				String[] ids = arguments.get("datasetIds").split(" ");
				
				for (String instanceIds : ids)
					uploadData.dataSetIds.put(instanceIds.split(":")[0], instanceIds.split(":")[1]);
				
				if(arguments.get("loadDirectory")==null){
					System.out.println(new Date().toString() + " loadDirectory is missing from the arguments list");
					System.exit(1);
				}
				else
				{ 
					uploadData.loadDirectory = arguments.get("loadDirectory");
							
							if(arguments.get("upsert")==null){
									System.out.println(new Date().toString() + " Upsert/Replace is not set in the arguments list");
									System.exit(1);
								}
							else
							{
								uploadData.appendToDomoDataset = Boolean.parseBoolean(arguments.get("upsert"));
								
								if(arguments.get("moveDirectory")==null)
									{System.out.println(new Date().toString() + " moveDirectory is missing from the arguments list");
									System.exit(1);
									}
									else
									{ 

								uploadData.moveDirectory = arguments.get("moveDirectory");
								File srcDirectory = new File(uploadData.loadDirectory);
								
									if(new File(uploadData.loadDirectory).listFiles().length!=0)
								//if(new File(uploadData.loadDirectory+"/"+uploadData.config.prop.getProperty("doneFileName")).exists())
									{
										System.out.println(new Date().toString() + " New files found");
										try {
											
											for (Entry<String, String> instanceIds : uploadData.dataSetIds.entrySet())
												uploadData.upload(instanceIds.getKey(), instanceIds.getValue());
											
											File destDirectory = new File(uploadData.moveDirectory+"/"+dirDateFormat.format(new Date()));
											
											File[] srcFiles = srcDirectory.listFiles();
											
											for (File file : srcFiles)
												{
													File destFile = new File(destDirectory.getAbsolutePath()+"\\"+file.getName());
													if(destFile.exists())
														{
															destFile.delete();
															System.out.println(destFile.getAbsolutePath()+" file deleted since it alreday exists");
														}
													
													FileUtils.moveFileToDirectory(file, destDirectory, true);
												
												}
											
											System.out.println(new Date().toString() + " Files are successfully moved to "+destDirectory.getAbsolutePath());
											
										} catch (Exception e) {
											// TODO Auto-generated catch block
											System.out.println(Utils.getStackTrace(e));
											System.exit(1);
										}
									}
								else
									{System.out.println(new Date().toString() + " no new files found!");
									System.exit(2);
									}
								
							}
						}
				}
			}

		}
		else
			{System.out.println(new Date().toString() + " No Command Line Arguments found");
			System.exit(1);
			}


	}

}