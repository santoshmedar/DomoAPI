package com.domo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Properties;

public class ConfigService {

	public String fileName;
	public Properties prop;

	public ConfigService() {

		if (System.getProperty("prop") != null) {
		//	System.out.println(new Date().toString() + " Properites: Loading from java command line argument.");
			fileName = System.getProperty("prop");

		} else {
			this.fileName = "/config.properties";
		//	System.out.println(new Date().toString() + " Properties: Loading properties file from eclipse directory.");
		}

		this.load();

	}

	public Properties load(){
		/**
		 * called automatically from constructor, can be re-loaded if necessary.
		 * 
		 */
		this.prop = new Properties();

		InputStreamReader input = null;

		try {

			//input = new FileInputStream(this.fileName);
			input = new InputStreamReader(ConfigService.class.getResourceAsStream(this.fileName));
			prop.load(input);

		} catch (IOException ex) {
			System.out.println(Utils.getStackTrace(ex));
			System.exit(1);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					System.out.println(Utils.getStackTrace(e));
					System.exit(1);
				}
			}
		}

		return prop;
	}

	public Integer getPropertyAsInteger(String request) {
		int response = Integer.parseInt(this.prop.getProperty(request));
		return response;

	}

	public String getProperty(String request) {
		String response = this.prop.getProperty(request);
		return response;

	}

}