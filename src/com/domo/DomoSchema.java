package com.domo;

import java.lang.reflect.Array;
import java.util.List;

import com.domo.client.DomoClient;
import com.domo.client.Proxy;
import com.domo.client.auth.DomoApiTokenAuth;
import com.domo.client.exceptions.RequestFailedException;
import com.domo.client.manager.DatasetManager;
import com.domo.client.model.datasources.Schema;
import com.domo.client.model.datasources.Schema.Column;

public class DomoSchema {
	public void setkeyColumns(DatasetManager manager, String dataSetId) throws Exception {
		
		ConfigService config = new ConfigService();

		//String dataSetId = config.prop.getProperty("dataSetId");
		
		Schema schema = manager.getSchema(dataSetId);
		
		String[] keyColumns = config.prop.getProperty("keyColumns").split(",");
		
		for (int i = 0; i < keyColumns.length; i++) 
			schema.getColumn(keyColumns[i].trim()).setUpsertKey(true);
		
		manager.updateSchema(dataSetId, schema);

	}

	
/*	List<Column> getDatasetColumns(UiPanel userdata) throws RequestFailedException{
		
		
		String domoURL = userdata.config.prop.getProperty("domoURL");
		String ProxyURL = userdata.config.prop.getProperty("proxyURL");
		
		String domoApiToken = userdata.apiTokenFld.getText().trim();
		String ProxyUser = userdata.usernameFld.getText().trim();
		String ProxyPass = userdata.passwprdFld.getText().trim();
		
		String dataSetId = userdata.datasetIdFld.getText().trim();
		
		
		DomoClient client = new DomoClient(new DomoApiTokenAuth(domoApiToken), domoURL,
				new Proxy(ProxyURL, ProxyUser, ProxyPass));
		
		DatasetManager manager = new DatasetManager(client);
		Schema schema = manager.getSchema(dataSetId);
		
		return schema.getColumns();
	}*/
	
}