package ch.usi.da.smr.misc;

import java.util.Properties;

public class ConfigurationFileManager {

	Properties configFile;
	
	public ConfigurationFileManager(){
		configFile = new java.util.Properties();			
	}
	
	public void openFile(String file) throws Exception{
		configFile.load(this.getClass().getClassLoader().getResourceAsStream(file));		
	}

	public String getProperty(String key){
		String value = this.configFile.getProperty(key);
		return value;
	}
	
}
