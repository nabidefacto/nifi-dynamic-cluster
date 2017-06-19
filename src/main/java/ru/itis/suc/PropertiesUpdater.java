package ru.itis.suc;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

public class PropertiesUpdater {
 
	String path;
	
	public PropertiesUpdater(){
		
	}
	
	public PropertiesUpdater(String filePath) {
		path = filePath;
	}
	
	public void updateProperties(Properties newProps) throws IOException{
		// load current properties
		FileInputStream in = new FileInputStream(path); 
		Properties props = new Properties();
		props.load(in); 
		in.close();
		
		// set new properties
		for (final String name : newProps.stringPropertyNames()) { 
			props.setProperty(name, (String) newProps.remove(name)); 
		}
		FileOutputStream out = new FileOutputStream(path);
		props.store(out, null);
		out.close(); 
	} 
}