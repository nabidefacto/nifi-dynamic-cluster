package ru.itis.suc;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;

public class NifiLocalNode {
	String scheme = "http";
	String ip;
	int port = 8080;
	int protocolPort = 3030;
	
	/**
	 * path to nifi directory, 
	 * for example /etc/nifi/nifi-1.2.0/
	 */
	String nifiPath;
	
	public NifiLocalNode(String path){
		setNifiPath(path); 
		ip = initLocalIp();
	}
	/**
	 * @TODO add check directory is correct
	 * @param path
	 */
	public void setNifiPath(String path){
		if(!path.endsWith("/")){
			path = path + "/";
		}
		if(!path.startsWith("/")){
			path = "/" + path;
		}
		nifiPath = path;
	}
	public String getNifiPath(){
		return nifiPath;
	}
	public boolean isActive(){
		HostScanner scan = new HostScanner(port);
		scan.setConnectionTimeout(200); // let's do it fast, because local
		return scan.scanPort(getIp());
	}
	
	public String getStatus(){
		return "active";
	}
	
	public String getScheme(){
		return scheme;
	}
	
	public String getIp(){
		return ip;
	}
	
	public int getPort(){
		return port;
	} 

	public int getProtocolPort(){
		return protocolPort;
	} 
	
	public String initLocalIp(){
		String address = "";
		InetAddress lanIp = null;
		try {
			String ipAddress = null;
			Enumeration<NetworkInterface> net = null;
			net = NetworkInterface.getNetworkInterfaces();
			while (net.hasMoreElements()) {
				NetworkInterface element = net.nextElement();
				Enumeration<InetAddress> addresses = element.getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress ip = addresses.nextElement();
					if (ip instanceof Inet4Address) {
						if (ip.isSiteLocalAddress()) {
							ipAddress = ip.getHostAddress();
							lanIp = InetAddress.getByName(ipAddress);
						}
					}
				}
			}
			if (lanIp == null)
				return null; 
			address = lanIp.toString().replaceAll("^/+", ""); 
		} catch (UnknownHostException ex) {
			ex.printStackTrace();
		} catch (SocketException ex) {
			ex.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return address;
	}
	
	public void restart(){
		String cmd = nifiPath + "bin/nifi.sh restart";
		Process process; 
	    try {
	    	System.out.println("Restarting nifi with cmd [" + cmd + "]");
			// process = Runtime.getRuntime().exec(String.format("sh -c \"%s\" > /dev/null 2>&1 &", cmd));
			process = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", cmd});
			System.out.println("Process started");
			process.waitFor(); 
		} catch (InterruptedException e) {
			e.printStackTrace();		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String toString() {
		return getScheme() + "://" + getIp() + ":" + getPort(); // +  " Status:" + getStatus();
	}
	
	// update state/zk/state/myid
	public void setZkCluster(int myid, String clusterString, Properties zkProps) throws IOException {
		if(myid < 0){ // wrong zk id
			System.out.println("wrong zk id " + this);
			return;
		}
		// write myid for zk
		new File(nifiPath + "state/zookeeper/").mkdirs();
		PrintWriter writer = new PrintWriter(nifiPath + "state/zookeeper/myid", "UTF-8");
	    writer.println(myid);
	    writer.close();
	    
		// update properties
		String zkPropsPath = nifiPath + "conf/zookeeper.properties";
		PropertiesUpdater pu = new PropertiesUpdater(zkPropsPath);
		pu.updateProperties(zkProps);
		
		
		setStateManagementConnectString(clusterString);
		
	} 
	public void setStateManagementConnectString(String clusterConnectString) throws IOException{
		String fp = nifiPath + "conf/state-management.xml";
		String fixedPath = System.getProperty( "os.name" ).contains( "indow" ) ? fp.substring(1) : fp;

		Path path = Paths.get(fixedPath);
		Charset charset = StandardCharsets.UTF_8; 
		String content = new String(Files.readAllBytes(path), charset);
		content = content.replaceAll("<property name=\"Connect String\">+?</property>", 
				"<property name=\"Connect String\">"+clusterConnectString+ "</property>");
		content = content.replaceAll("<property name=\"Connect String\"></property>", 
				"<property name=\"Connect String\">"+clusterConnectString+ "</property>");
		Files.write(path, content.getBytes(charset));
	}
	/**
	 * join by creating cluster
	 * @param clusterConnectString
	 * @param flag
	 * @throws IOException
	 */
	public void joinStartCluster(String clusterConnectString) throws IOException
	{		
		PropertiesUpdater pu = new PropertiesUpdater(nifiPath + "conf/nifi.properties");
		Properties props = new Properties();
		props.setProperty("nifi.state.management.embedded.zookeeper.start", "true");
		props.setProperty("nifi.cluster.is.node", "true");	
		props.setProperty("nifi.cluster.node.address", getIp()); 
		props.setProperty("nifi.cluster.node.protocol.port", Integer.toString(getProtocolPort()));		
				
		props.setProperty("nifi.remote.input.host", getIp());
		props.setProperty("nifi.web.http.host", getIp()); 	
		props.setProperty("nifi.zookeeper.connect.string", clusterConnectString);
		pu.updateProperties(props);
	}
	
	public void joinCluster(String clusterConnectString) throws IOException{
		PropertiesUpdater pu = new PropertiesUpdater(nifiPath + "conf/nifi.properties");
		Properties props = new Properties();
		props.setProperty("nifi.state.management.embedded.zookeeper.start", "false");
		props.setProperty("nifi.cluster.is.node", "true");	
		props.setProperty("nifi.cluster.node.address", getIp()); 
		props.setProperty("nifi.cluster.node.protocol.port", Integer.toString(getProtocolPort()));		
				
		props.setProperty("nifi.remote.input.host", getIp());
		props.setProperty("nifi.web.http.host", getIp()); 	
		props.setProperty("nifi.zookeeper.connect.string", clusterConnectString);
		pu.updateProperties(props);
	}
	public boolean isInCluster(String clusterConnectString) { 
		return false;
	} 
}
