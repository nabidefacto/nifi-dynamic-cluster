/**
 * Nifi cluster ensemble.
 * scan in local network for hosts 
 * and return cluster connect string
 * 
 * @author Nabi Defacto
 */
package ru.itis.suc;
 
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
 
public class NifiClusterEnsemble implements Runnable{
	
	private List<String> clusterIps; 
	private NifiLocalNode localNode;
	private int zkPort = 2181;
	private String zkServerPorts = "2888:3888";
	private int ensembleMax = 3;
	public NifiClusterEnsemble(NifiLocalNode node, List<String> ips){
		clusterIps = sortIps(ips);
		localNode = node; 
	}
	
	public void run(){
		try {
			start();
		} catch (IOException e) { 
			e.printStackTrace();
		}
	}
	private List<String> sortIps(List<String> ips){		
	    TreeSet set = new TreeSet(ips);
	    List<String> list = new ArrayList<String>(set);
	    List<String> listEnsemble = list.subList(0, ensembleMax);
        return listEnsemble; 
	}
	
	public void start() throws IOException{
		// Setting up a ZooKeeper Ensemble
		System.out.println(clusterIps);
		String clusterString = getConnectString();
		int zkId = getZkServerId(localNode.getIp());
		if(zkId > 0){
			localNode.setZkCluster(zkId, clusterString,  getZkServersProperties());
		}
		localNode.joinStartCluster(clusterString); // updates nifi.properties
		System.out.println("Restarting local node");
		localNode.restart();
	} 
	/**
	 * get ZK server id
	 * we should find it by sorting cluster ips and getting index
	 * 
	 * @return
	 */
	public int getZkServerId(String ip)
	{		
		return clusterIps.indexOf(ip) + 1;
	}
	
	/**
	 * returns properties with server.{i} cluster list
	 * @return
	 */
	public Properties getZkServersProperties(){
		Properties props = new Properties();
		Iterator<String> iter = clusterIps.iterator();
		int i = 0;
		while(iter.hasNext()) {
			i++;
			if(i > ensembleMax){
				break;
			}
			String ip = iter.next();
			props.setProperty("server."+i, ip + ":" + zkServerPorts);
		} 
		props.setProperty("initLimit", "5");
		props.setProperty("syncLimit", "2");
		return props;
	}
	
	/**
	 * zk cluster connection string for nifi.properties and state-management.xml
	 * 
	 * @return
	 */
	public String getConnectString(){
		Iterator<String> iter = clusterIps.iterator();
		StringBuilder connectString = new StringBuilder();

		while(iter.hasNext()) {
			String ip = iter.next();
			connectString.append(ip).append(":").append(zkPort).append(",");
		} 
		connectString.deleteCharAt(connectString.length() - 1);
		return connectString.toString();
	}
}
