/**
 * Nifi cluster scanner.
 * scan in local network for hosts 
 * and return cluster connect string
 * 
 * @author Nabi Defacto
 */
package ru.itis.suc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class NifiClusterScanner {

	public int portScan = 8080;
	public boolean isSecure = false;
	public String clusterProtocol = "http";
	public int zookeeperPort = 2181;
	public HostScanner hostScanner;
	
	public NifiClusterScanner(HostScanner scanner){
		hostScanner = scanner; 
		portScan = scanner.getPort();
	}
	 
	public NifiClusterScanner(HostScanner scanner, String protocol){
		clusterProtocol = protocol; 
		portScan = scanner.getPort();
	}
	
	// @TODO replace with logger
	public void log(String str){
		System.out.println(str);
	} 
	
	/**
	 * Return cluster connect string
	 * @return
	 */
	public String findLocalCluster(){ 
		List<String> ipsList = hostScanner.scan();
		if(ipsList==null){
			return null;
		}
		System.out.println("[INFO] Found ips: " + ipsList);
		Iterator<String> ips = ipsList.iterator();
		while(ips.hasNext()) {
			String ip = ips.next();
			try{
				String clusterStr = getClusterCoordinatorByApi(ip);
				return clusterStr;
			}catch(ClientProtocolException e){
				log(e.getMessage());
			}catch(IOException io){
				log(io.getMessage());
			}
		}
		 
		return null;
	}
	
	/**
	 * 
	 * REST request to get cluster nodes information
	 * 
	 * @param ip
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public String getClusterCoordinatorByApi(String ip) throws ClientProtocolException, IOException{		
		int timeout = 2;
		RequestConfig config = RequestConfig.custom()
		  .setConnectTimeout(timeout * 1000)
		  .setConnectionRequestTimeout(timeout * 1000)
		  .setSocketTimeout(timeout * 1000).build();
		CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
		
        HttpGet request = new HttpGet(clusterProtocol + "://" + ip + ":"+portScan+"/nifi-api/controller/cluster");
        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent())); 
        String line = ""; 
       
        JSONParser parser = new JSONParser(); 
        Object obj;
		try {
			obj = parser.parse(rd); 
	        JSONObject jsonObject = (JSONObject) obj; 
	        JSONObject cluster = (JSONObject) jsonObject.get("cluster");
	        
	        JSONArray nodes = (JSONArray) cluster.get("nodes");
	        Iterator<JSONObject> iterator = nodes.iterator();
	        
	        System.out.println("Cluster nodes:");
	        while (iterator.hasNext()) {
	        	JSONObject node = (JSONObject) iterator.next();
	        	// get address, apiPort, roles. - connect to Primary node
	        	 String name = (String) node.get("address");
	        	 long apiPort = (Long) node.get("apiPort");
	             System.out.println("\t"  + name + ":" + apiPort); 
	             JSONArray nodeRoles = (JSONArray) node.get("roles");
	             if(nodeRoles.indexOf("Cluster Coordinator") > -1){
	            	 System.out.println(" Found: " + name);
	            	 return name + ":" + zookeeperPort;
	             }
	        }
		} catch (ParseException e) { 
			e.printStackTrace();
		} 
        return null;
	}
	
}
