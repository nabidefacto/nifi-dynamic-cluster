package ru.itis.suc;
 

import java.util.*;

import org.apache.http.client.ClientProtocolException;

import java.net.*;
import java.io.*;

class HostScanner {

	private int scanPort; 
	private int socketTimeout = 100;
	private int echoTimeout = 150;
	private String localIp;
	private List<String> ips;
	
	private static List<String> reachableIps;
	public HostScanner(){
		scanPort = 8080;
	}
	public HostScanner(int port){
		scanPort = port;
	}
	
	public List<String> scan() {
		if(ips==null){
			ips =  findIps();
		}
		return ips;
	}
	
	public List<String> rescan(){
		ips = new ArrayList<String>();
		reachableIps = new ArrayList<String>();
		return scan();
	}

	
	
	public List<String> findIps() {
		List<String> ips = new ArrayList<String>();

		byte[] ip;
		try {
			InetAddress ipAddress = InetAddress.getByName(getAddress());
			ip = ipAddress.getAddress();
		} catch (Exception e) {
			log("Cannot get local IP");
			return ips;
		}

		for (int i = 1; i <= 254; i++) {
			ip[3] = (byte) i;
			try {
				InetAddress address = InetAddress.getByAddress(ip); 
				String output = address.toString().substring(1);
				if (scanPort(output)) {
					log("Found required port: " + output);
					ips.add(output);
				} 
			} catch (IOException e) {
				log(e.getMessage());
			}
		}
		return ips;
	}
	
	/**
	 * 
	 * @return
	 */
	private List<String> findIpsOld() {
		if(reachableIps==null){
			findReachableIps();
		}
		System.out.println(reachableIps);
		if(reachableIps==null){
			return null;
		}
		Iterator<String> iter = reachableIps.iterator();
		while(iter.hasNext()) {
			String ip = iter.next(); 
			if (scanPort(ip)) {
				ips.add(ip);
			} 
		} 
		return ips;
	} 

	/**
	 * 
	 * @return
	 */
	private List<String> findReachableIps()
	{
		byte[] ip;
		try {
			InetAddress ipAddress = InetAddress.getByName(getAddress());
			ip = ipAddress.getAddress();
			System.out.println("Search with ip : " + InetAddress.getByAddress(ip));
		} catch (Exception e) {
			e.printStackTrace();
			return reachableIps;
		}
		
		for (int i = 1; i <= 254; i++) {
			ip[3] = (byte) i;
			try {
				InetAddress address = InetAddress.getByAddress(ip); 
				System.out.println("Check ip : " + address);
				if (address.isReachable(echoTimeout)) {
					String output = address.toString().substring(1); 
					reachableIps.add(output); 
					System.out.println("Found ip : " + output);
				}
			} catch (IOException e) { 
				e.printStackTrace();
			}
		}
		return reachableIps;
	}
	
	/**
	 * 
	 * @param host
	 * @return
	 */
	public boolean scanPort(String host) {
		SocketAddress sockaddr = new InetSocketAddress(host, scanPort);
		// Create your socket
		Socket socket = new Socket();
		boolean online = true;
		try {
			socket.connect(sockaddr, socketTimeout);
		} catch (SocketTimeoutException stex) {
			online = false;
		} catch (IOException iOException) {
			online = false;
		} finally {
			try {
				socket.close();
			} catch (IOException ex) {
			}
		}
		if (!online) { 
			return false;
		} 
		return true;
	}
 

	/**
	 * get ip address from network interface
	 * 
	 * @return
	 */
	private String getAddress() {
		if (localIp != null) {
			return localIp;
		}
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
							System.out.println("found local address: " + ipAddress);
							 
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
	
	/**
	 * 
	 * @param timeout
	 */
	public void setConnectionTimeout(int timeout){
		socketTimeout = timeout;
	} 
	
	/**
	 * 
	 * @param ip
	 */
	public void setLocalIp(String ip){
		localIp = ip;
	} 
	
	/**
	 * 
	 * @param aScanPort
	 */
	public void setPort(int aScanPort) {
		scanPort = aScanPort;
		ips = null;
	}
	/**
	 * get current scanning port
	 * @return
	 */
	public int getPort() {
		return scanPort;
	} 
	
	private void log(String str){
		System.out.println(str);
	}
}