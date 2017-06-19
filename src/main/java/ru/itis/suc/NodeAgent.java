package ru.itis.suc;

import java.io.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;

public class NodeAgent {
	
	public String[] ips;
	public static int agentPort = 8085;
	private static int quorumSize = 3; 
	public static int searchClusterTimeoutSeconds = 20; 
	static ServerSocketChannel serverSocketChannel;  
	public static void main(String args[]) {
		if(args.length < 1){
			System.out.println("Specify nifi root directory");
			return;
		}
		String nifiDir = args[0];
		if(args.length==2){
			agentPort = Integer.parseInt(args[1]);
		}
		boolean inWhile = true;
		System.out.println("Starting node agent.");
	 
		try{
			serverSocketChannel = ServerSocketChannel.open(); 
			serverSocketChannel.socket().bind(new InetSocketAddress(agentPort)); 
			serverSocketChannel.configureBlocking(false); 
		}catch(IOException e){
			e.printStackTrace(); 
		}
		
		int clearTimeout = (int) searchClusterTimeoutSeconds*1000*5;
		int currentTimeout = clearTimeout + 1;
		
		do{
			try{ 
				SocketChannel socketChannel = serverSocketChannel.accept();
				if(socketChannel!=null){
			 		System.out.println("Connected"); 
			 		socketChannel.finishConnect();
			 		socketChannel.close();
			 	}
				if(currentTimeout > clearTimeout){
					HostScanner hostScanner = new HostScanner(8080);
					System.out.println("Searching cluster..."); 
					if(!searchCluster(hostScanner, nifiDir)){
						System.out.println("Try to create..."); 
						createCluster(hostScanner, nifiDir); // try to ensemble new cluster
					}
					currentTimeout = 0;
				}
			 	
				Thread.sleep(200);
				currentTimeout++;
			}catch(IOException e){
				e.printStackTrace(); 
			}catch(Exception e){
				e.printStackTrace();
			}
		} while(inWhile); 
		 
	} 
	/**
	 * 
	 * @param nifiDir
	 */
	public static boolean searchCluster(HostScanner hostScanner, String nifiDir) throws IOException { 
		NifiLocalNode localNode = new NifiLocalNode(nifiDir);  
		NifiClusterScanner clusterScanner = new NifiClusterScanner(hostScanner);
		String clusterConnectString = clusterScanner.findLocalCluster();
		if(clusterConnectString!=null){			 
			if(localNode.isActive() && localNode.isInCluster(clusterConnectString)){
				log("Already in cluster: " + clusterConnectString);
				return true;
			}
			log("Found cluster: " + clusterConnectString);
			log("Updating local properties");
			localNode.joinCluster(clusterConnectString);
			log("Restarting local node");
			localNode.restart();
			return true;
		} 
		return false;
	}
	
	public static void createCluster(HostScanner hostScanner, String nifiDir){
		NifiLocalNode localNode = new NifiLocalNode(nifiDir); 
		hostScanner.setPort(agentPort);
		// find agents which can be used to ensemble new cluster
		List<String> ips = hostScanner.scan();
		if(ips.size() > quorumSize){
			NifiClusterEnsemble cluster = new NifiClusterEnsemble(localNode, ips);
			try {
				cluster.start();
			} catch (IOException e) { 
				e.printStackTrace();
			}
		}else{
			log("Waiting more nodes for quorum. Currently: " + ips.size());
		}
		
	}
	
	private static void log(String str){
		System.out.println(str);
	}
	 
} 