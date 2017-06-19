package ru.itis.suc;

import java.io.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class MainAgent {
 
	public static void main(String args[]){ 
		int sleepTimeout = 30000; // 30 seconds
		if(args.length < 1){
			System.out.println("Specify nifi root directory");
			return;
		}
		String nifiDir = args[0];
		boolean inWhile = args.length==2; // run in while
		
		System.out.println("Starting agent.");  
		do{
			try{
				System.out.println("Searching cluster...");
				searchCluster(nifiDir);
				Thread.sleep(sleepTimeout); 
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
	public static void searchCluster(String nifiDir) throws IOException { 
		NifiLocalNode localNode = new NifiLocalNode(nifiDir); 
		if(localNode.isActive()){
			System.out.println("Local node already running: " + localNode);
		}else{
			HostScanner hostScanner = new HostScanner(8080);
			NifiClusterScanner scanner = new NifiClusterScanner(hostScanner);
			String clusterConnectString = scanner.findLocalCluster();
			if(clusterConnectString!=null){
				System.out.println("Found cluster: " + clusterConnectString);
				System.out.println("Updating local properties");
				localNode.joinCluster(clusterConnectString);
				System.out.println("Restarting local node");
				localNode.restart();
			}
		} 
	}
	 
} 