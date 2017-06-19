package ru.itis.suc;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * to test if current ensemble will update properties in given folder
 * 
 * @author Nabi DeFacto
 */
public class NodeAgentTest implements Runnable{
	
	public String[] ips;
 
	public static int agentPort = 8085;
	public static int searchClusterTimeoutSeconds = 10; 
	static ServerSocketChannel serverSocketChannel; 
	Socket csocket;
	public static void main(String args[]){
		if(args.length < 1){
			System.out.println("Please specify test nifi dir");
		}
		// small tests
		NifiLocalNode node = new NifiLocalNode(args[0]);
		List <String> ips = new ArrayList<String>();
		ips.add("10.102.0.206");
		ips.add("10.102.0.123");
		ips.add("192.168.66.1");
		ips.add("10.102.0.125");

		
		NifiClusterEnsemble ensemble = new NifiClusterEnsemble(node, ips); 
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
				if(currentTimeout > clearTimeout){
					System.out.println("Searching cluster...");
					ensemble.start();
					currentTimeout = 0;
				}
				SocketChannel socketChannel = serverSocketChannel.accept();
			 	if(socketChannel!=null){
			 		System.out.println("Connected"); 
			 		socketChannel.finishConnect();
			 		socketChannel.close();
			 	}
				Thread.sleep(200);
				currentTimeout++;
			}catch(IOException e){
				e.printStackTrace(); 
			}catch(Exception e){
				e.printStackTrace();
			}
		} while(true); 
		 
		
	}
	
	private static Object startAgentListener() throws IOException{
		class ClientSocket implements Runnable{
			ServerSocketChannel ssock;
			public ClientSocket(ServerSocketChannel s){
				ssock = s;
			}
			public void run(){
				try{
					System.out.println("Listening");  
					SocketChannel sock = ssock.accept(); 
					if(sock!=null){
			        	System.out.println("Connected"); 
						sock.finishConnect();
						sock.close();
			        }
			        
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}  
		return new ClientSocket(serverSocketChannel);
	}
	
	NodeAgentTest(Socket csocket) {
	      this.csocket = csocket;
	}
	
	public void run() {
	  try {
	     PrintStream pstream = new PrintStream(csocket.getOutputStream());
	     for (int i = 10; i >= 0; i--) {
	        pstream.println(i + " ping");
	     }
	     pstream.close();
	     csocket.close();
	  } catch (IOException e) {
	     System.out.println(e);
	  }
	}
	private static void log(String str){
		System.out.println(str);
	}
	 
} 