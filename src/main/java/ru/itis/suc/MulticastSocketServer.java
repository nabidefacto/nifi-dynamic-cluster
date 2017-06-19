package ru.itis.suc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.*;
import java.util.TreeSet;
public class MulticastSocketServer {
    
    final static String INET_ADDR = "239.255.255.255";
    final static int PORT = 8888;
    static TreeSet setIps;
    
    final static int READY_FOR_CLUSTER = 0;
    final static int VOTING_FOR_CLUSTER = 1;
    final static int JOIN_CLUSTER = 3;
    
    public static void main2(String[] args) throws UnknownHostException, InterruptedException {
        // Get the address that we are going to connect to.
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        String msg = args[0];
        try {
        	DatagramSocket serverSocket = new DatagramSocket();
        	int i = 0; 	
            while (true) {
            	i++;                
                DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
                serverSocket.send(msgPacket);
                System.out.println("[SENT] " + msg);
                Thread.sleep(2500);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        // Get the address that we are going to connect to.
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        NifiLocalNode nfNode = new NifiLocalNode("/");
        String msg = nfNode.getIp();
        setIps = new TreeSet<String>();        
        byte[] buf = new byte[256]; 
        try {
        	MulticastSocket clientSocket = new MulticastSocket(PORT);
        	 
        	DatagramSocket serverSocket = new DatagramSocket(); 
        	clientSocket.joinGroup(addr); 
        	int i = 0;
        	boolean needSearch = true;
            while (needSearch) {
            	i++;            	
                DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
                serverSocket.send(msgPacket);
                System.out.println("[SENT] " + msg); 
                if( i > 5 ) {
                	DatagramPacket msgPacket2 = new DatagramPacket(buf, buf.length); 
                	clientSocket.receive(msgPacket2);
                	System.out.println("[RECEIVE] " + msgPacket2.getAddress());
                	String cmd = new String(buf, 0, buf.length);
                	System.out.println("[RECEIVE]  " + cmd);
                	setIps.add(msgPacket2.getAddress().toString());
            	}
                Thread.sleep(500);
                if(setIps.size()>2){
                	System.out.println("[START NEW CLUSTER]  " + setIps);
                	needSearch = false;
                	clientSocket.leaveGroup(addr); 
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
     
}
