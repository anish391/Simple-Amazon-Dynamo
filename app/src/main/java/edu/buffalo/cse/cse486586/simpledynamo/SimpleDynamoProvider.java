package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.TreeMap;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private String myPort;
	private int count=0;
	private int successorPort;
	private String myAvd;
	private String myHash;
	private String successorHash;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	private static final String keyField = "key";
	private static final String valueField = "value";
	static String portArray[] = {"11108","11112","11116","11120","11124"};
	private ArrayList<String> nodeList = new ArrayList<String>();
	private ArrayList<String> hashedNodes = new ArrayList<String>();
	private ArrayList<Integer> mySuccessors = new ArrayList<Integer>();
	private TreeMap<String, Integer> treeMap = new TreeMap();
	private TreeSet<String> keyList = new TreeSet<String>();
	private ArrayList<String> failedMessages = new ArrayList<String>();

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		myAvd = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(myAvd) * 2));
		try{
			int port = 11108;
			myHash = genHash(myAvd);
			for(int i=0;i<5;i++) {
				nodeList.add(Integer.toString(port/2));
				hashedNodes.add(genHash(Integer.toString(port/2)));
				treeMap.put(genHash(Integer.toString(port/2)),port);
				port += 4;
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Collections.sort(hashedNodes);
		initializeSuccessors();
		Log.e("ONCREATE", "My Port: " +myPort);
		System.out.println("My successors: "+ mySuccessors);
		try{
			Log.v("ONCREATE","Server Socket creation");
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			// Sending a Recovery Message to all other nodes to alert them to send failed Messages to the node sending Recovery message.
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "RECOVERY<><"+myPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	// Used to initialize two immediate successors for a given node.
	private void initializeSuccessors(){
		int n = hashedNodes.size();
		int i = hashedNodes.indexOf(myHash);
		String successor1 = hashedNodes.get((n+i+1)%n);
		String successor2 = hashedNodes.get((n+i+2)%n);
		successorHash = successor1;
		successorPort = treeMap.get(successorHash);
		Log.v(TAG,"Successor Hash: " + successorHash);
		Log.v(TAG, "Successor Port: " + successorPort);
		mySuccessors.add(treeMap.get(successor1));
		mySuccessors.add(treeMap.get(successor2));
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String) values.get(keyField);
		String value = (String) values.get(valueField)+"\n";
		try {
			String hashedKey = genHash(key);
			int position = findNode(hashedKey);
			Log.e("FIRSTINSERT","Key: "+key+" Hash: "+hashedKey+" value: "+value+" position: "+position);
			callInsert(key,value);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void callInsert(String key, String value) {
		String hashedMessage="";
		try {
			hashedMessage = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		int position = findNode(hashedMessage);
		Log.e("CALLINSERT", "Key: "+key +" hashmessage: "+ hashedMessage+"position: "+position);
		// If the message is to be entered at the current node. Add it and replicate it at the successors.
		if(position==Integer.parseInt(myPort)){
			nodeInsert(key,value);
			String message = "REPLICATE"+"<><"+key+"<><"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message+"<><"+Integer.toString(mySuccessors.get(0)));
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message+"<><"+Integer.toString(mySuccessors.get(1)));
		}
		// Otherwise pass the message to the correct node.
		else{
			String message = "INSERT"+"<><"+key+"<><"+value+"<><"+Integer.toString(position);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
		}

	}

	// Function to insert key, value pair in a node.
	private void nodeInsert(String key, String value){
		try{
			Log.e("NODEINSERT","Insert Message " + key +"with hash "+ genHash(key) +" value "+value+" at port "+myPort);
			keyList.add(key);
			FileOutputStream opStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			opStream.write(value.getBytes());
			opStream.close();
		} catch (FileNotFoundException e) {
			Log.e("NODEINSERT","File not found error.");
			e.printStackTrace();
		} catch (IOException e) {
			Log.e("NODEINSERT","IO Exception.");
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	// Function to find a node for a given key.
	private int findNode(String hashedKey){
		int position;
		String hash="";
		int size = hashedNodes.size();
		if((hashedKey.compareTo(hashedNodes.get(size-1)))>0 ||(hashedKey.compareTo(hashedNodes.get(0)))<0)
			hash = hashedNodes.get(0);
		else{
			for(int i=0;i<size-1;i++){
				if((hashedKey.compareTo(hashedNodes.get(i))) > 0 && (hashedKey.compareTo(hashedNodes.get(i+1))) < 0){
					hash = hashedNodes.get(i+1);
					break;
				}
			}
		}
		position = treeMap.get(hash);
		return position;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor matrixCursor = null;
		try {
			Thread.sleep(50);
			//matrixCursor = callQuery(selection);
			Context context = getContext();
			String[] s = {"key","value"};
			matrixCursor = new MatrixCursor(s);
			ArrayList<String> fileList = new ArrayList<String>();
			fileList.addAll(Arrays.asList(getContext().fileList()));
			// Local Node Query
			if(selection.equals("@")){
				Log.e("LOCALQUERY", String.valueOf(keyList.size()));
				System.out.println("LOCALQUERY\n"+keyList);
				return singleNodeQuery();
			}
			// Global Node Query
			else if(selection.equals("*")){
				Log.e(TAG,"Global Query");
				try {
					return globalQuery();
				} catch (IOException e){
					Log.e("GLOBAL QUERY","IOException");
				}
			}
			// If single key query exists in the current node.
			else if(fileList.contains(selection)){
				Log.e("Simple Node query.","Selection: "+ selection + " hashed selection "+genHash(selection));
				try{
					InputStream inputStream = context.openFileInput(selection);
					if(inputStream !=null){
						BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
						String value = bufferedReader.readLine();
						String[] pair = {selection,value};
						matrixCursor.addRow(pair);
						return matrixCursor;
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			// If single key query does not exist in current node, locate the correct node and return query result.
			else{
				String hashedSelection = genHash(selection);
				int position = findNode(hashedSelection);
				Log.e("Successor Node Query","Check message: "+selection+" with hash "+hashedSelection+" at node: "+position);
				String value ="";
				try {
					value = successorNodeQuery("QUERY",selection, Integer.toString(position));
					Log.e("QUERYNOTATNODE","Returned value: "+value);
					String[] pair = {selection,value};
					matrixCursor.addRow(pair);
					return matrixCursor;
				} catch (IOException e) {
					// If the node has failed, query at replicated node.
					Log.e("SUCCESSORNODEQUERY","IOException.");
					int n = hashedNodes.size();
					int pos = findNode(hashedSelection)/2;
					String hashedPosition = genHash(Integer.toString(pos));;
					Log.e("SUCCESSORNODEQUERY", "position: "+position+" hashedPosition :"+hashedPosition);
					int i = hashedNodes.indexOf(hashedPosition);
					int successor1 = treeMap.get(hashedNodes.get((n+i+1)%n));
					int successor2 = treeMap.get(hashedNodes.get((n+i+2)%n));
					try {
						value = successorNodeQuery("QUERY",selection,Integer.toString(successor1));
						String[] pair = {selection,value};
						matrixCursor.addRow(pair);
						return matrixCursor;
					} catch (IOException e1) {
						// If the successor has failed too, query second replicated node.
						Log.e("SUCCESSORNODEQUERY","Happened again.");
						try {
							value = successorNodeQuery("QUERY",selection,Integer.toString(successor2));
							String[] pair = {selection,value};
							matrixCursor.addRow(pair);
							return matrixCursor;
						} catch (IOException e2) {
							Log.e("SUCCESSORNODEQUERY", "AND AGAIN");
						}
					}
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return matrixCursor;
	}

	// Function to query all nodes in a single node.
	private MatrixCursor singleNodeQuery(){
		String[] s = {"key","value"};
		MatrixCursor matrixCursor = new MatrixCursor(s);
		String[] fileList = getContext().fileList();
		for(String key:fileList){
			try{
				InputStream inputStream = getContext().openFileInput(key);
				if(inputStream !=null){
					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
					String value = bufferedReader.readLine();
					MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
					mRowBuilder.add(s[0], key);
					mRowBuilder.add(s[1], value);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return matrixCursor;
	}

	// Function to find query at other nodes.
	private String successorNodeQuery(String type, String selection, String successorPort) throws IOException {
		String value = "";
		Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				Integer.parseInt(successorPort));
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		DataInputStream in = new DataInputStream(socket.getInputStream());
		// type = "QUERY"
		out.writeUTF(type+"<><"+selection);
		out.flush();
		while(!socket.isClosed()){
			String ack = in.readUTF();
			String[] serverContent = ack.split("<><");
			if(serverContent[0].equals("YES")){
				value = serverContent[1];
				Log.e(TAG,"Found value at Port: "+successorPort);
				out.close();
				socket.close();
				break;
			}

		}
		return value;
	}

	// Function to query all keys in all nodes.
	private MatrixCursor globalQuery() throws IOException {
		TreeMap<String, String> global = new TreeMap<String, String>();
		String[] s = {"key","value"};
		MatrixCursor matrixCursor = new MatrixCursor(s);
		ArrayList<String> fileList = new ArrayList<String>();
		fileList.addAll(Arrays.asList(getContext().fileList()));
		for(String k:fileList){
			InputStream inputStream = getContext().openFileInput(k);
			if(inputStream !=null) {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
				String value = bufferedReader.readLine();
				MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
				mRowBuilder.add(s[0], k);
				mRowBuilder.add(s[1], value);
			}
		}
		global = sendGlobalMessage("GLOBAL",Integer.toString(successorPort));
		Log.e(TAG,"GlobalQuery size: "+global.size());
		//System.out.println(global);
		for(String k: global.keySet()){
			MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
			Log.e(TAG,"GLOBAL KEY: "+k);
			Log.e(TAG,"GLOBAL VALUE: "+global.get(k));
			mRowBuilder.add(s[0], k);
			mRowBuilder.add(s[1], global.get(k));
		}
		return matrixCursor;
	}

	// Function to retrieve keys from all other nodes except current node and failed node.
	private TreeMap<String, String> sendGlobalMessage(String type, String port) throws IOException {
		TreeMap<String, String> global = new TreeMap<String, String>();
		for(String p: portArray){
			try {
				if (p.equals(port) || p.equals(myPort))
					continue;
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(p));
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("GLOBAL");
				out.flush();
				while(!socket.isClosed()){
					String ack = in.readUTF();
					String[] serverContent = ack.split("<><");
					Log.e(TAG,serverContent[0]);
					if(ack!=null){
						port = serverContent[0];
						Log.e(TAG,"Size of Key Value"+serverContent.length);
						System.out.println(ack);
						for(int i=1;i<serverContent.length;i+=2){
							global.put(serverContent[i],serverContent[i+1]);
						}
						out.close();
						socket.close();
						break;
					}
				}
				out.close();
				in.close();
				socket.close();
			} catch(IOException e){
				// If a node has failed, skip it.
				Log.e("GLOBAL QUERY","Port: "+p+ " has failed");
				continue;
			}
		}
		return global;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		getContext().deleteFile(selection);
		keyList.remove(selection);
		Log.e(TAG,"Size after deleting: "+keyList.size());
		return 0;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			Log.v(TAG,"Server Socket created.");
			try{
				while(true){
					Socket clientSocket = serverSocket.accept();
					DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
					DataInputStream in = new DataInputStream(clientSocket.getInputStream());
					String ip;
					while((ip = in.readUTF())!=null){
						String clientContent[] = ip.split("<><");
						if(clientContent[0].equals("REPLICATE")){
							String key = clientContent[1];
							String value = clientContent[2];
							String port = clientContent[3];
							Log.e("SERVERREPLICATE","Replicating key: "+key+"with keyHash "+genHash(key)+" value: "+value+" at port "+port);
							nodeInsert(key,value);
							out.writeUTF("ACK");
							out.flush();
							break;
						}
						if(clientContent[0].equals("INSERT")){
							String key = clientContent[1];
							String value = clientContent[2];
							String port = clientContent[3];
							String hashedSelection = genHash(key);
							int position = findNode(hashedSelection);
							Log.e("SERVERINSERT", "My Port: "+myPort);
							Log.e("SERVERINSERT", "INSERT "+key+" hashed "+hashedSelection+" value "+value+" at position: "+position);
							callInsert(key, value);
							out.writeUTF("ACK");
							out.flush();
							break;
						}
						if(clientContent[0].equals("QUERY")){
							String key = clientContent[1];
							String value="";
							ArrayList<String> fileList = new ArrayList<String>();
							fileList.addAll(Arrays.asList(getContext().fileList()));
							if(fileList.contains(key)){
								Log.e("SERVERQUERY",myPort+" contains key:"+key);
								InputStream inputStream = getContext().openFileInput(key);
								if(inputStream !=null) {
									BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
									value = bufferedReader.readLine();
									//Log.e(TAG, "Value: " + value);
									out.writeUTF("YES" + "<><" + value);
									out.flush();
									break;
								}
							}
							else{
								out.writeUTF("NO"+"<><"+successorPort);
								out.flush();
								break;
							}
						}
						if(clientContent[0].equals("GLOBAL")){
							String send = Integer.toString(successorPort);
							ArrayList<String> fileList = new ArrayList<String>();
							fileList.addAll(Arrays.asList(getContext().fileList()));
							for(String key: fileList){
								InputStream inputStream = getContext().openFileInput(key);
								if(inputStream !=null) {
									BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
									String value = bufferedReader.readLine();
									Log.e("SERVERGLOBAL", "Value: " + value);
									send = send+"<><"+key+"<><"+value;
								}
							}
							out.writeUTF(send);
							out.flush();
							break;
						}
						if(clientContent[0].equals("RECOVERY")){
							String returnMessage = "";
							Log.e("SERVERRECOVERY","Recovery from: "+clientContent[1]);
							System.out.println(failedMessages);
							if(failedMessages.size()!=0){
								for(String message: failedMessages){
									returnMessage += message + "<><";
								}
								failedMessages.clear();
								out.writeUTF(returnMessage);
								out.flush();
							}
							else{
								out.writeUTF("NAN");
								out.flush();
							}
							break;
						}
					}
					out.close();
					in.close();
					clientSocket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void>{

		@Override
		protected Void doInBackground(String... strings) {
			//Log.e(TAG,"Client Task.");
			String[] string = strings[0].split("<><");
			if(string[0].equals("REPLICATE")){
				//Log.e(TAG, "Replicate section.");
				String type = string[0];
				String key = string[1];
				String value = string[2];
				String port = string[3];
				String keyHash = "";
				try {
					keyHash = genHash(key);
					sendMessage(type,key,value,port);
				} catch (IOException e) {
					// If node has failed, store the failed Message in an arraylist to send later.
					Log.e("REPLICATEFAIL", "Port: "+port+" has failed. Key is: "+key+" with hash "+keyHash+" value "+value);
					String failedMessage = type+"<><"+key+"<><"+value+"<><"+port;
					failedMessages.add(failedMessage);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
			if(string[0].equals("INSERT")){
				String type = string[0];
				String key = string[1];
				String value = string[2];
				String port = string[3];
				try {
					sendMessage(type,key,value,port);
				} catch (IOException e) {
					try {
						// If node has failed, store the failed Message in an arraylist to send later but store the message at its successors.
						String hashedKey = genHash(key);
						Log.e("INSERTFAIL", "Port: "+port+" has failed. Key is: "+key+" with hash "+hashedKey+" value "+value);
						String failedMessage = type+"<><"+key+"<><"+value+"<><"+port;
						failedMessages.add(failedMessage);
						int n = hashedNodes.size();
						int position = findNode(hashedKey)/2;
						String hashedPosition = genHash(Integer.toString(position));;
						Log.e("INSERTFAIL", "position: "+position+" hashedPosition :"+hashedPosition);
						int i = hashedNodes.indexOf(hashedPosition);
						int successor1 = treeMap.get(hashedNodes.get((n+i+1)%n));
						int successor2 = treeMap.get(hashedNodes.get((n+i+2)%n));
						Log.e("INSERTFAIL","Successors: "+successor1+", "+successor2);
						sendMessage("REPLICATE",key,value,Integer.toString(successor1));
						sendMessage("REPLICATE",key,value,Integer.toString(successor2));
					} catch (NoSuchAlgorithmException e1) {
						e1.printStackTrace();
					} catch (IOException e1) {
						Log.e("INSERTFAIL", "FAIL AGAIN.");
						String failedMessage = type+"<><"+key+"<><"+value+"<><"+port;
						failedMessages.add(failedMessage);
					}
					//e.printStackTrace();
				}
			}
			// Code to handle recovery of nodes.
			if(string[0].equals("RECOVERY")){
				String type = string[0];
				String currentPort = string[1];
				for(String port:portArray){
					if(port.equals(currentPort))
						continue;
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						DataOutputStream out = new DataOutputStream(socket.getOutputStream());
						DataInputStream in = new DataInputStream(socket.getInputStream());
						out.writeUTF(type+"<><"+currentPort);
						out.flush();
						while(!socket.isClosed()){
							String ack = in.readUTF();
							if(ack!=null){
								String[] serverContent = ack.split("<><");
								out.close();
								socket.close();
								Log.e("CLIENTRECOVERY", "Reply from port "+port);
								// If a node does not contain any failed messages or if all nodes are just starting.
								if(serverContent[0].equals("NAN")) {
									Log.e(TAG,"NAN");
									break;
								}
								// Get failed messages from all other nodes.
								else{
									for(int i=0;i<serverContent.length;i+=4){
										String t = serverContent[i];
										String key = serverContent[i+1];
										String value = serverContent[i+2];
										String cport = serverContent[i+3];
										Log.e("CLIENTRECOVERY","Recovery of type "+t+" Inserting key: "+key+" value: "+value+" from port: "+port);
										nodeInsert(key,value);
									}
									break;
								}
							}
						}
						in.close();
						socket.close();
					} catch (IOException e) {
						Log.e("CLIENTRECOVERY","Last catch block.");
					}
				}
			}
			return null;
		}
	}

	// Code to send Insert/Replicate messages.
	private void sendMessage(String type, String key, String value, String port) throws IOException {
		if(port.equals(myPort)) {
			nodeInsert(key, value);
		}
		else {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF(type + "<><" + key + "<><" + value + "<><" + port);
			out.flush();
			while (!socket.isClosed()) {
				String ack = in.readUTF();
				if (ack != null) {
					out.close();
					socket.close();
				}
			}
			in.close();
			socket.close();
		}
	}
}
