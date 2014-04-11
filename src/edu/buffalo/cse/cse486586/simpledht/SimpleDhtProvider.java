package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.Executor;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
	private static final String KEY_FIELD = "key",VALUE_FIELD="value";
	private static final String TABLE_NAME = "messageTable";
	private String MY_PORT,MY_PORT_HASH;
	private SQLiteDatabase messageTable;
	private DatabaseHelper mOpenHelper;
	private static final String SQL_CREATE_MAIN = "CREATE TABLE " +
		    TABLE_NAME +                       // Table's name
		    " (" +                           // The columns in the table
		    " key STRING, " +
		    " value STRING )";
	static final int SERVER_PORT = 10000;
	private static final String TAG = "SimpleDHT";
	private volatile String predecessor = null;
	private volatile String successor = null;
	private int MY_AVD_NUM;
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
    	Log.d("delete",selection.toString());
    	messageTable = mOpenHelper.getWritableDatabase();
    	messageTable.delete(TABLE_NAME, null, null);
    	return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
    	Log.d("insert", values.toString());
    	messageTable = mOpenHelper.getWritableDatabase();
    	Log.d("writabledatabase", values.toString());
    	String keyHash=null;
    	Message[] M = null;
    	try {
			if(successor ==null){
				Log.d(TAG, "sucessor = null");
				messageTable.insert(TABLE_NAME, null, values);
			}
			// the hash is less than equal to current
			else{
				String succHash = genHash(successor);
				String predHash = genHash(predecessor);
				keyHash = genHash(values.getAsString(KEY_FIELD));
				//current hash is bigger than key hash
				if(MY_PORT_HASH.compareTo(keyHash)>=0){
				//the hash is greater than predecessor
				if(keyHash.compareTo(predHash)>0){
					messageTable.insert(TABLE_NAME, null, values);
				}else{//the hash is smaller the the predecessor
					if(MY_PORT_HASH.compareTo(predHash)<0 && MY_PORT_HASH.compareTo(succHash)<0){
						messageTable.insert(TABLE_NAME, null, values);
					}else{
						M[0]=new Message("insert");
						M[0].setDestination(predecessor);
						M[0].setKeyHash(keyHash);
						M[0].setInsertKey(values.getAsString(KEY_FIELD));
						M[0].setInsertData(values.getAsString(VALUE_FIELD));
						new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, M);
					}
				}
			}else{
				//the key hash is larger than current hash
				if(MY_PORT_HASH.compareTo(predHash)<0 && MY_PORT_HASH.compareTo(succHash)<0 && keyHash.compareTo(predHash)>0){
					messageTable.insert(TABLE_NAME, null, values);
				}else{
					M[0]=new Message("insert");
					M[0].setDestination(successor);
					M[0].setKeyHash(keyHash);
					M[0].setInsertKey(values.getAsString(KEY_FIELD));
					M[0].setInsertData(values.getAsString(VALUE_FIELD));
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, M);
				}
			}
		}
			} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        return uri;
    }

    @Override
    public boolean onCreate() {
    	mOpenHelper = new DatabaseHelper(getContext(),TABLE_NAME+".db");
    	TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));
        MY_AVD_NUM= (Integer.parseInt(portStr));
        try {
			MY_PORT_HASH=genHash(String.valueOf(MY_AVD_NUM));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, (Message[])null);
			Log.d(TAG, "test tag");
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}
    	return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
    	Cursor cursor= null;
    	String keyHash=null;
    	String predHash = null;
    	String succHash=null;
    	Message[] M = null;
    	messageTable = mOpenHelper.getReadableDatabase();
    	if(selection.equalsIgnoreCase("*")){
    		//queryBuilder.appendWhere("key = '" + selection + "'");
    		cursor=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME, null);
    	}
    	else if(selection.equalsIgnoreCase("@")){
    		cursor=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME, null);	
    	}else{
    		try {
    			succHash = genHash(successor);
				predHash = genHash(predecessor);
				keyHash=genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
    		//current hash is bigger than key hash
			if(MY_PORT_HASH.compareTo(keyHash)>=0){
			//the hash is greater than predecessor
				if(keyHash.compareTo(predHash)>0){
					cursor=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE key='"+selection+"'", null);
	    		}
			else{//the hash is smaller the the predecessor
				if(MY_PORT_HASH.compareTo(predHash)<0 && MY_PORT_HASH.compareTo(succHash)<0){
					cursor=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE key='"+selection+"'", null);
				}else{
					M[0]= new Message("query");
					M[0].setDestination(predecessor);
					M[0].setKeyHash(keyHash);
					M[0].setSelection(selection);
					new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, M);
				}
			}
		}else{
			//the key hash is larger than current hash
			if(MY_PORT_HASH.compareTo(predHash)<0 && MY_PORT_HASH.compareTo(succHash)<0 && keyHash.compareTo(predHash)>0){
				cursor=messageTable.rawQuery("SELECT * FROM "+TABLE_NAME+" WHERE key='"+selection+"'", null);
			}else{
				M[0]= new Message("query");
				M[0].setDestination(predecessor);
				M[0].setKeyHash(keyHash);
				M[0].setSelection(selection);
				new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, M);
			}
		}
	}
    	Log.d("query ", selection +" ");
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
    protected static final class DatabaseHelper extends SQLiteOpenHelper {

		//private SQLiteDatabase messageTable;
		private static final String TAG = "DatabaseHelper"; 
		
		public DatabaseHelper(Context context,String tableName){
			super(context,tableName,null,1);
		}
		
		@Override
		public void onCreate(SQLiteDatabase database) {
			Log.i(TAG,"inside onCreate" );
			database.execSQL(SQL_CREATE_MAIN);
		}
		
		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// TODO Auto-generated method stub
			Log.i(TAG,"inside onUpgrade" );
		}
	}
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			
			ServerSocket serverSocket = sockets[0];
			Socket socket=null;
			Socket successorSocket = null;
			Message inputString =null,successorMessage=null;
			ObjectInputStream input =null,inputSuccessor=null;
			String requesterPortNum=null,requesterPortHash=null;
			ObjectOutputStream outputSuccessor=null,out=null;
			while(true){
				try {
					Log.d(TAG, "before sever socket is created in SEVERTASK");
					socket = serverSocket.accept();
					Log.d(TAG, "sever socket is created in SEVERTASK");
					input = new ObjectInputStream(socket.getInputStream());
					inputString=(Message)input.readObject();
					Log.d(TAG, "after recieing object");
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				if(inputString.getMsgType().equalsIgnoreCase("request")){
					Log.d(TAG, "is a requst msg");
					requesterPortNum=inputString.getPort();
					try {
						requesterPortHash = genHash(requesterPortNum);
						Log.d(TAG, "hash generated is "+ requesterPortHash+" and my hash is "+MY_PORT_HASH);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					if(MY_PORT_HASH.compareTo(requesterPortHash)<0){
						//TODO: send to successor
						Log.d(TAG, "hash is larger");
						try {
							if(inputString.getInsertAfter()){
								Log.d(TAG,"insert after called");
								successorMessage = insertAfter(inputString);
							}else if(inputString.getInsertBefore()){
								Log.d(TAG,"insert before called");
								successorMessage = insertBefore(inputString);
							}
							else if(successor!=null&&predecessor!=null){
								Log.d(TAG,"successor isnt null");
								String succHash = genHash(successor);
								String predHash = genHash(predecessor);
								if(predecessor.equals(successor)){
									Log.d(TAG, "second insertion");
									successorSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(predecessor)*2);
									outputSuccessor = new ObjectOutputStream(successorSocket.getOutputStream());
									if(MY_PORT_HASH.compareTo(predHash)>0){
										Log.d(TAG, "current is larger than pred");
										inputString.setPred(String.valueOf(MY_AVD_NUM));
										inputString.setInsertBefore(true);
										successor = inputString.getPort();
									}else{
										Log.d(TAG, "current is smaller than pred");
										//////////////////////////////////////////////////////////////////////
										if(requesterPortHash.compareTo(predHash)<0){
											Log.d(TAG, "new is smaller than pred");
											successor=inputString.getPort();
											inputString.setInsertBefore(true);
											inputString.setPred(String.valueOf(MY_AVD_NUM));
										}else{
											Log.d(TAG,"new is larger than pred");
											predecessor = inputString.getPort();
											inputString.setInsertAfter(true);
											inputString.setSuc(String.valueOf(MY_AVD_NUM));
										}
									}
									outputSuccessor.writeObject(inputString);
									outputSuccessor.flush();
									inputSuccessor=new ObjectInputStream(successorSocket.getInputStream());
									successorMessage = (Message)inputSuccessor.readObject();
									successorSocket.close();
								}else if(predHash.compareTo(MY_PORT_HASH)>0 &&
										succHash.compareTo(MY_PORT_HASH)>0 &&
										requesterPortHash.compareTo(predHash)>0){
									Log.d(TAG, "largest element insertion");
									inputString.setInsertAfter(true);
									inputString.setSuc(String.valueOf(MY_AVD_NUM));
									predecessor=inputString.getPort();
									Log.d(TAG, "send back to prev object to insertAfter");
									successorMessage=inputString;
								}else{
									Log.d(TAG, "forward to next");
									successorSocket= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(successor)*2);
									outputSuccessor = new ObjectOutputStream(successorSocket.getOutputStream());
									if(requesterPortHash.compareTo(succHash)<0){
										inputString.setInsertBefore(true);
										successor=inputString.getPort();
										inputString.setPred(String.valueOf(MY_AVD_NUM));
									}
									outputSuccessor.writeObject(inputString);
									outputSuccessor.flush();
									inputSuccessor=new ObjectInputStream(successorSocket.getInputStream());
									successorMessage = (Message)inputSuccessor.readObject();
									Log.d(TAG, "received after forwarding");
									if(successorMessage.getInsertAfter()){
										Log.d(TAG, "insert after called");
										successorMessage=insertAfter(successorMessage);
									}
									successorSocket.close();
								}
							}else{
								Log.d(TAG, "setting suc and pred for first time");
								predecessor =successor=inputString.getPort();
								successorMessage=inputString;
								successorMessage.setPred(String.valueOf(MY_AVD_NUM));
								successorMessage.setSuc(String.valueOf(MY_AVD_NUM));
//								Log.i(TAG,MY_PORT+"pred = "+ predecessor+" succ = "+successor);
							}
							Log.d(TAG,"sending to "+socket.getLocalPort());
							out = new ObjectOutputStream(socket.getOutputStream());
							out.writeObject(successorMessage);
							out.flush();
							out.close();
						} catch (NumberFormatException e) {
							e.printStackTrace();
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						} catch (NoSuchAlgorithmException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else{
						//TODO: insert node behind you and tell your predecessor that the new node is his successor
						Log.d(TAG, "hash is smaller");
						if(inputString.getInsertAfter()){
							Log.d(TAG,"insert after called");
							inputString = insertAfter(inputString);
						}else if(inputString.getInsertBefore()){
							Log.d(TAG,"insert Before called");
							inputString = insertBefore(inputString);
						}else if(successor==null){
							Log.d(TAG, "setting suc and pred for first time");
							predecessor = inputString.getPort();
							successor=inputString.getPort();
							inputString.setPred(String.valueOf(MY_AVD_NUM));
							inputString.setSuc(String.valueOf(MY_AVD_NUM));
						}else {
							try {
								String predHash=null;
								String succHash = null;
								try {
									succHash = genHash(successor);
									predHash = genHash(predecessor);
								} catch (NoSuchAlgorithmException e) {
									e.printStackTrace();
								}
								successorSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(predecessor)*2);
								if(predecessor.equals(successor)&&
										MY_PORT_HASH.compareTo(predHash)<0){
									predecessor=inputString.getPort();
									inputString.setSuc(String.valueOf(MY_AVD_NUM));
									inputString.setInsertAfter(true);
									out=new ObjectOutputStream(successorSocket.getOutputStream());
									out.writeObject(inputString);
									input = new ObjectInputStream(successorSocket.getInputStream());
									inputString = (Message)input.readObject();
									successorSocket.close();
								}else if(predecessor.equals(successor)){
									if(requesterPortHash.compareTo(predHash)>0){
										inputString.setPred(String.valueOf(MY_AVD_NUM));
										successor= inputString.getPort();
										inputString.setInsertBefore(true);	
									}else{
										predecessor=inputString.getPort();
										inputString.setSuc(String.valueOf(MY_AVD_NUM));
										inputString.setInsertAfter(true);
									}
									out=new ObjectOutputStream(successorSocket.getOutputStream());
									out.writeObject(inputString);
									input = new ObjectInputStream(successorSocket.getInputStream());
									inputString = (Message)input.readObject();
									successorSocket.close();
								}else{
									Log.d(TAG,"send it behind");
									outputSuccessor = new ObjectOutputStream(successorSocket.getOutputStream());
									if(requesterPortHash.compareTo(predHash)>0){
										Log.d(TAG,"insert after is set to true");
										inputString.setInsertAfter(true);
										predecessor=inputString.getPort();
										inputString.setSuc(String.valueOf(MY_AVD_NUM));
									}else if(MY_PORT_HASH.compareTo(predHash)<0 && MY_PORT_HASH.compareTo(succHash)<0){
										inputString.setInsertAfter(true);
										predecessor=inputString.getPort();
										inputString.setSuc(String.valueOf(MY_AVD_NUM));
									}
									outputSuccessor.writeObject(inputString);
									outputSuccessor.flush();
									inputSuccessor=new ObjectInputStream(successorSocket.getInputStream());
									successorMessage = (Message)inputSuccessor.readObject();
									Log.d(TAG, "got back from predecessor");
									if(successorMessage.getInsertBefore()){
										Log.d(TAG, "insertBefore called");
										successorMessage=insertBefore(successorMessage);
									}
									inputString=successorMessage;
									successorSocket.close();
								}
								
							} catch (NumberFormatException e) {
								e.printStackTrace();
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							}
						}
						try {
							out=new ObjectOutputStream(socket.getOutputStream());
							out.writeObject(inputString);
							out.flush();
							out.close();
							Log.d(TAG,"out closed");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					Log.d(TAG,MY_AVD_NUM+" pred = "+ predecessor+" succ = "+successor);
				}else if(inputString.getMsgType().equalsIgnoreCase("message")){
					//TODO: code for query
					ContentValues cV = new ContentValues();
					cV.put(KEY_FIELD, inputString.getInsertKey());
					cV.put(VALUE_FIELD, inputString.getInsertData());
					insert(null, cV);
				}
			}
		}
		private Message insertAfter(Message message){
			successor = message.getPort();
			message.setPred(String.valueOf(MY_AVD_NUM));
			message.setInsertAfter(false);
			return message;
		}
		private Message insertBefore(Message message){
			predecessor = message.getPort();
			message.setSuc(String.valueOf(MY_AVD_NUM));
			message.setInsertBefore(false);
			return message;
		}
    }
    private class ClientTask extends AsyncTask<Message, Void, Void>{

		@Override
		protected Void doInBackground(Message... msg) {
			try {
				if(MY_AVD_NUM!=5554 && msg==null){
					Log.d(TAG, "in Client Tast");
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					        Integer.parseInt(REMOTE_PORT0));
					Message joinRequest = new Message("request");
					joinRequest.setPort(String.valueOf(MY_AVD_NUM));
					ObjectInputStream in = null;
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(joinRequest);
					out.flush();
					in = new ObjectInputStream(socket.getInputStream());
					joinRequest = (Message) in.readObject();
					successor = joinRequest.getSuc();
					predecessor = joinRequest.getPred();
					socket.close();
					Log.d(TAG,"socket closed in client");
					Log.d(TAG,MY_AVD_NUM+" pred = "+ predecessor+" succ = "+successor);
				}else if(msg!=null&&msg[0].getMsgType().equalsIgnoreCase("insert")){
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					        Integer.parseInt(msg[0].getDestination())*2);
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msg[0]);
					out.flush();
					socket.close();
				}else if(msg!=null&&msg[0].getMsgType().equalsIgnoreCase("query")){
					
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}
    }
}