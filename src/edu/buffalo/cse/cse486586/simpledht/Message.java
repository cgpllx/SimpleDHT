package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

public class Message implements Serializable {
	private boolean insertAfter=false,insertBefore=false;
	private String msgType = null,keyHash = null;
	private String portNum;
	private String pred=null, suc=null;
	private String destination= null;
	private String origin = null;
	private String selection = null;
	private String insertKey = null;
	private String insertData = null;
	private HashMap<String,String> db=null;
	
	
	public Message() {
	}
	public Message(String msgType){
		this.setMsgType(msgType);
	}
	public void setPort(String portNum){
		this.portNum = portNum;
	}
	public String getPort(){
		return portNum;
	}
	public boolean getInsertAfter() {
		return this.insertAfter;
	}
	public void setInsertAfter(boolean insertHere) {
		this.insertAfter = insertHere;
	}
	public String getPred() {
		return pred;
	}
	public void setPred(String pred) {
		this.pred = pred;
	}
	public String getSuc() {
		return suc;
	}
	public void setSuc(String suc) {
		this.suc = suc;
	}
	public boolean getInsertBefore() {
		return insertBefore;
	}
	public void setInsertBefore(boolean insertBefore) {
		this.insertBefore = insertBefore;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public String getMsgType() {
		return msgType;
	}
	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}
	public String getKeyHash() {
		return keyHash;
	}
	public void setKeyHash(String keyHash) {
		this.keyHash = keyHash;
	}
	public String getSelection() {
		return selection;
	}
	public void setSelection(String selection) {
		this.selection = selection;
	}
	public String getInsertKey() {
		return insertKey;
	}
	public void setInsertKey(String insertKey) {
		this.insertKey = insertKey;
	}
	public String getInsertData() {
		return insertData;
	}
	public void setInsertData(String insertData) {
		this.insertData = insertData;
	}
	public String getOrigin() {
		return origin;
	}
	public void setOrigin(String origin) {
		this.origin = origin;
	}
	public HashMap<String,String> getDb() {
		return db;
	}
	public void setDb(HashMap<String,String> db) {
		this.db = db;
	}
}
