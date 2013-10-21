package edu.ucla.nesl.sensorsafe.db;

import java.sql.SQLException;
import java.util.List;

import net.minidev.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.ucla.nesl.sensorsafe.model.Rule;
import edu.ucla.nesl.sensorsafe.model.RuleCollection;
import edu.ucla.nesl.sensorsafe.model.Stream;

public interface StreamDatabaseDriver extends DatabaseDriver {
	
	public RuleCollection getRules() 
			throws SQLException;
	
	public void storeRule(Rule rule) 
			throws SQLException;
	
	public void deleteAllRules() 
			throws SQLException;
	
	public void createStream(Stream stream) 
			throws Exception;
	
	public void addTuple(String name, String strTuple) 
			throws Exception;

	public void addTuple(String name, String time, String value) 
			throws Exception;
	
	public int queryStream(String name, String startTime, String endTime, String expr) 
			throws Exception;
	
	public Stream getStreamInfo(String name) 
			throws SQLException;
	
	public List<Stream> getStreamList() 
			throws SQLException;

	public void deleteStream(String name, String startTime, String endTime)
			throws SQLException;
	
	public void deleteAllStreams() 
			throws SQLException;

	public void setCurrentUser(String username);
	
}
