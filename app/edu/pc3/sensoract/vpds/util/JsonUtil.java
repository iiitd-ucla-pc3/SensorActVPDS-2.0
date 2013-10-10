package edu.pc3.sensoract.vpds.util;

import java.lang.reflect.Type;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class JsonUtil {

	public static Gson json = null;
	public static Gson json1 = null;

	static {

		// source :
		// https://sites.google.com/site/gson/gson-type-adapters-for-common-classes-1
		json = new GsonBuilder().registerTypeAdapter(DateTime.class,
				new DateTimeTypeConverter()).create();
		
		json1 = new GsonBuilder()
		.serializeSpecialFloatingPointValues().setPrettyPrinting().create();

	}

	private static class DateTimeTypeConverter implements
			JsonSerializer<DateTime>, JsonDeserializer<DateTime> {
		@Override
		public JsonElement serialize(DateTime src, Type srcType,
				JsonSerializationContext context) {
			return new JsonPrimitive(src.toString());
		}

		@Override
		public DateTime deserialize(JsonElement json, Type type,
				JsonDeserializationContext context) throws JsonParseException {
			// System.out.println("Deser.." + json.getAsString());
			try {
				return new DateTime(json.getAsString(), DateTimeZone.UTC);
			} catch (IllegalArgumentException e) {
				// May be it came in formatted as a java.util.Date, so try that
				Date date = context.deserialize(json, Date.class);
				return new DateTime(date);
			}
		}
	}

	public class TestExclStrat implements ExclusionStrategy {

		@Override
		public boolean shouldSkipClass(Class<?> arg0) {
			return false;
		}

		@Override
		public boolean shouldSkipField(FieldAttributes arg0) {
			// TODO Auto-generated method stub
			return false;
		}
	}
	

	public static <T> T fromJson(final String requestJson, Class<T> classOfT)
			throws Exception {

		T reqObj = null;
		try {
			reqObj = json.fromJson(requestJson, classOfT);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		if (null == reqObj) {
			throw new Exception("Empty object : " + requestJson);
		}
		return reqObj;
	}

}
