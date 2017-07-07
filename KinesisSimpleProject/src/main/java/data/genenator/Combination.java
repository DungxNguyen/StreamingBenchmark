package data.genenator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Combination {
	private static final ObjectMapper mObjectMapper = new ObjectMapper();
	
	public static Map<String, String> parse(String flatCombination){
		try {
			return mObjectMapper.readValue(flatCombination, HashMap.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new HashMap<String, String>();
	}

}
