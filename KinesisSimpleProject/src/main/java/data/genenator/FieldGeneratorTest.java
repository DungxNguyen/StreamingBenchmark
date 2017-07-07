package data.genenator;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FieldGeneratorTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(FieldGeneratorTest.class);
	private static final ObjectMapper M_OBJECT_MAPPER = new ObjectMapper();
	private static final String FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE = "Field_Value_Combination_Distribution.json";

	public static void main(String[] args) throws IOException {
		Map<String, Double> combinationDistributionTable = M_OBJECT_MAPPER.readValue(new File(FieldGeneratorTest.class
				.getClassLoader().getResource(FIELD_VALUE_COMBINATION_DISTRIBUTION_FILE).getFile()), 
				new TypeReference<Map<String, Double>>(){});
		// Map<String, Double> combinationDistributionTable = new
		// HashMap<String, Double>();
		// combinationDistributionTable.put("{\"cat\":\"application\",
		// \"level\":\"info\"}", 0.4);
		// combinationDistributionTable.put("{\"cat\":\"communication\",
		// \"level\":\"warn\"}", 0.6);
		LOGGER.info(M_OBJECT_MAPPER.writeValueAsString(combinationDistributionTable));
		FieldGenerator mFieldGenerator = new FieldGenerator(combinationDistributionTable);
		Map<String, Integer> combinationCount = new HashMap<String, Integer>();

		for (int i = 0; i < 100000; i++) {
			Map<String, String> fieldValues = mFieldGenerator.genFieldValuePairs();
			// LOGGER.info(fieldValues.toString());
			String combination = M_OBJECT_MAPPER.writeValueAsString(fieldValues);
			combinationCount.put(combination, combinationCount.getOrDefault(combination, 0) + 1);
		}
		LOGGER.info(combinationCount.toString());
	}
}
