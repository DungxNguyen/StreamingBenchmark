package data.genenator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldGenerator {
	private Map<String, Double> combinationDistributionTable;
	private Map<Integer, String> probabilityMap;
	private List<Double> probabilityPoint;
	private Random mRandom;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FieldGenerator.class);
	
	public FieldGenerator(Map<String, Double> combinationDistributionTable) {
		this.combinationDistributionTable = combinationDistributionTable;
		mRandom = new Random();
		probabilityPoint = new ArrayList<Double>();
		probabilityMap = new HashMap<Integer, String>();
		buildProbabilityMap();
		LOGGER.info("Distribution Table: " + combinationDistributionTable.toString());
		LOGGER.info("Distribution Map: " + probabilityMap.toString());
		LOGGER.info("Probability Point List: " + probabilityPoint.toString());
	}

	private void buildProbabilityMap() {
		int counter = 0;
		double currentProbabilityPoint = 0.0;
		probabilityPoint.add(counter, currentProbabilityPoint);
		for (String combination: combinationDistributionTable.keySet()){
			currentProbabilityPoint += combinationDistributionTable.get(combination);
			probabilityMap.put(counter, combination);
			probabilityPoint.add(++counter, currentProbabilityPoint);
		}
	}

	public Map<String, String> genFieldValuePairs() {
		return Combination.parse(probabilityMap.get(getRange(mRandom.nextDouble())));
	}

	private int getRange(double prob) {
		for (int i = 0; i < probabilityPoint.size() - 1; i++) {
			if (prob >= probabilityPoint.get(i) &&
					prob < probabilityPoint.get(i + 1)) {
				return i;
			}
		}
		return probabilityPoint.size() - 1;
	}
}
