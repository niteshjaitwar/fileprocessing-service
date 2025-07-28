package com.adp.esi.digitech.file.processing.generator.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.file.processing.ds.config.model.DataAggregation;
import com.adp.esi.digitech.file.processing.exception.GenerationException;
import com.adp.esi.digitech.file.processing.model.Row;


@Service("pivotGeneratorService")
public class PivotGeneratorService {

	private static final String GROUP_SEPERATOR = "#####";

	public String[][] generate(JSONObject sheetRule, List<Row> rows) throws GenerationException {

		var pivotRule = sheetRule.getJSONObject("pivot");

		if (!pivotRule.has("rows") && pivotRule.isNull("rows"))
			return null;

		var sheetHeaderMap = new HashMap<String, String>();
		var sheetHeaderJSONArray = sheetRule.getJSONArray("headers");

		sheetHeaderJSONArray.forEach(item -> {
			var headerJson = (JSONObject) item;
			var headerName = headerJson.getString("name");
			var headerValue = headerJson.getString("field");
			sheetHeaderMap.put(headerValue, headerName);
		});


		var rowGroupIdentifiers = pivotRule.getJSONArray("rows").toList().stream().map(Object::toString)
				.collect(Collectors.toList());

		var rowGroupIdentifierNames = rowGroupIdentifiers.stream().map(uuid -> sheetHeaderMap.get(uuid))
				.collect(Collectors.toList());

		var columnGroupIdentifiers = pivotRule.getJSONArray("columns").toList().stream().map(Object::toString)
				.collect(Collectors.toList());

		Function<Row, String> rowGroupClassifier = getIndexGroupClassifier(rowGroupIdentifiers);
		Function<Row, String> columnGroupClassifier = getColumnGroupClassifier(columnGroupIdentifiers.get(0));

		var valuesJSONArray = pivotRule.getJSONArray("values");
		var valueJson = valuesJSONArray.getJSONObject(0);

		var column = valueJson.getString("value");
		var aggregator = valueJson.getString("operation");

		var aggregation = new DataAggregation();
		aggregation.setColumn(column);
		aggregation.setAggregator(aggregator.toLowerCase().trim());

		Map<String, List<Row>> indexedGroupedRows = rows.stream().collect(Collectors.groupingBy(rowGroupClassifier));

		Map<String, Map<String, Double>> finalData = indexedGroupedRows.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey,
						indexedEntry -> indexedEntry.getValue().stream()
								.collect(Collectors.groupingBy(columnGroupClassifier)).entrySet().stream()
								.collect(Collectors.toMap(Map.Entry::getKey,
										columnEentry -> RoundOff(summarize(columnEentry.getValue(), aggregation)),
										(oldData, newData) -> newData, HashMap<String, Double>::new))));

		Map<String, Double> columnsGrandTotal = finalData.values().stream().flatMap(map -> map.entrySet().stream())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Double::sum));

		AtomicInteger counter = new AtomicInteger(rowGroupIdentifierNames.size());

		LinkedHashMap<Integer, String> baseUniqueColumns = rows.stream().map(columnGroupClassifier).distinct().sorted()
				.collect(Collectors.toMap(dataColumns -> counter.getAndIncrement(), Function.identity(),
						(oldData, newData) -> newData, LinkedHashMap<Integer, String>::new));

		var headers = new ArrayList<String>();
		headers.addAll(rowGroupIdentifierNames);
		headers.addAll(baseUniqueColumns.values());

		var headersArray = headers.toArray(new String[headers.size()]);

		String[][] output = new String[finalData.size() + 2][headersArray.length];
		AtomicInteger i = new AtomicInteger(0);
		
		//Setting Headers
		output[i.getAndIncrement()] = headersArray;
		
		//Setting Aggregated Values
		finalData.forEach((key, columnValues) -> {
			int j = 0;
			var data = new String[headersArray.length];
			data[j] = key;
			if(rowGroupIdentifierNames.size() > 1) {
				String [] splitData = key.split(GROUP_SEPERATOR, -1);
				while(rowGroupIdentifierNames.size() > j) {
					if(splitData.length > j)
						data[j] = splitData[j];
					
					j++;
				}
			}
			baseUniqueColumns.keySet().forEach(index -> {			
				var value = columnValues.getOrDefault(baseUniqueColumns.get(index), null);
				if(value != null) {
					var celldata = String.format("%.2f", value);
					celldata = celldata.contains(".") ? celldata.replaceAll("0*$", "").replaceAll("\\.$", "") : celldata;
					data[index] = celldata;
				}
			});			
			output[i.getAndIncrement()] = data;
		});
		
		//Setting Grand Totals
		var totals = new String[headersArray.length];
		totals[0] = "Grand Total";
		
		baseUniqueColumns.keySet().forEach(index -> {			
			var value = RoundOff(columnsGrandTotal.getOrDefault(baseUniqueColumns.get(index), null));
			if(value != null) {
				var celldata = String.format("%.2f", value);
				celldata = celldata.contains(".") ? celldata.replaceAll("0*$", "").replaceAll("\\.$", "") : celldata;
				totals[index] = celldata;
			}
		});	
		output[i.getAndIncrement()] = totals;
		
		return output;
	}

	private Function<Row, String> getIndexGroupClassifier(List<String> items) {
		return row -> items.stream().map(item -> row.getColumns().get(UUID.fromString(item)).getTargetValue())
				.collect(Collectors.joining(GROUP_SEPERATOR));
	}

	private Function<Row, String> getColumnGroupClassifier(String column) {
		return row -> row.getColumns().get(UUID.fromString(column)).getTargetValue();
	}

	private Double RoundOff(Double output) {
		return output != null ? new BigDecimal(output).setScale(2, RoundingMode.HALF_UP).doubleValue(): null;
	}
	private Double summarize(List<Row> rows, DataAggregation aggregation) {
		var column = aggregation.getColumn();
		BiFunction<Row, String, Object> sourceDataFunction = (row, uuid) -> row.getColumns().get(UUID.fromString(uuid))
				.getValue();

		Function<List<Row>, DoubleStream> rowsFilterFunction = (rowsData) -> rowsData.stream()
				.map(row -> sourceDataFunction.apply(row, column))
				.filter(data -> Objects.nonNull(data) && NumberUtils.isParsable(data.toString()))
				.mapToDouble(data -> Double.valueOf(data.toString()));

		return switch (aggregation.getAggregator()) {
		case "sum" -> rowsFilterFunction.apply(rows).sum();
		case "avg" -> rowsFilterFunction.apply(rows).average().getAsDouble();
		case "max" -> rowsFilterFunction.apply(rows).max().getAsDouble();
		case "min" -> rowsFilterFunction.apply(rows).min().getAsDouble();
		case "count" -> Double.valueOf(rows.size());
		default -> null;
		};

	}

}
