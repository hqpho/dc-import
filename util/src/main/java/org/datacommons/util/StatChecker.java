// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.util;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Debug.DataPoint;
import org.datacommons.proto.Debug.DataPoint.DataValue;
import org.datacommons.proto.Debug.Log.Level;
import org.datacommons.proto.Debug.StatValidationResult;
import org.datacommons.proto.Debug.StatValidationResult.StatValidationEntry;
import org.datacommons.proto.LogLocation;
import org.datacommons.proto.LogLocation.Location;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.util.PlaceSeriesSummary.SeriesSummary;
import org.datacommons.util.SummaryReportGenerator.StatVarSummary;

// A checker that checks time-series for holes, variance in values, etc.
// This class is thread-safe.
// Concurrency of map objects are handled by:
// a. using a ConcurrentMap when methods that use the map are on a hot path and it is ok for access
//    to the map to be race-y (samplePlaces and svObsValues).
// b. making methods that access the map synchronized because when methods are not on a hot path and
//    it is not ok for access to the map to be race-y (seriesSummaryMap).
public class StatChecker {
  private static final int MAX_PLACES_FOR_TYPE_INFERRED_NS = 5;
  private static final int MAX_PLACES_FOR_TYPELESS_NS = 25;
  private static final double SMALL_NUMBER = 0.000001;
  private static final String RECEIVED_SAMPLE_PLACES_KEY = "Received_Sample_Places";
  private static final Character PLACE_NAMESPACE_DELIMITER = '/';
  private static final List<String> SVOBS_VALUE_KEY_PROPS =
      List.of(
          Vocabulary.OBSERVATION_ABOUT,
          Vocabulary.VARIABLE_MEASURED,
          Vocabulary.MEASUREMENT_METHOD,
          Vocabulary.OBSERVATION_PERIOD,
          Vocabulary.SCALING_FACTOR,
          Vocabulary.UNIT,
          Vocabulary.OBSERVATION_DATE);
  private static final List<String> COUNTER_KEYS =
      List.of(
          "StatsCheck_MultipleValueTypesInASeries",
          "StatsCheck_Inconsistent_Values",
          "StatsCheck_3_Sigma",
          "StatsCheck_MaxPercentFluctuationGreaterThan500",
          "StatsCheck_MaxPercentFluctuationGreaterThan100",
          "StatsCheck_Invalid_Date",
          "StatsCheck_Inconsistent_Date_Granularity",
          "StatsCheck_Data_Holes");
  private static final Set<String> TYPE_INFERRED_PLACE_NAMESPACES = Set.of("geoId", "nuts");
  private static final int NUM_SUMMARY_ENTRIES_PER_COUNTER = 10;
  private final LogWrapper logCtx;
  // key is place dcid
  private final Map<String, PlaceSeriesSummary> placeSeriesSummaryMap;
  private final Map<String, StatVarSummary> svSummaryMap;
  // key is generated by getSamplePlacesKey, value is set of place dcids
  private final ConcurrentMap<String, Set<String>> samplePlaces;
  private final boolean shouldGenerateSamplePlaces;
  // Tracks global state on StatVarObservations to detect whether there are multiple of the
  // same StatVarObservation with inconsistent values. The key is a hash made up of a set of
  // properties that distinguish a StatVarObservation and the value is the first value seen of that
  // StatVarObservation.
  private final ConcurrentMap<Long, Float> svObValues;
  private final String EMPTY_PROP_STRING = "EMPTY_PROP";
  private StatVarState statVarState;
  private ExistenceChecker existenceChecker;
  private boolean checkMeasurementResult;

  // Creates a StatChecker instance. If no samplePlaces are provided, for each pair of (place
  // namespace, place dcid length), we will use the first 5 places that stat var observations are
  // added for as the sample places.
  public StatChecker(
      LogWrapper logCtx,
      Set<String> samplePlaces,
      StatVarState statVarState,
      ExistenceChecker existenceChecker,
      boolean checkMeasurementResult) {
    this.logCtx = logCtx;
    this.placeSeriesSummaryMap = new HashMap<>();
    this.samplePlaces = new ConcurrentHashMap<>();
    this.svObValues = new ConcurrentHashMap<>();
    this.svSummaryMap = new HashMap<>();
    if (samplePlaces == null) {
      this.shouldGenerateSamplePlaces = true;
    } else {
      this.shouldGenerateSamplePlaces = false;
      this.samplePlaces.put(RECEIVED_SAMPLE_PLACES_KEY, samplePlaces);
    }
    this.statVarState = statVarState;
    this.existenceChecker = existenceChecker;
    this.checkMeasurementResult = checkMeasurementResult;
    if (this.checkMeasurementResult) {
      if (this.existenceChecker == null || this.statVarState == null) {
        throw new IllegalArgumentException(
            "ExistenceChecker and StatVarState objects must be non-null when checkMeasurementResult is true");
      }
    }
  }

  public StatChecker(LogWrapper logCtx, Set<String> samplePlaces) {
    this(logCtx, samplePlaces, null, null, false);
  }

  // Given a graph, extract stat var info and time series info (about the chosen sample places) from
  // the statVarObservation nodes.
  //
  // TODO (chejennifer): Look into optimizing this so that there can be less contention
  public synchronized void extractStatsFromGraph(McfGraph graph) {
    for (Map.Entry<String, McfGraph.PropertyValues> nodeEntry : graph.getNodesMap().entrySet()) {
      McfGraph.PropertyValues node = nodeEntry.getValue();
      // We will extract basic stat var information from every StatVarObservation nodes
      extractStatVarInfoFromNode(node);
      // We will only extract series information from StatVarObservation nodes about sample places
      String placeDcid = McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT);
      if (shouldExtractSeriesInfo(placeDcid)) {
        placeSeriesSummaryMap
            .computeIfAbsent(placeDcid, k -> new PlaceSeriesSummary())
            .extractSeriesFromNode(node);
      }
    }
  }

  // Given a graph, for each node that is a statVarObservation node with a number value, check for
  // any value inconsistencies. Return false if there are any svObsValueInconsistencies found.
  public boolean checkSvObsInGraph(McfGraph graph) {
    boolean success = true;
    for (Map.Entry<String, McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      if (McfUtil.isSvObWithNumberValue(node.getValue())) {
        success &= checkSvObsValueInconsistency(node.getValue());
      }
    }
    return success;
  }

  // Iterate through seriesSummaries, perform a list of checks (value inconsistencies, sigma
  // variance, percent fluctuations, holes in dates, invalid dates, etc) and add these results to
  // the logCtx.
  public synchronized void check() throws IOException, InterruptedException {
    Map<String, Integer> countersRemaining = new HashMap<>();
    for (String counterKey : COUNTER_KEYS) {
      countersRemaining.put(counterKey, NUM_SUMMARY_ENTRIES_PER_COUNTER);
    }
    for (PlaceSeriesSummary placeSeriesSummary : placeSeriesSummaryMap.values()) {
      for (Map<Long, SeriesSummary> seriesSummaryMap :
          placeSeriesSummary.getSvSeriesSummaryMap().values()) {
        for (SeriesSummary seriesSummary : seriesSummaryMap.values()) {
          String svDcid = seriesSummary.getValidationResult().getStatVarDcid();
          List<DataPoint> timeSeries = seriesSummary.getTimeSeriesAsList();

          StatValidationResult.Builder resBuilder = seriesSummary.validationResult;

          // General checks; these don't depend on the type of the values.

          // Check that the ValueType of all values are the same
          checkSeriesTypeInconsistencies(timeSeries, resBuilder, logCtx);
          // Check inconsistent values (sawtooth).
          checkSeriesValueInconsistencies(timeSeries, resBuilder, logCtx);
          // Check for holes in dates, invalid dates, etc.
          checkDates(timeSeries, resBuilder, logCtx);

          ValueType type = seriesSummary.getValueType();

          if (type == ValueType.NUMBER) {
            // Check N-Sigma variance.
            checkSigmaDivergence(timeSeries, resBuilder, logCtx);
            // Check N-Percent fluctuations.
            checkPercentFluctuations(timeSeries, resBuilder, logCtx);
          }

          if (type == ValueType.RESOLVED_REF && checkMeasurementResult) {
            checkMeasurementResult(svDcid, timeSeries);
          }

          // add result to log.
          if (!resBuilder.getValidationCountersList().isEmpty() && !countersRemaining.isEmpty()) {
            // only add entry if resBuilder contains a validation counter that we still want to add
            // entries for.
            boolean shouldAddEntry = false;
            for (StatValidationEntry entry : resBuilder.getValidationCountersList()) {
              String counterKey = entry.getCounterKey();
              if (countersRemaining.containsKey(counterKey)) {
                shouldAddEntry = true;
                countersRemaining.compute(counterKey, (k, v) -> v != null ? v - 1 : 0);
                if (countersRemaining.get(counterKey) < 1) countersRemaining.remove(counterKey);
              }
            }
            if (shouldAddEntry) logCtx.addStatsCheckSummaryEntry(resBuilder.build());
          }
        }
      }
    }
    // Drain the existence check calls that potentially have been submitted by
    // calls to checkMeasurementResult.
    if (existenceChecker != null && checkMeasurementResult) {
      existenceChecker.drainRemoteCalls();
    }
  }

  // This feature is not within check() because it uses ExistenceChecker instead
  // of using the StatChecker.check() mechanism of counter keys.
  //
  // It still lives in this file, at least temporarily, because we are performing
  // measurementResult checks on only sample places right now. That is because
  // the current implementation synchronously fetches the statTypes of unknown
  // StatVars from the API, and we don't want to pay that performance cost for
  // all nodes.
  public synchronized void checkMeasurementResult(String svDcid, List<DataPoint> timeSeries)
      throws IOException, InterruptedException {

    String statType = statVarState.getStatType(svDcid);

    // If StatType is null, we were not able to determine the statType of SV
    // from the local cache OR the API. Log an error and continue to next SV.
    if (statType == null) {
      List<Location> locations = new ArrayList<Location>();
      if (timeSeries != null) {
        locations = timeSeries.get(0).getValues(0).getLocationsList();
      }
      logCtx.addEntry(
          Level.LEVEL_ERROR,
          "Existence_CheckMeasurementResult_StatTypeUnknown",
          "Could not find the statType of a StatisticalVariable to determine if it is subject to measurementResult checks :: "
              + "node: '"
              + svDcid
              + "'",
          locations);
      return;
    }

    // Only perform checks when statType is measurementResult
    if (!statType.equals(Vocabulary.MEASUREMENT_RESULT)) {
      return;
    }

    for (DataPoint dp : timeSeries) {
      String value = SeriesSummary.getValueOfDataPoint(dp);
      LogLocation.Location location = dp.getValues(0).getLocations(0);
      String fileName = location.getFile();
      long lineNumber = location.getLineNumber();

      LogCb logCb =
          new LogCb(logCtx, Debug.Log.Level.LEVEL_ERROR, fileName, lineNumber)
              .setDetail(LogCb.PREF_KEY, Vocabulary.MEASUREMENT_RESULT)
              .setDetail(LogCb.VALUE_KEY, value)
              .setCounterSuffix("value_StatType_measurementResult");

      existenceChecker.submitNodeCheck(value, logCb);
    }
  }

  public Map<String, PlaceSeriesSummary> getPlaceSeriesSummaryMap() {
    return this.placeSeriesSummaryMap;
  }

  public Map<String, StatVarSummary> getSVSummaryMap() {
    return this.svSummaryMap;
  }

  // Only extract series information for a statVarObservation node that is about a sample place.
  public boolean shouldExtractSeriesInfo(String placeDcid) {
    if (placeDcid.isEmpty()) return false;

    String placekey = getSamplePlacesKey(placeDcid);

    // If shouldGenerateSamplePlaces is true, and the bucket that the placekey
    // belongs has empty space, the sample place will be added to the
    // samplePlaces map
    if (shouldGenerateSamplePlaces) {
      samplePlaces.computeIfAbsent(placekey, k -> new HashSet<>());

      // If the the bucket has space, add the sample place
      if (samplePlacesBucketHasSpace(placeDcid, placekey)) {
        samplePlaces.get(placekey).add(placeDcid);
      }
    }

    return samplePlaces.get(placekey).contains(placeDcid);
  }

  // Returns true if the bucket for this places key is not full.
  private boolean samplePlacesBucketHasSpace(String placeDcid, String placekey) {
    boolean bucketNotFull = samplePlaces.get(placekey).size() < getSamplePlaceBucketSize(placeDcid);
    return bucketNotFull;
  }

  // Given a place dcid, returns the namespace of the place
  private String getNamespaceOfPlaceName(String placeDcid) {
    String namespace = "";

    int namespaceSplit = placeDcid.indexOf(PLACE_NAMESPACE_DELIMITER);
    if (namespaceSplit >= 0) {
      namespace = placeDcid.substring(0, namespaceSplit);
    }
    return namespace;
  }

  // Given a place dcid, returns whether we want to bucket it by its length
  private boolean isPlaceBucketedByLength(String placeDcid) {
    String namespace = getNamespaceOfPlaceName(placeDcid);
    return TYPE_INFERRED_PLACE_NAMESPACES.contains(namespace);
  }

  // Given a place dcid, returns the key in the samplePlaces map
  // if should not generate sample places, simply returns RECEIVED_SAMPLE_PLACES_KEY
  // otherwise, checks if it is to be bucketed by length and returns
  // the appropriate samplePlaces map key.
  private String getSamplePlacesKey(String placeDcid) {
    if (shouldGenerateSamplePlaces) {
      boolean bucketByLength = isPlaceBucketedByLength(placeDcid);
      String placeNamespace = getNamespaceOfPlaceName(placeDcid);

      if (bucketByLength) {
        return placeNamespace + placeDcid.length();
      } else {
        return placeNamespace;
      }
    } else {
      return RECEIVED_SAMPLE_PLACES_KEY;
    }
  }

  // Given a place dcid, returns the size of the bucket it should hold in
  // the samplePlaces map.
  private int getSamplePlaceBucketSize(String placeDcid) {
    boolean bucketByLength = isPlaceBucketedByLength(placeDcid);
    if (bucketByLength) {
      return MAX_PLACES_FOR_TYPE_INFERRED_NS;
    } else {
      return MAX_PLACES_FOR_TYPELESS_NS;
    }
  }

  protected static void checkSeriesTypeInconsistencies(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    StatValidationEntry.Builder inconsistentTypeCounter = StatValidationEntry.newBuilder();
    String counterKey = "StatsCheck_MultipleValueTypesInASeries";
    inconsistentTypeCounter.setCounterKey(counterKey);

    ValueType firstType = null;
    boolean foundInconsistencies = false;

    for (DataPoint dp : timeSeries) {
      for (DataValue val : dp.getValuesList()) {
        ValueType type = val.getValue().getType();
        if (firstType == null) {
          firstType = type;
          // we want a pair of points in the logged counter, so we always add
          // the first dp as a "problem point". however, this will only get logged
          // if foundInconsistencies is set to True
          inconsistentTypeCounter.addProblemPoints(dp);
        }
        if (!firstType.equals(type)) {
          inconsistentTypeCounter.addProblemPoints(dp);
          foundInconsistencies = true;
          logCtx.incrementWarningCounterBy(counterKey, 1);
        }
      }
    }
    if (foundInconsistencies) {
      resBuilder.addValidationCounters(inconsistentTypeCounter.build());
    }
  }

  protected static void checkSeriesValueInconsistencies(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    StatValidationEntry.Builder inconsistentValueCounter = StatValidationEntry.newBuilder();
    String counterKey = "StatsCheck_Inconsistent_Values";
    inconsistentValueCounter.setCounterKey(counterKey);
    for (DataPoint dp : timeSeries) {
      String v = null;
      boolean vInitialized = false;
      for (DataValue val : dp.getValuesList()) {
        if (vInitialized && !val.getValue().getValue().equals(v)) {
          inconsistentValueCounter.addProblemPoints(dp);
          logCtx.incrementWarningCounterBy(counterKey, 1);
          // Once we log this DataPoint, iterating on any further values would
          // duplicate the log incorrectly, so we break out of the loop and
          // move to the next DataPoint in the timeSeries
          break;
        }
        vInitialized = true;
        v = val.getValue().getValue();
      }
    }
    if (!inconsistentValueCounter.getProblemPointsList().isEmpty()) {
      resBuilder.addValidationCounters(inconsistentValueCounter.build());
    }
  }

  protected static void checkSigmaDivergence(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    MeanAndStdDev meanAndStdDev = getStats(timeSeries);
    if (meanAndStdDev.stdDev == 0) {
      return;
    }
    String sigma3CounterKey = "StatsCheck_3_Sigma";
    StatValidationEntry.Builder sigma3Counter = StatValidationEntry.newBuilder();
    sigma3Counter.setCounterKey(sigma3CounterKey);
    // Only add data points to the counter of the greatest standard deviation that it belongs to.
    // ie. if the data point is beyond 3 std deviation, only add it to that counter.
    for (DataPoint dp : timeSeries) {
      if (SeriesSummary.getTypeOfDataPoint(dp) != ValueType.NUMBER) continue;
      double val = SeriesSummary.getValueOfDataPointAsNumber(dp);
      if (Math.abs(val - meanAndStdDev.mean) > 3 * meanAndStdDev.stdDev) {
        sigma3Counter.addProblemPoints(dp);
        logCtx.incrementWarningCounterBy(sigma3CounterKey, 1);
      }
    }
    if (!sigma3Counter.getProblemPointsList().isEmpty()) {
      resBuilder.addValidationCounters(sigma3Counter.build());
    }
  }

  private static class MeanAndStdDev {
    double mean = 0;
    double stdDev = 0;
  }

  private static MeanAndStdDev getStats(List<DataPoint> timeSeries) {
    MeanAndStdDev result = new MeanAndStdDev();
    if (timeSeries.size() < 2) return result;
    double weights = 0;
    double sum = 0;
    double sumSqDev = 0;
    for (DataPoint dp : timeSeries) {
      if (SeriesSummary.getTypeOfDataPoint(dp) != ValueType.NUMBER) continue;
      double val = SeriesSummary.getValueOfDataPointAsNumber(dp);
      if (weights > 0) {
        sumSqDev += 1 * weights / 1 / (weights + 1) * Math.pow((1 / weights * sum - val), 2);
      }
      weights++;
      sum += val;
    }
    if (weights > 0) {
      result.stdDev = Math.sqrt(sumSqDev * (1.0 / weights));
      result.mean = sum * (1.0 / weights);
    }
    return result;
  }

  // Goes through sorted (but possibly discontinuous) time series, saving the largest fluctuation
  // for each bucket of fluctuations >100, and >500.
  protected static void checkPercentFluctuations(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    double maxDelta = 0;
    DataPoint maxDeltaDP = null;
    DataPoint maxDeltaBaseDP = null;
    DataPoint baseDataPoint = null;
    for (DataPoint dp : timeSeries) {
      // Don't try to compare between times because this is a Sawtooth
      if (dp.getValuesCount() > 1) return;
      if (dp.getValuesCount() == 0) continue;
      if (SeriesSummary.getTypeOfDataPoint(dp) != ValueType.NUMBER) continue;
      double currVal = SeriesSummary.getValueOfDataPointAsNumber(dp);
      if (baseDataPoint != null
          && SeriesSummary.getTypeOfDataPoint(baseDataPoint) == ValueType.NUMBER) {
        double currDelta;
        double baseVal = SeriesSummary.getValueOfDataPointAsNumber(baseDataPoint);
        if (baseVal == 0) {
          currDelta = (currVal) / SMALL_NUMBER;
        } else {
          currDelta = (currVal - baseVal) / Math.abs(baseVal);
        }
        if (Math.abs(maxDelta) < Math.abs(currDelta)) {
          maxDelta = currDelta;
          maxDeltaDP = dp;
          maxDeltaBaseDP = baseDataPoint;
        }
      }
      baseDataPoint = dp;
    }
    String counterKey = "";
    if (Math.abs(maxDelta) > 5) {
      counterKey = "StatsCheck_MaxPercentFluctuationGreaterThan500";
    } else if (Math.abs(maxDelta) > 1) {
      counterKey = "StatsCheck_MaxPercentFluctuationGreaterThan100";
    }
    if (counterKey.isEmpty()) return;
    StatValidationEntry.Builder maxPercentFluctuationCounter = StatValidationEntry.newBuilder();
    maxPercentFluctuationCounter.setCounterKey(counterKey);
    maxPercentFluctuationCounter.addProblemPoints(maxDeltaBaseDP);
    maxPercentFluctuationCounter.addProblemPoints(maxDeltaDP);
    // We want to format the percentDifference as the maxDelta multiplied by 100 to get the percent
    // and truncated to 2 decimal points.
    maxPercentFluctuationCounter.setPercentDifference(Math.round(maxDelta * 10000) / 100.0);
    resBuilder.addValidationCounters(maxPercentFluctuationCounter.build());
    logCtx.incrementWarningCounterBy(counterKey, 1);
  }

  // Check if there are holes in the dates by inferring based on whether the successive dates have
  // equal duration. If the sizes of the date strings don't match or there are invalid dates, don't
  // try to infer holes and instead mark as "series_inconsistent_date_granularity" and
  // "series_invalid_date".
  protected static void checkDates(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    Set<LocalDateTime> dateTimes = new TreeSet<>();
    StatValidationEntry.Builder invalidDateCounter = StatValidationEntry.newBuilder();
    String invalidDateCounterKey = "StatsCheck_Invalid_Date";
    invalidDateCounter.setCounterKey(invalidDateCounterKey);
    // To keep track of the different lengths of the date strings.
    Map<Integer, List<DataPoint>> dateLen = new HashMap<>();

    // In the first pass, get sorted dates in LocalDateTime form and check for invalid dates and
    // inconsistent date granularities.
    for (DataPoint dp : timeSeries) {
      String date = dp.getDate();
      LocalDateTime dateTime = StringUtil.getValidISO8601Date(date);
      if (dateTime == null) {
        invalidDateCounter.addProblemPoints(dp);
        logCtx.incrementWarningCounterBy(invalidDateCounterKey, 1);
        continue;
      }
      if (!dateLen.containsKey(date.length())) {
        dateLen.put(date.length(), new ArrayList<>());
      }
      dateLen.get(date.length()).add(dp);
      dateTimes.add(dateTime);
    }
    List<Integer> dateLenList = new ArrayList<>(dateLen.keySet());
    if (dateLenList.size() > 1) {
      StatValidationEntry.Builder inconsistentDateCounter = StatValidationEntry.newBuilder();
      String inconsistentDateCounterKey = "StatsCheck_Inconsistent_Date_Granularity";
      inconsistentDateCounter.setCounterKey(inconsistentDateCounterKey);
      // When there are multiple date granularity, the problem points will be those that are of a
      // different date granularity than the most common one.
      dateLenList.sort((d1, d2) -> dateLen.get(d2).size() - dateLen.get(d1).size());
      for (int i = 1; i < dateLenList.size(); i++) {
        dateLen.get(dateLenList.get(i)).forEach(inconsistentDateCounter::addProblemPoints);
      }
      // Increment counter for each series where there is an inconsistent date problem.
      logCtx.incrementWarningCounterBy(inconsistentDateCounterKey, 1);
      resBuilder.addValidationCounters(inconsistentDateCounter.build());
      return;
    }
    if (!invalidDateCounter.getProblemPointsList().isEmpty()) {
      resBuilder.addValidationCounters(invalidDateCounter.build());
      return;
    }
    long window = -1;
    LocalDateTime prev = null;
    List<LocalDateTime> dateTimesList = new ArrayList<>(dateTimes);

    // In this second pass, compute the data holes.
    // Date arithmetic is complicated by leap year considerations. For now assume
    // only month boundary.
    // TODO: Handle days granularity
    for (LocalDateTime dt : dateTimesList) {
      if (prev != null) {
        long delta = ChronoUnit.MONTHS.between(prev, dt);
        if (window >= 0 && window != delta) {
          StatValidationEntry.Builder dataHoleCounter = StatValidationEntry.newBuilder();
          String dataHoleCounterKey = "StatsCheck_Data_Holes";
          dataHoleCounter.setCounterKey(dataHoleCounterKey);
          List<String> dateList = new ArrayList<>();
          for (DataPoint dp : timeSeries) {
            dateList.add(dp.getDate());
          }
          dataHoleCounter.setAdditionalDetails(
              "Possible data hole found. Dates in this series: " + String.join(", ", dateList));
          logCtx.incrementWarningCounterBy(dataHoleCounterKey, 1);
          resBuilder.addValidationCounters(dataHoleCounter.build());
          return;
        }
        window = delta;
      }
      prev = dt;
    }
  }

  // Return false if we have already seen the same StatVarObservation node but with a different
  // value.
  private boolean checkSvObsValueInconsistency(McfGraph.PropertyValues node) {
    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (String prop : SVOBS_VALUE_KEY_PROPS) {
      String val = McfUtil.getPropVal(node, prop);
      if (prop.isEmpty()) {
        hasher.putString(EMPTY_PROP_STRING, StandardCharsets.UTF_8);
      } else {
        hasher.putString(val, StandardCharsets.UTF_8).putInt(val.length());
      }
    }
    Long fp = hasher.hash().asLong();
    Float val = null;
    try {
      val = Float.parseFloat(McfUtil.getPropVal(node, Vocabulary.VALUE));
    } catch (NumberFormatException e) {
      // If value is not a float, val will stay as null and this will be handled later.
    }
    if (this.svObValues.containsKey(fp) && !this.svObValues.get(fp).equals(val)) {
      logCtx.addEntry(
          Level.LEVEL_ERROR,
          "Sanity_InconsistentSvObsValues",
          "Found nodes with different values for the same StatVarObservation :: observationAbout: '"
              + McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT)
              + "', variableMeasured: '"
              + McfUtil.getPropVal(node, Vocabulary.VARIABLE_MEASURED)
              + "', observationDate: '"
              + McfUtil.getPropVal(node, Vocabulary.OBSERVATION_DATE)
              + "', value1: "
              + this.svObValues.get(fp)
              + ", value2: "
              + val,
          node.getLocationsList());
      return false;
    } else {
      this.svObValues.put(fp, val);
      return true;
    }
  }

  public List<String> getSamplePlaces() {
    List<String> places = new ArrayList<String>();

    for (Set<String> placesOfType : samplePlaces.values()) {
      places.addAll(placesOfType);
    }

    return places;
  }

  public void setNameForSamplePlace(String place_dcid, String name) {
    if (placeSeriesSummaryMap.containsKey(place_dcid)) {
      placeSeriesSummaryMap.get(place_dcid).setPlaceName(name);
    }
  }

  private synchronized void extractStatVarInfoFromNode(McfGraph.PropertyValues node) {
    // TODO (chejennifer): extract prop value into a struct and pass around instead of looking it up
    // in multiple places
    String svDcid = McfUtil.getPropVal(node, Vocabulary.VARIABLE_MEASURED);
    if (svDcid.isEmpty()) return;
    StatVarSummary svMap = svSummaryMap.computeIfAbsent(svDcid, k -> new StatVarSummary());
    svMap.numObservations++;
    try {
      double value = Double.parseDouble(McfUtil.getPropVal(node, Vocabulary.VALUE));
      svMap.minValue = Math.min(svMap.minValue, value);
      svMap.maxValue = Math.max(svMap.maxValue, value);
    } catch (NumberFormatException e) {
      // Ignore if the value is not a number.
    }
    svMap.dates.add(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_DATE));
    svMap.places.add(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT));
    svMap.mMethods.add(McfUtil.getPropVal(node, Vocabulary.MEASUREMENT_METHOD));
    svMap.units.add(McfUtil.getPropVal(node, Vocabulary.UNIT));
    svMap.scalingFactors.add(McfUtil.getPropVal(node, Vocabulary.SCALING_FACTOR));
    svMap.observationPeriods.add(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_PERIOD));
  }

  // Makes API requests to get the names of the sample places from the DC
  // API, and puts that information to StatChecker.
  public void fetchSamplePlaceNames(HttpClient httpClient) {
    try {
      JsonObject apiResponse = ApiHelper.fetchPropertyValues(httpClient, getSamplePlaces(), "name");
      if (apiResponse != null) {
        for (var entry : apiResponse.entrySet()) {
          String placeDcid = entry.getKey();
          JsonObject nodeJson = entry.getValue().getAsJsonObject();

          if (nodeJson.has("out")) {
            JsonArray receivedNamesForPlace = nodeJson.getAsJsonArray("out");
            // in case there are multiple names, simply use the first.
            // for example, Ivory Coast has multiple names:
            // https://datacommons.org/browser/country/CIV
            String placeName =
                receivedNamesForPlace.get(0).getAsJsonObject().get("value").getAsString();
            this.setNameForSamplePlace(placeDcid, placeName);
          }
        }
      } else {
        logCtx.incrementWarningCounterBy("API_EmptyDcCallResponse", 1);
      }
    } catch (IOException | InterruptedException e) {
      logCtx.incrementWarningCounterBy("API_FailedDcCall", 1);
    }
  }
}
