/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.blur.shell;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.SortField;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class QueryCommandHelper {

  public static final String QUERY = "query";
  public static final String WIDTH = "width";
  public static final String SORT = "sort";
  public static final String FACET = "facet";
  public static final String ROW_ID = "rowId";
  public static final String MINIMUM_NUMBER_OF_RESULTS = "min";
  public static final String MAX_QUERY_TIME = "max";
  public static final String FETCH = "fetch";
  public static final String START = "start";
  public static final String DISABLE_ROW_QUERY = "disableRowQuery";
  public static final String SCORE_TYPE = "scoreType";
  public static final String RECORD_FILTER = "recordFilter";
  public static final String ROW_FILTER = "rowFilter";

  @SuppressWarnings("unchecked")
  public static BlurQuery getBlurQuery(CommandLine commandLine) {
    Option[] options = commandLine.getOptions();
    Query query = new Query();
    if (commandLine.hasOption(QUERY)) {
      String queryString = join(Arrays.asList(commandLine.getOptionValues(QUERY)), " ");
      query.setQuery(queryString);
    }
    if (commandLine.hasOption(DISABLE_ROW_QUERY)) {
      query.setRowQuery(false);
    }
    if (commandLine.hasOption(SCORE_TYPE)) {
      String scoreTypeStr = commandLine.getOptionValue(SCORE_TYPE);
      ScoreType scoreType = ScoreType.valueOf(scoreTypeStr.toUpperCase());
      query.setScoreType(scoreType);
    }
    if (commandLine.hasOption(RECORD_FILTER)) {
      String recordFilter = commandLine.getOptionValue(RECORD_FILTER);
      query.setRecordFilter(recordFilter);
    }
    if (commandLine.hasOption(ROW_FILTER)) {
      String rowFilter = commandLine.getOptionValue(ROW_FILTER);
      query.setRecordFilter(rowFilter);
    }

    // String recordFilter;
    // String rowFilter;
    // String rowId;
    // long start;
    // int fetch;
    // long maxQueryTime;
    // long minimumNumberOfResults;
    // List<Facet> facets;
    // List<SortField> sortFields;

    BlurQuery blurQuery = new BlurQuery();
    blurQuery.setQuery(query);
    Selector selector = new Selector(Main.selector);
    if (!query.isRowQuery()) {
      selector.setRecordOnly(true);
    }
    blurQuery.setSelector(selector);
    blurQuery.setCacheResult(false);
    blurQuery.setUseCacheIfPresent(false);

    if (commandLine.hasOption(START)) {
      String startStr = commandLine.getOptionValue(START);
      blurQuery.setStart(Long.parseLong(startStr));
    }
    if (commandLine.hasOption(FETCH)) {
      String fetchStr = commandLine.getOptionValue(FETCH);
      blurQuery.setFetch(Integer.parseInt(fetchStr));
    }
    if (commandLine.hasOption(MAX_QUERY_TIME)) {
      String maxQueryTimeStr = commandLine.getOptionValue(MAX_QUERY_TIME);
      blurQuery.setMaxQueryTime(Long.parseLong(maxQueryTimeStr));
    }
    if (commandLine.hasOption(MINIMUM_NUMBER_OF_RESULTS)) {
      String minNumbResultsStr = commandLine.getOptionValue(MINIMUM_NUMBER_OF_RESULTS);
      blurQuery.setMinimumNumberOfResults(Long.parseLong(minNumbResultsStr));
    }
    if (commandLine.hasOption(ROW_ID)) {
      String rowId = commandLine.getOptionValue(ROW_FILTER);
      blurQuery.setRowId(rowId);
    }
    List<Facet> facets = new ArrayList<Facet>();
    for (Option option : options) {
      if (option.getOpt().equals(FACET)) {
        List<String> valuesList = option.getValuesList();
        Facet facet = new Facet();
        facet.setQueryStr(join(valuesList, " "));
        facets.add(facet);
      }
    }
    if (!facets.isEmpty()) {
      blurQuery.setFacets(facets);
    }

    List<SortField> sortFields = new ArrayList<SortField>();
    for (Option option : options) {
      if (option.getOpt().equals(SORT)) {
        List<String> valuesList = option.getValuesList();
        if (valuesList.size() == 2) {
          sortFields.add(new SortField(valuesList.get(0), valuesList.get(1), false));
        } else if (valuesList.size() == 3) {
          sortFields.add(new SortField(valuesList.get(0), valuesList.get(1), Boolean.parseBoolean(valuesList.get(2))));
        } else {
          throw new RuntimeException("Sort take 2 or 3 parameters.");
        }
      }
    }
    if (!sortFields.isEmpty()) {
      blurQuery.setSortFields(sortFields);
    }

    if (Main.highlight) {
      blurQuery.getSelector().setHighlightOptions(new HighlightOptions());
    }

    return blurQuery;
  }

  private static String join(List<String> argList, String sep) {
    StringBuilder builder = new StringBuilder();
    for (String s : argList) {
      if (builder.length() != 0) {
        builder.append(sep);
      }
      builder.append(s);
    }
    return builder.toString();
  }

  @SuppressWarnings("static-access")
  public static CommandLine parse(String[] otherArgs, Writer out, String usage) {
    Options options = new Options();
    options.addOption(OptionBuilder.hasArgs().withDescription("* Query string.").isRequired().create(QUERY));
    options.addOption(OptionBuilder.withDescription("Disables row query. (Enabled by default)").create(
        DISABLE_ROW_QUERY));
    options.addOption(OptionBuilder.hasArg().withArgName(SCORE_TYPE).withDescription("Specify the scoring type.")
        .create(SCORE_TYPE));
    options.addOption(OptionBuilder.hasArgs().withArgName(ROW_FILTER).withDescription("Specify row filter.")
        .create(ROW_FILTER));
    options.addOption(OptionBuilder.hasArgs().withArgName(RECORD_FILTER).withDescription("Specify record filter.")
        .create(RECORD_FILTER));
    options.addOption(OptionBuilder.hasArg().withArgName(START)
        .withDescription("Specify the starting position (paging).").create(START));
    options.addOption(OptionBuilder.hasArg().withArgName(FETCH)
        .withDescription("Specify the number of elements to fetch in a single page.").create(FETCH));
    options.addOption(OptionBuilder.hasArg().withArgName(MAX_QUERY_TIME)
        .withDescription("Specify the maximum amount of time to allow query to execute.").create(MAX_QUERY_TIME));
    options.addOption(OptionBuilder.hasArg().withArgName(MINIMUM_NUMBER_OF_RESULTS)
        .withDescription("Specify the minimum number of results required before returning from query.")
        .create(MINIMUM_NUMBER_OF_RESULTS));
    options.addOption(OptionBuilder.hasArg().withArgName(ROW_ID)
        .withDescription("Specify the rowId to execute the query against (this reduces the spray to other shards).")
        .create(ROW_ID));
    options.addOption(OptionBuilder.withArgName(FACET).hasArgs()
        .withDescription("Specify facet to be executed with this query.").create(FACET));
    options.addOption(OptionBuilder.withArgName(SORT).hasArgs()
        .withDescription("Specify a sort to be applied to this query <family> <column> [<reverse>].").create(SORT));
    options.addOption(OptionBuilder.withDescription("Displays help for this command.").create("h"));
    options.addOption(OptionBuilder.withArgName(WIDTH).hasArgs()
        .withDescription("Specify max column width for display.").create(WIDTH));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(out, true);
        formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, usage, null, options, HelpFormatter.DEFAULT_LEFT_PAD,
            HelpFormatter.DEFAULT_DESC_PAD, null, false);
        return null;
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, usage, null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }
}
