/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.attribution;

import static org.apache.crunch.fn.Aggregators.*;
import static org.apache.crunch.types.avro.Avros.*;

import java.util.Collection;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class MultiTouchAttributionModel extends CrunchTool {

  @Parameter(names = "--user-channels-path", required=true,
      description = "HDFS path to the CSV file of <UserID,ChannelID> values.")
  private String userChannelsPath;
  
  @Parameter(names = "--positive-users-path", required=true,
      description = "HDFS path to the file containing UserIDs that had a positive outcome.")
  private String positiveUsersPath;
  
  @Parameter(names = "--output-path", required=true,
      description = "HDFS path to write the <UserID,ChannelID,Score> CSV data to. Overwrites existing outputs.")
  private String outputPath;
  
  public MultiTouchAttributionModel(boolean inMemory) {
    super(inMemory);
  }
  
  @Override
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    if (args.length == 0 || (args.length == 1 && args[0].contains("help"))) {
      jc.usage();
      return 0;
    }
    
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      return 1;
    }
    
    PCollection<String> out = exec(read(from.textFile(userChannelsPath)),
        read(from.textFile(positiveUsersPath)));
    out.write(to.textFile(outputPath), WriteMode.OVERWRITE);
    
    done();
    return 0;
  }
  
  public PCollection<String> exec(
      PCollection<String> touches,
      PCollection<String> positives) throws Exception {
    
    PTable<String, String> touchesTbl = touches
        .parallelDo(new SplitLineFn(), tableOf(strings(), strings()));
    
    PTable<String, Boolean> positivesTbl = positives
        .parallelDo(new AsTableFn(), tableOf(strings(), booleans()));
    
    PTable<String, Pair<Boolean, Collection<String>>> cogrouped = positivesTbl.cogroup(touchesTbl)
        .parallelDo(new SessionsFn(), tableOf(strings(), pairs(booleans(), collections(strings()))));
    cogrouped.materialize();
    
    PTable<String, Double> probs = cogrouped.values()
        .parallelDo(new TouchesFn(), tableOf(strings(), pairs(longs(), longs())))
        .groupByKey()
        .combineValues(pairAggregator(SUM_LONGS(), SUM_LONGS()))
        .parallelDo(new ValueRatiosFn(), tableOf(strings(), doubles()));
    probs.materialize();
    
    PTable<String, String> inverted = cogrouped.parallelDo(new UserLevelFn(),
        tableOf(strings(), strings()));
    
    PCollection<Tuple3<String, String, Double>> scores = probs.join(inverted)
        .parallelDo(new ReMapFn(), tableOf(strings(), pairs(strings(), doubles())))
        .groupByKey()
        .parallelDo(new ScoreFn(), triples(strings(), strings(), doubles()));
    
    return scores.parallelDo(new StringifyFn<Tuple3<String, String, Double>>(), strings());
  }

  static class StringifyFn<T extends Tuple> extends MapFn<T, String> {
    @Override
    public String map(T t) {
      StringBuilder sb = new StringBuilder().append(t.get(0));
      for (int i = 1; i < t.size(); i++) {
        sb.append(',').append(t.get(i));
      }
      return sb.toString();
    }
  }
  
  static class SplitLineFn extends MapFn<String, Pair<String, String>> {
    @Override
    public Pair<String, String> map(String input) {
      String[] pieces = input.split(",");
      return Pair.of(pieces[0], pieces[1]);
    }
  }
  
  static class AsTableFn extends MapFn<String, Pair<String, Boolean>> {
    @Override
    public Pair<String, Boolean> map(String input) {
      return Pair.of(input, true);
    }
  }
  
  static class ValueRatiosFn extends MapFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>> {
    @Override
    public Pair<String, Double> map(Pair<String, Pair<Long, Long>> in) {
      Pair<Long, Long> yesNo = in.second();
      double ratio = yesNo.first() / (yesNo.first().doubleValue() + yesNo.second().doubleValue());
      return Pair.of(in.first(), ratio);
    }
  }
  
  static class ReMapFn extends
      MapFn<Pair<String, Pair<Double, String>>, Pair<String, Pair<String, Double>>> {
    @Override
    public Pair<String, Pair<String, Double>> map(Pair<String, Pair<Double, String>> p) {
      return Pair.of(p.second().second(), Pair.of(p.first(), p.second().first()));
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int rc = ToolRunner.run(conf, new MultiTouchAttributionModel(false), args);
    System.exit(rc);
  }
}
