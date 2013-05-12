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

import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple3;

import com.google.common.collect.Maps;

/**
 *
 */
public class ScoreFn extends
    DoFn<Pair<String, Iterable<Pair<String, Double>>>, Tuple3<String, String, Double>> {
  @Override
  public void process(Pair<String, Iterable<Pair<String, Double>>> in,
      Emitter<Tuple3<String, String, Double>> emitter) {
    String identifier = in.first();
    Map<String, Double> scores = Maps.newHashMap();
    Map<String, Map<String, Double>> pairScores = Maps.newHashMap();
    for (Pair<String, Double> p : in.second()) {
      if (p.first().contains(",")) {
        String[] pieces = p.first().split(",");
        
        Map<String, Double> first = pairScores.get(pieces[0]);
        if (first == null) {
          first = Maps.newHashMap();
          pairScores.put(pieces[0], first);
        }
        first.put(pieces[1], p.second());
        
        Map<String, Double> second = pairScores.get(pieces[1]);
        if (second == null) {
          second = Maps.newHashMap();
          pairScores.put(pieces[1], second);
        }
        second.put(pieces[0], p.second());
        
      } else {
        scores.put(p.first(), p.second());
      }
    }
    
    for (Map.Entry<String, Double> e : scores.entrySet()) {
      double score = e.getValue();
      double scaledSums = 0;
      Map<String, Double> subScores = pairScores.get(e.getKey());
      if (subScores != null) {
        for (Map.Entry<String, Double> f : subScores.entrySet()) {
          scaledSums += f.getValue() - score - scores.get(f.getKey());
        }
        score += scaledSums / (2 * subScores.size());
      }
      emitter.emit(Tuple3.of(identifier, e.getKey(), score));
    }
  }

}
