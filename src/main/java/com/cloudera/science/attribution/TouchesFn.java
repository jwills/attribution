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

import java.util.Collection;
import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 *
 */
public class TouchesFn extends
    DoFn<Pair<Boolean, Collection<String>>, Pair<String, Pair<Long, Long>>> {

  private static final Joiner JOINER = Joiner.on(',');
  
  @Override
  public void process(Pair<Boolean, Collection<String>> input,
      Emitter<Pair<String, Pair<Long, Long>>> emitter) {
    boolean hasEvents = input.first();
    Pair<Long, Long> out = hasEvents ? Pair.of(1L, 0L) : Pair.of(0L, 1L);
    List<String> touches = Lists.newArrayList(input.second());
    for (String touch : touches) {
      emitter.emit(Pair.of(touch, out));
    }
    
    for (int i = 0; i < touches.size(); i++) {
      for (int j = i + 1; j < touches.size(); j++) {
        emitter.emit(Pair.of(JOINER.join(touches.get(i), touches.get(j)), out));
      }
    }
  }
}
