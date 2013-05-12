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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;

/**
 *
 */
public class UserLevelFn extends
    DoFn<Pair<String, Pair<Boolean, Collection<String>>>, Pair<String, String>> {

  private static final Joiner JOINER = Joiner.on(',');
  
  @Override
  public void process(Pair<String, Pair<Boolean, Collection<String>>> in,
      Emitter<Pair<String, String>> emitter) {
    if (in.second().first()) {
      String id = in.first();
      List<String> touches = Lists.newArrayList();
      for (String touch : in.second().second()) {
        emitter.emit(Pair.of(touch, id));
        touches.add(touch);
      }
      
      for (int i = 0; i < touches.size(); i++) {
        for (int j = i + 1; j < touches.size(); j++) {
          emitter.emit(Pair.of(JOINER.join(touches.get(i), touches.get(j)), id));
        }
      }
    }
  }

}
