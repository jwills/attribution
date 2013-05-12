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

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 *
 */
public class SessionsFn extends
    MapFn<Pair<String, Pair<Collection<Boolean>, Collection<String>>>,
    Pair<String, Pair<Boolean, Collection<String>>>> {
  @Override
  public Pair<String, Pair<Boolean, Collection<String>>> map(
      Pair<String, Pair<Collection<Boolean>, Collection<String>>> in) {
    boolean hasEvent = !in.second().first().isEmpty();
    Collection<String> unique = Lists.newArrayList(Sets.newTreeSet(in.second().second()));
    return Pair.of(in.first(), Pair.of(hasEvent, unique));
  }
}
