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

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

public class MultiTouchAttributionModelTest {
  @Test
  public void testSimpleInMemory() throws Exception {
    PCollection<String> touches = MemPipeline.typedCollectionOf(
        Avros.strings(),
        "a,1",
        "a,2",
        "c,2",
        "b,1",
        "a,3",
        "c,3",
        "d,1",
        "e,1",
        "e,3",
        "e,1",
        "f,2",
        "f,1",
        "f,2",
        "f,3",
        "f,4");
    PCollection<String> events = MemPipeline.typedCollectionOf(
        Avros.strings(), "a", "e", "f");
    
    MultiTouchAttributionModel model = new MultiTouchAttributionModel(true);
    PCollection<String> out = model.exec(touches, events);
    for (String row : out.materialize()) {
      System.out.println(row);
    }
  }
}
