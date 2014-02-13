/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scratch

import scala.collection.JavaConverters.mapAsScalaMapConverter

import cascading.tap.Tap
import com.twitter.scalding.Args

import org.kiji.express._
import org.kiji.express.flow._

/**
 * A demonstration of the Kiji API.
 *
 * Reads in data from a table and writes that data to another column.
 */
class DemoKiji(args: Args) extends KijiJob(args) {
  val tableUri: String = args("table")

  // Save the output that we'll need to add an OutputCommitter to.
  val mOutputToModify = KijiOutput.builder
    .withTableURI(tableUri)
    .withColumns('nameCopy -> "info:nameCopy")
    .build

  KijiInput.builder
      .withTableURI(tableUri)
      .withColumns("info:name" -> 'name)
      .build
      // A no-op read/write for example purposes.
      .map('name -> 'nameCopy) { slice: Seq[FlowCell[CharSequence]] =>
        slice.head.datum.toString
      }
      // Force another mapreduce job for demonstrating that intermediate steps don't have the
      // custom outputcommitter.
      .unique('nameCopy, 'entityId)
      .unique('nameCopy, 'entityId)
      // Write the number of each name to the specified table. (Should always be 1)
      .write(mOutputToModify)

  // ---- BEGIN OUTPUTCOMMITTER DEMO ----

  // Get all the sinks for this job.
  val sinks: Map[String, Tap[_,_,_]] = flowDef.getSinks().asScala

  // Find the tap you want to modify.  In this case it's mOutputToModify, which we saved earlier.
  val tapToModify: Tap[_, _, _] = {
    val maybeTap: Option[Tap[_,_,_]] = sinks.get(mOutputToModify.toString)
    require(maybeTap.isDefined)
    maybeTap.get
  }
  println("Modifying tap: " + tapToModify)

  // Modify only this tap to set the outputcommitter class you want to use.
  tapToModify
    .getStepConfigDef
    .setProperty("mapred.output.committer.class", "org.kiji.scratch.TestOutputCommitter")
}
