/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  * Adapted from Confluent Inc. whose copyright is reproduced below.
  */
/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lightbend.kafka.scala.streams

import org.apache.kafka.streams.KeyValue

trait StreamToTableJoinTestData {
  val brokers = "localhost:9092"

  val userClicksTopic  = s"user-clicks.${scala.util.Random.nextInt(100)}"
  val userRegionsTopic = s"user-regions.${scala.util.Random.nextInt(100)}"
  val outputTopic      = s"output-topic.${scala.util.Random.nextInt(100)}"
  val localStateDir    = "local_state_data"

  // Input 1: Clicks per user (multiple records allowed per user).
  val userClicks: Seq[KeyValue[String, Long]] = Seq(
    new KeyValue("alice", 13L),
    new KeyValue("bob", 4L),
    new KeyValue("chao", 25L),
    new KeyValue("bob", 19L),
    new KeyValue("dave", 56L),
    new KeyValue("eve", 78L),
    new KeyValue("alice", 40L),
    new KeyValue("fang", 99L)
  )

  // Input 2: Region per user (multiple records allowed per user).
  val userRegions: Seq[KeyValue[String, String]] = Seq(
    new KeyValue("alice", "asia"), /* Alice lived in Asia originally... */
    new KeyValue("bob", "americas"),
    new KeyValue("chao", "asia"),
    new KeyValue("dave", "europe"),
    new KeyValue("alice", "europe"), /* ...but moved to Europe some time later. */
    new KeyValue("eve", "americas"),
    new KeyValue("fang", "asia")
  )

  val expectedClicksPerRegion: Seq[KeyValue[String, Long]] = Seq(
    new KeyValue("americas", 101L),
    new KeyValue("europe", 109L),
    new KeyValue("asia", 124L)
  )
}