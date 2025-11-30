/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron

import org.apache.spark.sql.{AuronQueryTest, Row}
import org.apache.spark.sql.auron.AuronConverters
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}

class AuronCheckConvertShuffleExchangeSuite
    extends AuronQueryTest
    with BaseAuronSQLSuite {

  test("test set auron shuffle manager convert to native shuffle exchange " +
    "when set spark.auron.enable is true") {
    withTempView("v") {
      spark.range(1, 2)
        .toDF("c1")
        .createOrReplaceTempView("v")
      val executePlan =
        sql("select /*+ REPARTITION */ * from v")

      val shuffleExchangeExecList =
        collect(executePlan.queryExecution.executedPlan) {
          case shuffleExchangeExec: ShuffleExchangeLike =>
            shuffleExchangeExec
        }

      assert(shuffleExchangeExecList.length == 1)
      val afterConvertPlan = AuronConverters.convertSparkPlan(shuffleExchangeExecList.head)
      assert(afterConvertPlan.isInstanceOf[NativeShuffleExchangeExec])
      checkAnswer(executePlan, Seq(Row(1)))
    }
  }

  test("test set non auron shuffle manager do not convert to native shuffle exchange " +
      "when set spark.auron.enable is true") {
    withSQLConf("spark.shuffle.manager" -> "org.apache.spark.shuffle.sort.SortShuffleManager") {
      withTempView("v") {
        spark.range(1, 2)
          .toDF("c1")
          .createOrReplaceTempView("v")
        val executePlan =
          sql("select /*+ REPARTITION */ * from v")
        val shuffleExchangeExecList =
          collect(executePlan.queryExecution.executedPlan) {
            case shuffleExchangeExec: ShuffleExchangeLike => shuffleExchangeExec
          }
        val afterConvertPlan = AuronConverters.convertSparkPlan(shuffleExchangeExecList.head)
        assert(afterConvertPlan.isInstanceOf[ShuffleExchangeExec])
        checkAnswer(executePlan, Seq(Row(1)))
      }
    }
  }
}
