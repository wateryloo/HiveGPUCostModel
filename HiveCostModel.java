/*
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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import java.util.Set;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel.DefaultJoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cost model interface.
 */
public abstract class HiveCostModel {

  private static final Logger LOG = LoggerFactory.getLogger(HiveCostModel.class);

  private final Set<JoinAlgorithm> joinAlgorithms;

  /**
   * param for T_0. Non-negative.
   */
  private final static double T0 = 1.0;

  /**
   * param for band-width. Positive.
   */
  private final static double BAND_WIDTH = 1.0;

  /**
   * Memory bandwidth of coalesced access. Positive.
   */
  private final static double B_H = 1.0;

  /**
   * Memory bandwidth of non-coalesced access. Positive.
   */
  private final static double B_L = 1.0;

  /**
   * Block size of the device memory in bytes. Positive.
   */
  private final static int BLOCK_SIZE = 1;

  public HiveCostModel(Set<JoinAlgorithm> joinAlgorithms) {
    this.joinAlgorithms = joinAlgorithms;
  }

  /**
   * @param relNode The operator to evaluate the cost to transfer data between CPU memory and GPU
   *                memory.
   * @param mq      The metadata.
   * @return The value of cost. TODO: Currently, only table-scan contains this cost.
   */
  public static double getTmmdm(RelNode relNode, RelMetadataQuery mq) {
    double averageRowSize = mq.getAverageRowSize(relNode);
    double rowCount = mq.getRowCount(relNode);
    double dataSize = averageRowSize * rowCount;
    return HiveCostModel.T0 + dataSize / HiveCostModel.BAND_WIDTH;
  }

  public static double getTcomputation() {
    return 0.0;
  }

  public static double getTmem() {
    return 0.0;
  }

  /**
   * @param relNode The operator which contains a map primitive.
   * @param mq      The metadata.
   * @return The value of map primitive cost.
   */
//  TODO: Currently no way to compute cardinality of output, input used twice instead.
  public static double getCmap(RelNode relNode, RelMetadataQuery mq) {
    double cardinalityOfIn = mq.getRowCount(relNode);
    double cardinalityOfOut = cardinalityOfIn;

    return (cardinalityOfIn + cardinalityOfOut) / HiveCostModel.B_H;
  }

  public static double getCscatter() {
    return 0.0;
  }

  /**
   * TODO: It only implements non-sequential, and R_in, R_out and L have equal cardinality.
   *
   * @param relNode The operator which contains a gather primitive.
   * @param mq      The metadata.
   * @return The value of gather primitive cost.
   */
  public static double getCgather(RelNode relNode, RelMetadataQuery mq) {
    double rIn = mq.getRowCount(relNode);
    double rOut = mq.getRowCount(relNode);
    double lCardinality = mq.getRowCount(relNode);
    double cost = (rOut + lCardinality) / B_H + rIn * BLOCK_SIZE / B_L;
    return cost;
  }

  public static double getCreduce() {
    return 0.0;
  }

  public static double getCpscan() {
    return 0.0;
  }

  public static double getCsplit() {
    return 0.0;
  }

  public static double getCfilter() {
    return 0.0;
  }

  public static double getCqsort() {
    return 0.0;
  }

  public static double getCtree() {
    return 0.0;
  }

  public static double getChash() {
    return 0.0;
  }

  public static double getCoutput() {
    return 0.0;
  }

  public static double getCninlj() {
    return 0.0;
  }

  public static double getCinlj() {
    return 0.0;
  }

  public static double getCsmj() {
    return 0.0;
  }

  public static double getChj() {
    return 0.0;
  }

  public abstract RelOptCost getDefaultCost();

  public abstract RelOptCost getAggregateCost(HiveAggregate aggregate);

  public abstract RelOptCost getScanCost(HiveTableScan ts, RelMetadataQuery mq);

  /**
   * TODO: The select join algorithm is always DefaultJoinAlgorithm.
   *
   * @param join The join operator.
   * @return The min cost of {@code join}.
   */
  public RelOptCost getJoinCost(HiveJoin join) {
    // Select algorithm with min cost
    JoinAlgorithm joinAlgorithm = null;
    RelOptCost minJoinCost = null;

    if (LOG.isTraceEnabled()) {
      LOG.trace("Join algorithm selection for:\n" + RelOptUtil.toString(join));
    }

    joinAlgorithms.forEach(joinAlgorithm1 -> {
      System.out.printf("join algorithm: %s\n", joinAlgorithm1);
    });

    for (JoinAlgorithm possibleAlgorithm : this.joinAlgorithms) {

//      We do not know what is the implementation of DefaultJoinAlgorithm.
      if (possibleAlgorithm instanceof DefaultJoinAlgorithm) {
        continue;
      }

      if (!possibleAlgorithm.isExecutable(join)) {
        System.out.printf("%s not executable\n", possibleAlgorithm);
        continue;
      }
      RelOptCost joinCost = possibleAlgorithm.getCost(join);

      if (LOG.isTraceEnabled()) {
        LOG.trace(possibleAlgorithm + " cost: " + joinCost);
      }

      System.out.printf("%s cost: %s\n", possibleAlgorithm, joinCost);

      if (minJoinCost == null || joinCost.isLt(minJoinCost)) {
        joinAlgorithm = possibleAlgorithm;
        minJoinCost = joinCost;
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(joinAlgorithm + " selected");
    }

//    join.setJoinAlgorithm(joinAlgorithm);
    join.setJoinAlgorithm(DefaultJoinAlgorithm.INSTANCE);
    join.setJoinCost(minJoinCost);

    return minJoinCost;
  }

  /**
   * Interface for join algorithm.
   */
  public interface JoinAlgorithm {

    String toString();

    boolean isExecutable(HiveJoin join);

    RelOptCost getCost(HiveJoin join);

    ImmutableList<RelCollation> getCollation(HiveJoin join);

    RelDistribution getDistribution(HiveJoin join);

    Double getMemory(HiveJoin join);

    Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join);

    Boolean isPhaseTransition(HiveJoin join);

    Integer getSplitCount(HiveJoin join);
  }

}
