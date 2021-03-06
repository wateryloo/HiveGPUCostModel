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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * Default implementation of the cost model. Currently used by MR and Spark execution engines.
 */
public class HiveDefaultCostModel extends HiveCostModel {

  private static HiveDefaultCostModel INSTANCE;

  private HiveDefaultCostModel() {
    super(Sets.newHashSet(DefaultJoinAlgorithm.INSTANCE));
  }

  synchronized public static HiveDefaultCostModel getCostModel() {
    if (INSTANCE == null) {
      INSTANCE = new HiveDefaultCostModel();
    }

    return INSTANCE;
  }

  @Override
  public RelOptCost getDefaultCost() {
    return HiveCost.FACTORY.makeZeroCost();
  }

  /**
   * @param ts The {@code HiveTableScan} to compute cost.
   * @param mq The metadata.
   * @return The {@code HiveCost} of the {@code HiveTableScan}.
   */
  @Override
  public RelOptCost getScanCost(HiveTableScan ts, RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(ts);
    double gpuCostVal = 0.0;
    double ioCostVal = HiveCostModel.getTmmdm(ts, mq) + HiveCostModel.getCmap(ts, mq);
    HiveCost cost = new HiveCost(rowCount, gpuCostVal, ioCostVal);
    System.out.printf("scan cost: %s\n", cost);
    return cost;
  }

  @Override
  public RelOptCost getAggregateCost(HiveAggregate aggregate) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  /**
   * Default join algorithm. Cost is based on cardinality.
   * <p>
   * This join algorithm is blocked during optimization because we do not know how to measure the
   * cost according to the paper. However, it is the only algorithm used for implementation.
   */
  public static class DefaultJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new DefaultJoinAlgorithm();
    private static final String ALGORITHM_NAME = "DefaultJoin";


    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      double leftRCount = mq.getRowCount(join.getLeft());
      double rightRCount = mq.getRowCount(join.getRight());
      return HiveCost.FACTORY.makeCost(leftRCount + rightRCount, 0.0, 0.0);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return RelDistributions.SINGLETON;
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return null;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      return null;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return false;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return 1;
    }
  }

  public static class NonIndexedNestedLoopJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new NonIndexedNestedLoopJoinAlgorithm();
    private static final String ALGORITHM_NAME = "NonIndexedNestedLoopJoin";

    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {

      return null;
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return RelDistributions.SINGLETON;
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return null;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      return null;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return null;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return 1;
    }
  }

  public static class IndexedNestedLoopJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new IndexedNestedLoopJoinAlgorithm();
    private static final String ALGORITHM_NAME = "IndexedNestedLoopJoin";

    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      return null;
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return RelDistributions.SINGLETON;
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return null;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      return null;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return null;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return 1;
    }
  }

  public static class SortMergeJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new SortMergeJoinAlgorithm();
    private static final String ALGORITHM_NAME = "SortMergeJoin";

    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      return null;
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return RelDistributions.SINGLETON;
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return null;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      return null;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return null;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return 1;
    }
  }

  public static class HashJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new HashJoinAlgorithm();
    private static final String ALGORITHM_NAME = "HashJoin";

    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    /**
     * TODO: Currently, I find no way to compute GPU.
     *
     * @param join The {@code HashJoin} to compute cost.
     * @return {@code HiveCost}.
     */
    @Override
    public RelOptCost getCost(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      RelNode left = join.getInput(0);
      RelNode right = join.getInput(1);
      double rowCount = mq.getRowCount(join);
      double gpu = 0.0;
      double io = 0.0;
      io += getCfilter(left, left.getCluster().getMetadataQuery());
      io += getCfilter(right, right.getCluster().getMetadataQuery());
      io += getCoutput();
      return new HiveCost(rowCount, gpu, io);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return RelDistributions.SINGLETON;
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return null;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      return null;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return null;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return 1;
    }
  }

}
