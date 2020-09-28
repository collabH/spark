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

package org.apache.spark.deploy.master

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging

private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  // zookeeper存spark选举的目录
  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  private var zk: CuratorFramework = _
  //  leader锁存器
  private var leaderLatch: LeaderLatch = _
  // leader撤销状态
  private var status = LeadershipStatus.NOT_LEADER

  /**
   * 开始选举
   */
  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = SparkCuratorUtil.newClient(conf)
    // 创建leader所
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    // 添加监听器
    leaderLatch.addListener(this)
    // 开启
    leaderLatch.start()
  }

  override def stop() {
    leaderLatch.close()
    zk.close()
  }

  /**
   * 是否是leader
   */
  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  /**
   * 不是leader
   */
  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  /**
   * 修改leader撤回状态
   * @param isLeader
   */
  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      // 选举出leader
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      masterInstance.revokedLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
