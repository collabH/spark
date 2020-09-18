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

package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf

/**
 * An interface for all the broadcast implementations in Spark (to allow
 * multiple broadcast implementations). SparkContext uses a BroadcastFactory
 * implementation to instantiate a particular broadcast for the entire Spark job.
 */
private[spark] trait BroadcastFactory {

  /**
   * 初始化广播工厂
   * @param isDriver 是否driver
   * @param conf spark配置
   * @param securityMgr 安全管理器
   */
  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager): Unit

  /**
   * 创建一个新的广播变量
   * Creates a new broadcast variable.
   *
   * @param value value to broadcast
   * @param isLocal whether we are in local mode (single JVM process)
   * @param id unique id representing this broadcast variable 广播变量的唯一id
   */
  def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T]

  /**
   * 移除广播变量
   * @param id 广播变量的唯一id
   * @param removeFromDriver 是否从driver移除
   * @param blocking 是否阻塞
   */
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit

  def stop(): Unit
}
