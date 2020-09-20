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

package org.apache.spark.util.collection

import java.util.Comparator

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // 负载因子，超过该Capacity*0。7自动扩容
  private val LOAD_FACTOR = 0.7

  // 容量，转换为2的n次方，主要为了后去计算hash时可以直接将取模的逻辑运算优化为算术运算
  private var capacity = nextPowerOf2(initialCapacity)
  // 计算数据存放位置的掩码。计算mask的表达式为capacity -1。
  private var mask = capacity - 1
  // 记录当前已经放入data的key与聚合值的数量。
  private var curSize = 0
  // :data数组容量增长的阈值。计算growThreshold的表达式为grow-Threshold =LOAD_FACTOR * capacity。
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  // 存储数据的数组，初始化为2倍的capacity
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  // 是否存在null值
  private var haveNullValue = false
  // 控制
  private var nullValue: V = null.asInstanceOf[V]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  // 表示data数组是否不再使用
  private var destroyed = false
  // 当destroyed为true时，打印的消息内容为"Map state is invalid fromdestructive sorting! "。
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /** Get the value for a given key */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    // 相当于key的hashcode&cap-1 === k.hashcode % cap
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      // 计算当前key的offset
      val curKey: AnyRef = data(2 * pos)
      // 如果k等于当前key
      if (k.eq(curKey) || k.equals(curKey)) {
        // 返回当前key的value，value存储在key的offset+1的位置
        return data(2 * pos + 1).asInstanceOf[V]
        // 如果key为null，返回null
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        // pos往后移动
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /**
   * 设置值，存在这修改，不存在这插入
   */
  /** Set the value for a key */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    // 如果key为null
    if (k.eq(null)) {
      // 判断是否存在null值，如果不存在，这容量增长
      if (!haveNullValue) {
        // 判断是否需要扩容
        incrementSize()
      }
      // 为nullValue赋值
      nullValue = value
      // 设置存在key为Null的value
      haveNullValue = true
      // 返回
      return
    }
    // 计算pos
    var pos = rehash(key.hashCode) & mask
    var i = 1
    // 遍历
    while (true) {
      // 计算当前key的位置
      val curKey = data(2 * pos)
      // 如果当前key在数组中的位置不存在
      if (curKey.eq(null)) {
        // 设置值
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
        // 如果存在
      } else if (k.eq(curKey) || k.equals(curKey)) {
        // 修改value
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        // 如果不等，则向后移动
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * 为key的value设置updateFunc
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      // nullVlaue等于Null
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      // 返回null
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        // newValue为null
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        // 设置value为null
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        // 判断是否需要扩容
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): (K, V) = {
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /**
   * 增加1 table的容器 如果有必要就重新计算hash
   */
  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    // 当前size+1
    curSize += 1
    // 如果当前size大于growThreshold则扩容
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /**
   * 2被扩容table并且重新计算hash
   */
  /** Double the table's size and re-hash everything */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    // 新的容量
    val newCapacity = capacity * 2
    // check cap
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    // 穿件新的数组
    val newData = new Array[AnyRef](2 * newCapacity)
    // 计算新的mask
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    // 遍历data数组
    while (oldPos < capacity) {
      // 过滤key为null的数据
      if (!data(2 * oldPos).eq(null)) {
        // 获取key
        val key = data(2 * oldPos)
        // 获取value
        val value = data(2 * oldPos + 1)
        // 重新计算新的pos
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        // 保持前进
        var keepGoing = true
        while (keepGoing) {
          // 计算新的key
          val curKey = newData(2 * newPos)
          // 如果key为null
          if (curKey.eq(null)) {
            // 赋值key和value
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            // 如果已经存在数据
            val delta = i
            // 向后移动
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    // 重新复制
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    var keyIndex, newIndex = 0
    // 遍历数组
    while (keyIndex < capacity) {
      // 如果key不为null，去掉不连续的null key
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))

    // 创建Sort排序起
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29)
}
