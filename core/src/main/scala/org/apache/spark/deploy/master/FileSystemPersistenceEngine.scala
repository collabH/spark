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

import java.io._

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.Utils

import scala.reflect.ClassTag


/**
 * 在每个应用程序和worker一个文件一个磁盘上的目录中存储数据。 应用程序和worker被删除时，文件被删除
 * Stores data in a single on-disk directory with one file per application and worker.
 * Files are deleted when applications and workers are removed.
 *
 * @param dir        Directory to store files. Created if non-existent (but not recursively).
 * @param serializer Used to serialize our objects.
 */
private[master] class FileSystemPersistenceEngine(
                                                   val dir: String,
                                                   val serializer: Serializer)
  extends PersistenceEngine with Logging {

  // 创建目录
  new File(dir).mkdir()

  override def persist(name: String, obj: Object): Unit = {
    // 序列化到文件
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  override def unpersist(name: String): Unit = {
    val f = new File(dir + File.separator + name)
    if (!f.delete()) {
      logWarning(s"Error deleting ${f.getPath()}")
    }
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
  }

  /**
   * 序列化数据到文件
   *
   * @param file
   * @param value
   */
  private def serializeIntoFile(file: File, value: AnyRef) {
    // 创建新的文件
    val created = file.createNewFile()
    // 如果不能创建报错
    if (!created) {
      throw new IllegalStateException("Could not create file: " + file)
    }
    // 获取文件输出流
    val fileOut = new FileOutputStream(file)
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      // 序列化方式
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
  }

  /**
   * 反序列化文件数据
   * @param file
   * @param m
   * @tparam T
   * @return
   */
  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    val fileIn = new FileInputStream(file)
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
