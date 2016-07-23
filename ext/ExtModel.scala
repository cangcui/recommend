package com.tencent.ieg.tgp.recommend

import org.apache.hadoop.fs.Path

/**
  * Created by lambdachen on 2016/7/16.
  */

private[ieg] class ExtModel {

  def dataPath(path: String) = new Path(path, "data").toUri.toString

  def metadataPath(path: String) = new Path(path, "metadata").toUri.toString

}
