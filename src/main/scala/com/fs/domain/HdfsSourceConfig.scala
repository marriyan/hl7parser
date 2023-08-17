package com.fs.domain

import org.apache.hadoop.conf.Configuration

case class HdfsSourceConfig(conf: Configuration)
  extends SourceConfig
