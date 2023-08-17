package com.fs.domain

case class SFTPSourceConfig(host: String,
                            port: String,
                            username: String,
                            password: String) extends SourceConfig
