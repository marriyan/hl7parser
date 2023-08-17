package com.parser.exeptions


final case class SchemaFilePathMissingException(private val message: String = "",
                                                private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
