package com.microsoft.ml.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType

object SparkUserDefinedFunctionAccessor {
  private def getField(field:String,udf: UserDefinedFunction): AnyRef = {
    val clazz = udf.getClass
    if( clazz.getSimpleName != "SparkUserDefinedFunction" )
      throw new UnsupportedOperationException("Only SparkUserDefinedFunctions can be accessed!")
    val field = clazz.getDeclaredField("f")
    field.setAccessible(true)
    field.get()
  }

  def getF(udf: UserDefinedFunction): AnyRef = getField("f",udf)

  def getDatatype(udf: UserDefinedFunction): DataType = getField("dataType",udf).asInstanceOf[DataType]
}
