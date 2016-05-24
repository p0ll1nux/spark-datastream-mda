import org.apache.spark.ml._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TweetTransformer(override val uid: String)
  extends UnaryTransformer[String, String, TweetTransformer] {

  def this() = this(Identifiable.randomUID("tweettrans"))

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType)
  }

  protected def createTransformFunc: String => String = { originStr =>
    if(originStr.indexOf("http")>0)
      originStr.substring(0,originStr.indexOf("http"))
    else
      originStr
  }


  protected def outputDataType: DataType = StringType


}
