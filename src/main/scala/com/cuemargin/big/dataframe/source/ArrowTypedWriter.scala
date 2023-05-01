package com.cuemargin.big.dataframe.source

import org.apache.arrow.vector._
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.ArrowType

import java.time.LocalDate

/**
 * Created by Bharathi Pairan on 09/04/2023.
 */
object ArrowTypedWriter {

  def typedWrite(arrowType: ArrowType, fieldVector: FieldVector, idx: Int, valueOption: Option[Any]): Unit = {
    valueOption match {
      case Some(value) =>
        arrowType match {
          case t if t == Types.MinorType.FLOAT8.getType => fieldVector.asInstanceOf[Float8Vector].setSafe(idx, value.asInstanceOf[Double])
          case t if t == Types.MinorType.BIGINT.getType => fieldVector.asInstanceOf[BigIntVector].setSafe(idx, value.asInstanceOf[Long])
          case t if t == Types.MinorType.UINT4.getType => fieldVector.asInstanceOf[UInt4Vector].setSafe(idx, value.asInstanceOf[Int])
          case t if t == Types.MinorType.INT.getType => fieldVector.asInstanceOf[IntVector].setSafe(idx, value.asInstanceOf[Int])
          case t if t == Types.MinorType.BIT.getType => fieldVector.asInstanceOf[BitVector].setSafe(idx, if (value.asInstanceOf[Boolean]) 1 else 0)
          case t if t == Types.MinorType.VARCHAR.getType => fieldVector.asInstanceOf[VarCharVector].setSafe(idx, value.asInstanceOf[String].getBytes())
          case t if t == Types.MinorType.DATEDAY.getType => fieldVector.asInstanceOf[DateDayVector].setSafe(idx, value.asInstanceOf[LocalDate].toEpochDay.intValue())
        }
      case None => fieldVector.setNull(idx)
    }
  }
}
