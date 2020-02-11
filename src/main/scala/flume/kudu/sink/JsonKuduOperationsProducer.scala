package flume.kudu.sink

import java.nio.charset.Charset
import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util
import java.util.{Date, List}

import com.google.common.base.Preconditions
import com.google.common.collect.Lists
import org.apache.commons.lang.StringUtils
import org.apache.flume.{Context, Event, FlumeException}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{KuduTable, Operation, PartialRow}
import org.apache.kudu.flume.sink.{KuduOperationsProducer, RegexpKuduOperationsProducer}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * A json operations producer that generates one or more Kudu
 * {@link Insert} or {@link Upsert} operations per Flume {@link Event} by
 * parsing the event {@code body} using a regular expression. Values are
 * coerced to the types of the named columns in the Kudu table.
 * <p>
 * <p>Example: If the Kudu table has the schema:
 * <p>
 * <pre>
 * key INT32
 * name STRING</pre>
 * <p>
 * <p>and {@code producer event body like {"key":"value_of_key","name":"the_value_of_name"}} :
 * <p>
 *
 * into the rows: {@code (key=12345, name=Mike)}.
 *
 * <p>Note: This class relies on JDK8 named capturing groups, which are
 * documented in {@link Pattern}. The name of each capturing group must
 * correspond to a column name in the destination Kudu table.
 */

object JsonKuduOperationsProducer {
  private val logger = LoggerFactory.getLogger(classOf[RegexpKuduOperationsProducer])
  private val INSERT = "insert"
  private val UPSERT = "upsert"
  private val validOperations = Lists.newArrayList(UPSERT, INSERT)
  val ENCODING_PROP = "encoding"
  val DEFAULT_ENCODING = "utf-8"
  val OPERATION_PROP = "operation"
  val DEFAULT_OPERATION: String = UPSERT
  val SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue"
  val DEFAULT_SKIP_BAD_COLUMN_VALUE = false
  val SKIP_ERROR_EVENT_PROP = "skipErrorEvent"
  val DEFAULT_SKIP_ERROR_EVENT = false
  val ADD_CONTENT_MD5 = "addContentMD5"
  val ADD_TIMESTAMP_COLUM = "addTimeStampColum"
  val KUDU_TIMESTAMP_COLUMS = "kuduTimeStampColums"
  //case sensitive
  val KUDU_PRIMARY_KEYS = "jsonPrimaryKeys"
  val INPUT_DATE_FORMAT = "inputDateFormat"
  val DEFAULT_INPUT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.S"
}

class JsonKuduOperationsProducer extends KuduOperationsProducer {
  private var primaryKeySet: util.Set[String] = _
  private var table: KuduTable = _
  private var charset: Charset = _
  private var operation: String = _
  private var skipBadColumnValue: Boolean = false
  private var skipErrorEvent: Boolean = false
  private var timeColumSet: util.Set[String] = _
  private var dateFormat: DateFormat = _
  private var addContentMD5: Boolean = false
  private var addTimeStamp: Boolean = false

  override def configure(context: Context): Unit = {
    val charsetName: String = context.getString(JsonKuduOperationsProducer.ENCODING_PROP, JsonKuduOperationsProducer.DEFAULT_ENCODING)
    try charset = Charset.forName(charsetName)
    catch {
      case e: IllegalArgumentException =>
        throw new FlumeException(String.format("Invalid or unsupported charset %s", charsetName), e)
    }
    operation = context.getString(JsonKuduOperationsProducer.OPERATION_PROP, JsonKuduOperationsProducer.DEFAULT_OPERATION).toLowerCase
    Preconditions.checkArgument(JsonKuduOperationsProducer.validOperations.contains(operation), "Unrecognized operation '%s'", operation)
    skipErrorEvent = context.getBoolean(JsonKuduOperationsProducer.SKIP_ERROR_EVENT_PROP, JsonKuduOperationsProducer.DEFAULT_SKIP_ERROR_EVENT)
    skipBadColumnValue = context.getBoolean(JsonKuduOperationsProducer.SKIP_BAD_COLUMN_VALUE_PROP, JsonKuduOperationsProducer.DEFAULT_SKIP_BAD_COLUMN_VALUE)
    addContentMD5 = context.getBoolean(JsonKuduOperationsProducer.ADD_CONTENT_MD5, true)
    addTimeStamp = context.getBoolean(JsonKuduOperationsProducer.ADD_TIMESTAMP_COLUM, true)
    val primaryKeys: String = context.getString(JsonKuduOperationsProducer.KUDU_PRIMARY_KEYS, "")
    primaryKeySet = new util.HashSet[String]
    val pks: Array[String] = primaryKeys.split(",")
    for (pk <- pks) {
      if (!("" == pk.trim)) primaryKeySet.add(pk.trim)
    }
    val inputDateFormat: String = context.getString(JsonKuduOperationsProducer.INPUT_DATE_FORMAT, JsonKuduOperationsProducer.DEFAULT_INPUT_DATE_FORMAT)
    dateFormat = new SimpleDateFormat(inputDateFormat)
    val timeColumes: String = context.getString(JsonKuduOperationsProducer.KUDU_TIMESTAMP_COLUMS)
    if (timeColumes != null && !StringUtils.isEmpty(timeColumes)) {
      val msg: String = "kuduTimeStampColums=%s,inputDateFormat=%s"
      JsonKuduOperationsProducer.logger.info(String.format(msg, timeColumes, inputDateFormat))
      val array: Array[String] = timeColumes.split(",")
      timeColumSet = new util.HashSet[String]
      for (col <- array) {
        timeColumSet.add(StringUtils.trim(col))
      }
    }
  }

  override def initialize(table: KuduTable): Unit = {
    this.table = table
  }

  @throws[FlumeException]
  override def getOperations(event: Event): List[Operation] = {
    val ops: List[Operation] = Lists.newArrayList[Operation]
    val raw: String = new String(event.getBody, charset)
    try {
      val json = JsonUtil.toMap[Any](raw)
//      val json: JSONObject = JSONObject.parseObject(raw)
      //            if (addContentMD5) {
      //                json.put("md5", DigestUtils.md5Hex(raw));
      //            }
      //            if (addTimeStamp) {
      //                json.put("timestamp", currentUnixTimeMicros());
      val schema: Schema = table.getSchema
      if (json.nonEmpty) {
        val op: Operation = getOperation(raw, json, schema)
        ops.add(op)
      }
    } catch {
      case e: Exception =>
        val msg: String = String.format("Event '%s' Can't parse to Json Object", raw)
        logOrThrow(skipErrorEvent, msg, e)
    }
    ops
  }

  private def currentUnixTimeMicros: Long = System.currentTimeMillis * 1000

  private def getOperation(raw: String, json: mutable.Map[String, Any], schema: Schema): Operation = {
    val op: Operation = getOperationType
    var col: ColumnSchema = null
    val row: PartialRow = op.getRow
    primaryKeySet.forEach(primary => {
      val `val`: Object = json.get(primary)
      if (`val` == null) json.put(primary, "")
    })
    json.foreach(entry => {
      try {
        val key: String = entry._1
        col = schema.getColumn(key.toLowerCase)
        var value: String = null
        if (entry._2 != null) value = entry._2.toString
        if (timeColumSet != null && timeColumSet.contains(col.getName)) {
          value = toUnixtimeMicros(value)
          //Bigger than current Time 2 hours
          val hours: Long = currentUnixTimeMicros + 2 * 60 * 3600 * 1000 * 1000L
          if (value.toLong > hours) {
            JsonKuduOperationsProducer.logger.warn(String.format("%s Bigger than current Time %s  2 hours: %s", col.getName, hours.toString, JsonUtil.toJson(json)))
            value = currentUnixTimeMicros.toString
          }
        }
        if (!isNullOrEmpty(value)) coerceAndSet(value, col.getName, col.getType, row)
        if (isNullOrEmpty(value) && isPrimaryKey(key)) coerceAndSet(col.getName, col.getType, row)
      } catch {
        case e: NumberFormatException =>
          val msg: String = String.format("Raw value '%s' couldn't be parsed to type %s for column '%s'", raw, col.getType, col.getName)
          logOrThrow(skipBadColumnValue, msg, e)
        case e: Exception =>
          throw new FlumeException("Failed to create Kudu operation", e)
      }
    })
    if (JsonKuduOperationsProducer.logger.isDebugEnabled) JsonKuduOperationsProducer.logger.debug("Operation: " + op.getRow.toString)
    op
  }

  @throws[ParseException]
  private def toUnixtimeMicros(dateString: String): String = {
    if (dateString == null || "" == dateString.trim) return dateString
    val date: Date = dateFormat.parse(dateString)
    val micros: Long = date.getTime * 1000
    micros.toString
  }

  private def getOperationType: Operation = {
    var op: Operation = null
    operation match {
      case JsonKuduOperationsProducer.UPSERT =>
        op = table.newUpsert

      case JsonKuduOperationsProducer.INSERT =>
        op = table.newInsert

      case _ =>
        throw new FlumeException(String.format("Unrecognized operation type '%s' in getOperations(): " + "this should never happen!", operation))
    }
    op
  }

  /**
   * Coerces the string `rawVal` to the type `type` and sets the resulting
   * value for column `colName` in `row`.
   *
   * @param rawVal  the raw string column value
   * @param colName the name of the column
   * @param type    the Kudu type to convert `rawVal` to
   * @param row     the row to set the value in
   * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
   */
  @throws[NumberFormatException]
  private def coerceAndSet(rawVal: String, colName: String, `type`: Type, row: PartialRow): Unit = {
    `type` match {
      case Type.INT8 =>
        row.addByte(colName, rawVal.toByte)

      case Type.INT16 =>
        row.addShort(colName, rawVal.toShort)

      case Type.INT32 =>
        row.addInt(colName, rawVal.toInt)

      case Type.INT64 =>
        row.addLong(colName, rawVal.toLong)

      case Type.BINARY =>
        row.addBinary(colName, rawVal.getBytes(charset))

      case Type.STRING =>
        row.addString(colName, rawVal)

      case Type.BOOL =>
        row.addBoolean(colName, rawVal.toBoolean)

      case Type.FLOAT =>
        row.addFloat(colName, rawVal.toFloat)

      case Type.DOUBLE =>
        row.addDouble(colName, rawVal.toDouble)

      case Type.UNIXTIME_MICROS =>
        row.addLong(colName, rawVal.toLong)

      case _ =>
        JsonKuduOperationsProducer.logger.warn("got unknown type {} for column '{}'-- ignoring this column", `type`, colName)
    }
  }

  @throws[NumberFormatException]
  private def coerceAndSet(colName: String, `type`: Type, row: PartialRow): Unit = {
    `type` match {
      case Type.INT8 =>
        row.addByte(colName, (-1).toByte)

      case Type.INT16 =>
        row.addShort(colName, (-1).toShort)

      case Type.INT32 =>
        row.addInt(colName, -1)

      case Type.INT64 =>
        row.addLong(colName, -1)

      case Type.BINARY =>
        row.addBinary(colName, "".getBytes(charset))

      case Type.STRING =>
        row.addString(colName, "")

      case Type.BOOL =>
        row.addBoolean(colName, false)

      case Type.FLOAT =>
        row.addFloat(colName, -1)

      case Type.DOUBLE =>
        row.addDouble(colName, -1)

      case Type.UNIXTIME_MICROS =>
        row.addLong(colName, currentUnixTimeMicros)

      case _ =>
        JsonKuduOperationsProducer.logger.warn("got unknown type {} for column '{}'-- ignoring this column", `type`, colName)
    }
  }

  @throws[FlumeException]
  private def logOrThrow(log: Boolean, msg: String, e: Exception): Unit = {
    if (log) JsonKuduOperationsProducer.logger.warn(msg, e)
    else throw new FlumeException(msg, e)
  }

  private def isNullOrEmpty(value: String): Boolean = {
    if (value == null || "" == value.trim || "null" == value.trim.toLowerCase) return true
    false
  }

  private def isPrimaryKey(key: String): Boolean = primaryKeySet.contains(key)

  override def close(): Unit = {
  }
}
