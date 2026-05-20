package lab03

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{Text, IntWritable, LongWritable}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * BÀI 2: MEDIAN VARIETY - Độ đa dạng trung vị
 */
object MedianVarietyJob {

  val VALID_SIZES = Set("XXL", "3XL", "4XL", "5XL", "6XL")

  /**
   * JOB 1 MAPPER: Emit TẤT CẢ đơn hàng hợp lệ
   */
  class Job1Mapper extends Mapper[LongWritable, Text, Text, Text] {
    private val outKey = new Text()
    private val outValue = new Text()
    private val dateFormat = new SimpleDateFormat("MM-dd-yy")

    override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      try {
        val line = value.toString
        if (line.startsWith("index,Order ID")) return

        val fields = parseCSVLine(line)
        if (fields.length < 18) return

        val dateStr = fields(2).trim
        val style = fields(7).trim
        val sku = fields(8).trim
        val sizeRaw = fields(10).trim
        val stateRaw = fields(17).trim

        // Chuẩn hóa State
        val state = stateRaw.toUpperCase

        // Validate dữ liệu
        if (state.isEmpty || state.equalsIgnoreCase("NULL")) return
        if (style.isEmpty || sku.isEmpty) return
        if (sizeRaw.isEmpty) return

        val size = sizeRaw.toUpperCase.trim

        // Parse ngày tháng để lấy Năm-Tháng
        val orderDate = try {
          dateFormat.parse(dateStr)
        } catch {
          case _: Exception => return
        }

        val cal = java.util.Calendar.getInstance()
        cal.setTime(orderDate)
        val year = cal.get(java.util.Calendar.YEAR)
        val month = cal.get(java.util.Calendar.MONTH) + 1
        val monthStr = f"$year%04d-$month%02d"

        // EMIT TẤT CẢ: Key = (Month_State_Style), Value = (SKU_Size)
        outKey.set(s"${monthStr}_${state}_${style}")
        outValue.set(s"${sku}_${size}")
        context.write(outKey, outValue)

      } catch {
        case e: Exception =>
          System.err.println(s"Job1Mapper error: ${e.getMessage}")
      }
    }

    private def parseCSVLine(line: String): Array[String] = {
      val result = new mutable.ArrayBuffer[String]()
      var current = new StringBuilder()
      var inQuotes = false

      for (c <- line) {
        c match {
          case '"' => inQuotes = !inQuotes
          case ',' if !inQuotes =>
            result += current.toString
            current = new StringBuilder()
          case _ => current.append(c)
        }
      }
      result += current.toString
      result.toArray
    }
  }

  /**
   * JOB 1 REDUCER: Kiểm tra cờ hasXXL + Đếm số lượng SKU phân biệt
   */
  class Job1Reducer extends Reducer[Text, Text, Text, IntWritable] {
    private val result = new IntWritable()

    override def reduce(
      key: Text,
      values: java.lang.Iterable[Text],
      context: Reducer[Text, Text, Text, IntWritable]#Context
    ): Unit = {
      try {
        // Cấu trúc 1: Set (lưu trữ các SKU không trùng lặp)
        val distinctSKUs = new mutable.HashSet[String]()
        // Cấu trúc 2: Biến cờ (kiểm tra xem có phục vụ size XXL+ hay không)
        var hasXXL = false

        values.asScala.foreach { value =>
          val parts = value.toString.split("_", 2)
          if (parts.length == 2) {
            val sku = parts(0)
            val size = parts(1)

            distinctSKUs += sku

            if (VALID_SIZES.contains(size)) {
              hasXXL = true
            }
          }
        }

        // CHỈ EMIT KHI CÓ PHỤC VỤ XXL TRỞ LÊN
        if (hasXXL && distinctSKUs.nonEmpty) {
          val varietyCount = distinctSKUs.size
          result.set(varietyCount)
          context.write(key, result)
        }

      } catch {
        case e: Exception =>
          System.err.println(s"Job1Reducer error: ${e.getMessage}")
      }
    }
  }

  /**
   * ==================== JOB 2: TÍNH MEDIAN ====================
   */

  /**
   * JOB 2 MAPPER: Đọc kết quả từ file tạm của Job 1
   */
  class Job2Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    private val outKey = new Text()
    private val outValue = new IntWritable()

    override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context
    ): Unit = {
      try {
        val line = value.toString.trim

        // Tách chuỗi bằng dấu \t
        val parts = line.split("\t")
        if (parts.length != 2) return

        val keyPart = parts(0)
        val varietyCount = try {
          parts(1).toInt
        } catch {
          case _: Exception => return
        }

        // Parse Month và State từ Key của Job 1 (Month_State_Style)
        val keyParts = keyPart.split("_")
        if (keyParts.length < 3) return

        val month = keyParts(0)
        val state = keyParts(1)

        // Đẩy đi với Key mới là (Month_State)
        outKey.set(s"${month}_${state}")
        outValue.set(varietyCount)
        context.write(outKey, outValue)

      } catch {
        case e: Exception =>
          System.err.println(s"Job2Mapper error: ${e.getMessage}")
      }
    }
  }

  /**
   * JOB 2 REDUCER: Gom nhóm, Sort mảng và tính Toán Median
   */
  class Job2Reducer extends Reducer[Text, IntWritable, Text, Text] {

    // Ghi header chuẩn CSV ngay khi Reducer khởi động
    override def setup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
      context.write(new Text("Month"), new Text("State,MedianVariety"))
    }

    override def reduce(
      key: Text,
      values: java.lang.Iterable[IntWritable],
      context: Reducer[Text, IntWritable, Text, Text]#Context
    ): Unit = {
      try {
        // Thu thập toàn bộ giá trị Độ đa dạng vào 1 danh sách và Sắp xếp tăng dần
        val varieties = new java.util.ArrayList[Integer]()

        values.asScala.foreach { value =>
          varieties.add(Integer.valueOf(value.get()))
        }

        java.util.Collections.sort(varieties)

        val median = if (varieties.isEmpty) {
          0.0
        } else {
          val n = varieties.size()
          if (n % 2 == 0) {
            (varieties.get(n/2 - 1).intValue() + varieties.get(n/2).intValue()) / 2.0
          } else {
            varieties.get(n/2).doubleValue()
          }
        }

        val keyParts = key.toString.split("_", 2)
        if (keyParts.length == 2) {
          val month = keyParts(0)
          val state = keyParts(1)

          context.write(
            new Text(month),
            new Text(s"${state},${median}")
          )
        }

      } catch {
        case e: Exception =>
          System.err.println(s"Job2Reducer error: ${e.getMessage}")
      }
    }
  }

  /**
   * MAIN: Khởi chạy 2 Jobs nối tiếp nhau
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: <input> <temp> <output>")
      System.exit(-1)
    }

    val conf1 = new Configuration()
    val fs = FileSystem.get(conf1)

    // ===== JOB 1: Lọc Style + Đếm Variety =====
    // Job 1 sử dụng định dạng xuất mặc định (Dấu Tab)
    val job1 = Job.getInstance(conf1, "Job 1: Filter+Count Variety")
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[Job1Mapper])
    job1.setReducerClass(classOf[Job1Reducer])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[Text])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, new Path(args(1)))

    val tempPath = new Path(args(1))
    if (fs.exists(tempPath)) {
      fs.delete(tempPath, true)
    }

    if (!job1.waitForCompletion(true)) {
      System.err.println("Job 1 failed!")
      System.exit(1)
    }
    println("Job 1 completed!")

    // ===== JOB 2: TÍNH MEDIAN =====
    val conf2 = new Configuration()
    // Ép Job 2 xuất ra file CSV dùng dải phân cách là dấu Phẩy (,)
    conf2.set("mapreduce.output.textoutputformat.separator", ",")

    val job2 = Job.getInstance(conf2, "Job 2: Calculate Median")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[Job2Mapper])
    job2.setReducerClass(classOf[Job2Reducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setMapOutputKeyClass(classOf[Text])
    job2.setMapOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))

    val outputPath = new Path(args(2))
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    if (!job2.waitForCompletion(true)) {
      System.err.println("Job 2 failed!")
      System.exit(1)
    }

    println("All jobs completed successfully!")
    System.exit(0)
  }
}
