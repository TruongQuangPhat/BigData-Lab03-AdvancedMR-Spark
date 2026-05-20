package lab03

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{Text, IntWritable, LongWritable}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * BÀI 1: SLIDING WINDOW - Cửa sổ trượt
 * ========================================
 * PHÂN TÍCH BÀI TOÁN:
 * - Xác định size được mua nhiều nhất tại mỗi bang trong cửa sổ 7 ngày
 * - Cửa sổ trượt: mỗi ngày D sẽ xét 7 ngày trước đó (từ D-7 đến D-1)
 * - Điều kiện: Status chứa "shipped" và Qty > 0
 * 
 * PHÂN RÃ BÀI TOÁN (DECOMPOSITION):
 * 1. Map Phase: 
 *    - Đọc từng đơn hàng, lọc theo điều kiện (shipped + qty > 0)
 *    - Với mỗi đơn hàng ở ngày D, emit key cho 7 ngày tương lai (D+1 đến D+7)
 *    - Key = (State, TargetDate), Value = Size
 *    - Lý do: Một đơn hàng ở ngày D sẽ thuộc cửa sổ của các ngày D+1, D+2, ..., D+7
 * 
 * 2. Reduce Phase:
 *    - Nhận tất cả sizes trong cửa sổ 7 ngày của mỗi (State, TargetDate)
 *    - Đếm tần suất xuất hiện của mỗi size
 *    - Chọn size có tần suất cao nhất
 *    - Output: State, TargetDate, MostBoughtSize, Count
 */
object SlidingWindowJob {
  
  /**
   * MAPPER: Phát sinh các cặp (State_TargetDate, Size) cho 7 ngày tương lai
   */
  class SlidingWindowMapper extends Mapper[LongWritable, Text, Text, Text] {
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
        
        val dateStr = fields(2).trim        // Date
        val status = fields(3).trim         // Status
        val size = fields(10).trim          // Size
        val qtyStr = fields(13).trim        // Qty
        val stateRaw = fields(17).trim      // ship-state
        
        // Chuẩn hóa tên bang .toUpperCase() để tránh inconsistency
        val state = stateRaw.toUpperCase
        
        // Điều kiện lọc 1: Status chứa "shipped" (case-insensitive)
        if (!status.toLowerCase.contains("shipped")) return
        
        // Điều kiện lọc 2: Qty > 0
        val qty = try { qtyStr.toInt } catch { case _: Exception => 0 }
        if (qty <= 0) return
        
        // Điều kiện lọc 3: State không rỗng hoặc null
        if (state.isEmpty || state.equalsIgnoreCase("NULL")) return
        
        // Điều kiện lọc 4: Size không rỗng
        if (size.isEmpty) return
        
        // Parse ngày tháng
        val orderDate = try {
          dateFormat.parse(dateStr)
        } catch {
          case _: Exception => return
        }
        
        val cal = Calendar.getInstance()
        cal.setTime(orderDate)
        
        // Emit cho 7 ngày tương lai (D+1 đến D+7)
        for (i <- 1 to 7) {
          cal.setTime(orderDate)
          cal.add(Calendar.DAY_OF_MONTH, i)
          val targetDate = dateFormat.format(cal.getTime)
          
          outKey.set(s"${state}_${targetDate}")
          outValue.set(size)
          context.write(outKey, outValue)
        }
        
      } catch {
        case e: Exception => 
          System.err.println(s"Error processing line: ${e.getMessage}")
      }
    }
    
    /**
     * Parse CSV line với state machine để xử lý quoted fields
     * Ví dụ: "field1","field2,with,comma","field3" → ["field1", "field2,with,comma", "field3"]
     */
    private def parseCSVLine(line: String): Array[String] = {
      val result = new mutable.ArrayBuffer[String]()
      var current = new StringBuilder()
      var inQuotes = false
      
      for (c <- line) {
        c match {
          case '"' => 
            inQuotes = !inQuotes
          case ',' if !inQuotes =>
            result += current.toString
            current = new StringBuilder()
          case _ => 
            current.append(c)
        }
      }
      result += current.toString
      result.toArray
    }
  }
  
  /**
   * REDUCER: Đếm tần suất và chọn size được mua nhiều nhất
   */
  class SlidingWindowReducer extends Reducer[Text, Text, Text, Text] {
    
    /**
     * Ghi header trong setup() - đảm bảo header luôn ở đầu file
     */
    override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      context.write(new Text("State"), new Text("TargetDate,MostBoughtSize,Count"))
    }
    
    override def reduce(
      key: Text, 
      values: java.lang.Iterable[Text], 
      context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      try {
        // Đếm tần suất xuất hiện của mỗi size
        val sizeCount = new mutable.HashMap[String, Int]()
        
        values.asScala.foreach { size =>
          val sizeStr = size.toString
          sizeCount(sizeStr) = sizeCount.getOrElse(sizeStr, 0) + 1
        }
        
        // Tìm size có tần suất cao nhất
        if (sizeCount.nonEmpty) {
          val (mostBoughtSize, maxCount) = sizeCount.maxBy(_._2)
          
          // Parse composite key để lấy State và TargetDate
          val keyParts = key.toString.split("_", 2)
          if (keyParts.length == 2) {
            val state = keyParts(0)
            val targetDate = keyParts(1)
            
            // Output CSV format chuẩn với comma separator
            // Key: State, Value: TargetDate,MostBoughtSize,Count
            // Kết hợp với config "mapreduce.output.textoutputformat.separator" = ","
            // sẽ tạo output: State,TargetDate,MostBoughtSize,Count
            context.write(
              new Text(state), 
              new Text(s"${targetDate},${mostBoughtSize},${maxCount}")
            )
          }
        }
        
      } catch {
        case e: Exception =>
          System.err.println(s"Error in reducer: ${e.getMessage}")
      }
    }
  }
  
  /**
   * MAIN: Cấu hình và chạy MapReduce job
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SlidingWindowJob <input path> <output path>")
      System.exit(-1)
    }
    
    val conf = new Configuration()
    
    // Thiết lập comma separator thay vì tab mặc định
    // Mặc định Hadoop dùng "\t" để phân cách key-value trong output
    // Ta ép dùng "," để tạo CSV chuẩn
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    
    val job = Job.getInstance(conf, "Sliding Window - Most Bought Size by State")
    
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[SlidingWindowMapper])
    job.setReducerClass(classOf[SlidingWindowReducer])
    
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    
    val fs = FileSystem.get(conf)
    val outputPath = new Path(args(1))
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
      println(s"Deleted existing output directory: $outputPath")
    }
    
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
