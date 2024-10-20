import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.text.DecimalFormat;

import java.io.IOException;

public class CashFlow {

    public static class SkipFirstLineInputFormat extends TextInputFormat {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new SkipFirstLineRecordReader();  // 返回一个符合要求的 RecordReader
        }

        // 静态的 RecordReader 类，继承 LineRecordReader
        public static class SkipFirstLineRecordReader extends LineRecordReader {
            private boolean firstLineSkipped = false;

            @Override
            public boolean nextKeyValue() throws IOException {
                // 跳过第一行
                if (!firstLineSkipped) {
                    firstLineSkipped = true;
                    // 调用一次 nextKeyValue 跳过第一行
                    super.nextKeyValue();  
                    return super.nextKeyValue();  // 返回第二行及以后的内容
                } else {
                    return super.nextKeyValue();  // 正常处理
                }
            }
        }
    }

    public static class CashFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
        if (fields.length < 14) return;

        String date = fields[1]; 
        double purchaseAmt = fields[4].isEmpty() ? 0 : Double.parseDouble(fields[4]); 
        double redeemAmt = fields[8].isEmpty() ? 0 : Double.parseDouble(fields[8]);
            context.write(new Text(date), new Text(purchaseAmt + "," + redeemAmt));
        }
    }

    public static class CashFlowCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalPurchase = 0;
        double totalRedeem = 0;

        for (Text value : values) {
            String[] amounts = value.toString().split(",");
            totalPurchase += Double.parseDouble(amounts[0]);
            totalRedeem += Double.parseDouble(amounts[1]);
        }

        context.write(key, new Text(totalPurchase + "," + totalRedeem));
    }
    }

    public static class CashFlowReducer extends Reducer<Text, Text, Text, Text> {
        private DecimalFormat decimalFormat = new DecimalFormat("#.##");
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalPurchase = 0;
            double totalRedeem = 0;

            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                totalPurchase += Double.parseDouble(amounts[0]);
                totalRedeem += Double.parseDouble(amounts[1]);
            }

        String formattedPurchase = decimalFormat.format(totalPurchase);
        String formattedRedeem = decimalFormat.format(totalRedeem);
        
        context.write(key, new Text(formattedPurchase + "," + formattedRedeem));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("you should enter <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Cash Flow Statistics");

        job.setJarByClass(CashFlow.class);
        job.setMapperClass(CashFlow.CashFlowMapper.class);
        job.setCombinerClass(CashFlow.CashFlowCombiner.class);
        job.setReducerClass(CashFlow.CashFlowReducer.class);
        job.setInputFormatClass(SkipFirstLineInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}