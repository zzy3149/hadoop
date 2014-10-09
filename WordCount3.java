/*Notice: this program can only sort all the 7-character words by their frequency, but cant pick top 100s. 
You may use command bin/hadoop fs -cat /output/part-r-00000 | head n100 to get the corrrect result. 
I am still working on the code. */

/*Notice2: If you want to compile the code. Please use the command"javac -cp hadoop-1.0.3/hadoop-core
-1.0.3.jar:hadoop-1.0.3/lib/commons-cli-1.2.jar WordCount3.java"*/
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class WordCount3 {
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
       
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
           
            String line = value.toString();
			
            StringTokenizer tokenizer = new StringTokenizer(line);
			

            while (tokenizer.hasMoreTokens()) {
			String nextWord=tokenizer.nextToken();
                if (nextWord.length() == 7) {
				word.set(nextWord);
                context.write(word, one);
				}
            }
        }
    }
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
     private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
          public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
          }
          
          public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
              return -super.compare(b1, s1, l1, b2, s2, l2);
          }
      }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    
         Path tempDir = new Path("wordcount-temp-" + Integer.toString(
                    new Random().nextInt(Integer.MAX_VALUE))); 		//temporary directory
        
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount3.class);
        try{
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, tempDir);
                                                         
            job.setOutputFormatClass(SequenceFileOutputFormat.class); //exchange key and value to sort
            if(job.waitForCompletion(true))
            {
                Job sortJob = new Job(conf, "sort");
                sortJob.setJarByClass(WordCount3.class);
                
                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                
               
                sortJob.setMapperClass(InverseMapper.class);
                
                sortJob.setNumReduceTasks(1); 
                FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));
                
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
               
                sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
     
                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        }finally{
            FileSystem.get(conf).deleteOnExit(tempDir);
        }
    }
}
