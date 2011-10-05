package mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;


public class WordCount {

  public static void main(String[] args) throws Exception {  
      Configuration conf = new Configuration();  
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args  
      if (otherArgs.length != 2) {  
        System.err.println("Usage: wordcount <in> <out>");  
        System.exit(2);  
      }  

       // create a job with name "wordcount"  
	    Job job = new Job(conf, "word count");   
	    job.setJarByClass(WordCount.class);  
	    job.setMapperClass(WordCountMapper.class);  
	    job.setReducerClass(WordCountReducer.class);             
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(IntWritable.class);  

	    job.setInputFormatClass(ZipFileInputFormat.class);

	     //set the HDFS path of the input data
	    ZipFileInputFormat.setInputPaths(job, new Path(otherArgs[0])); 
	    
	    //FileInputFormat.addInputPath(job, new Path(argssx[0]));  
	     //set the HDFS path of the input data  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	 

       // Wait till Job completion  
      System.exit(job.waitForCompletion(true) ? 0 : 1);  
  }
}