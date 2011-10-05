package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.junit.Test;

public class testwc 
{
	
	String[] argssx = new String[2];
	String[] otherArgs = new String[2];
    Configuration conf = new Configuration();
    
    @Test
    public void testWordCount() throws IOException, InterruptedException, ClassNotFoundException
    {
    	argssx[0]="ftpfile";
    	argssx[1]="saida1";
	    
	    if (argssx.length != 2) {  
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
	    ZipFileInputFormat.setInputPaths(job, new Path(argssx[0])); 
	    
	    //FileInputFormat.addInputPath(job, new Path(argssx[0]));  
	     //set the HDFS path of the input data  
	    FileOutputFormat.setOutputPath(job, new Path(argssx[1]));  
	
	     // Wait till Job completion  
	
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
