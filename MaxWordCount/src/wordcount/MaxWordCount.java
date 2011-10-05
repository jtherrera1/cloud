package wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;



public  class MaxWordCount
{  

	  public static void main(String[] args) throws Exception {  
		  
	      Configuration conf = new Configuration();  
	      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args  
	      if (otherArgs.length != 4) {  
	        System.err.println("Usage: maxwordcount <in> <in><out><out>");  
	        System.exit(2);  
	      }  
	      
		     // create a job with name "wordcount"
	        
		    Job job = new Job(conf, "word count"); 
		   
		    job.getConfiguration().setStrings("parametertext", otherArgs[2]);
		    job.getConfiguration().setStrings("intWordCount", otherArgs[3]);
		    
		    job.setJarByClass(MaxWordCount.class);  
		    
		    
		    job.setMapperClass(MaxWordCountMapper.class);  
		    job.setReducerClass(MaxWordCountReducer.class);       
		    
		    job.setOutputKeyClass(Text.class);  
		    job.setOutputValueClass(Text.class);  
		     //set the HDFS path of the input data  
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
		     //set the HDFS path of the input data  
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
		
		     // Wait till Job completion  
		
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	      
	      
	  }
}  