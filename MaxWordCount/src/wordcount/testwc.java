package wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class testwc 
{
	
	String[] argssx = new String[4];
	String[] otherArgs = new String[4];
    Configuration conf = new Configuration();
    
    @Test
    public void testMaxWordCount() throws IOException, InterruptedException, ClassNotFoundException
    {
    	argssx[0]="saida1";
    	argssx[1]="resumen";
    	argssx[2]="hate";
    	argssx[3]="10";
	    
	    if (argssx.length != 4) {  
	      System.err.println("Usage: wordcount <in> <out>");  
	      System.exit(2);  
	    }  
	

	     // create a job with name "wordcount"  
	    Job job = new Job(conf, "word count");  
	    	    
	    job.getConfiguration().setStrings("parametertext", argssx[2]);
	    job.getConfiguration().setStrings("intWordCount", argssx[3]);
	    
	    job.setJarByClass(MaxWordCount.class);  
	    job.setMapperClass(MaxWordCountMapper.class);  
	    job.setReducerClass(MaxWordCountReducer.class);       
	    
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);  
	     //set the HDFS path of the input data  
	    FileInputFormat.addInputPath(job, new Path(argssx[0]));  
	     //set the HDFS path of the input data  
	    FileOutputFormat.setOutputPath(job, new Path(argssx[1]));  
	
	     // Wait till Job completion  
	
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
