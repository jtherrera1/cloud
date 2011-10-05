package mapreduce;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;

public  class WordCountMapper  extends Mapper<Text, BytesWritable, Text, IntWritable>
{  
  private final static IntWritable one = new IntWritable(1); // type of output value  
  private Text word = new Text();                            // type of output key 
  private FileSystem fs ;
  private FSDataInputStream currentInputStream;
  private FSDataOutputStream fileOutStream;
  private BufferedReader currentReader;
  
  public void map(Text key, BytesWritable value, Context context  
            ) throws IOException, InterruptedException 
  {
	  /******************************************************************/
	  fs = FileSystem.get(context.getConfiguration());

	  byte[] data = value.getBytes();
	  Path file = new Path(key.toString());
	  fileOutStream = fs.create(file);
	  fileOutStream.write(data, 0, value.getLength());
	  fileOutStream.close();
	  currentInputStream = fs.open(file);
	  currentReader = new BufferedReader(new  InputStreamReader(currentInputStream));
	  /******************************************************************/
	  
	  String thisLine;
      while ((thisLine = currentReader.readLine()) != null) { // while loop begins here
    		
    	    StringTokenizer itr = new StringTokenizer(thisLine.toString()); // line to string token  
    	    while (itr.hasMoreTokens()) 
    	    {  
    	     word.set(itr.nextToken());    // set word as each input keyword  
    	     context.write(word, one);     // create a pair <keyword, 1>  
    	    }  
        } // end while
	  
	  currentReader.close();
	  fs.deleteOnExit(file);
	  fs.close();

  }  
}  