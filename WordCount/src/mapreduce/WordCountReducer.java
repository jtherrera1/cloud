package mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public  class WordCountReducer  extends Reducer<Text,IntWritable,Text,IntWritable> 
{  
   private IntWritable result = new IntWritable();  
   //implement the reduce function  
   public void reduce(Text key, Iterable<IntWritable> values,  
                	  Context context  
                     ) throws IOException, InterruptedException 
   {  
      int sum = 0; // initialize the sum for each keyword  
      for (IntWritable val : values) 
      {  
          sum += val.get();  
      }  
      result.set(sum);  
      context.write(key, result); // create a pair <keyword, number of occurences>  
   }
}
