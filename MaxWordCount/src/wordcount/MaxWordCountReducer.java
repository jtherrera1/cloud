package wordcount;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public  class MaxWordCountReducer  extends Reducer<Text,Text,Text,Text> 
{  
   private Text result = new Text();  
   public int intWordCount = 0;
   List<String> txtAryWord = new ArrayList<String>();
   List<Integer> txtAryCount= new ArrayList<Integer>();
   String strToken="@#$%h*&Q!0@%";
   //implement the reduce function  
   public void reduce(Text key, Iterable<Text> values, Context context
                     ) throws IOException, InterruptedException 
   {  
	  Configuration conf = context.getConfiguration();
	  intWordCount = Integer.parseInt(conf.get("intWordCount"));
	  String txtComparar = ""; // initialize the sum for each keyword  
      txtAryCount.add(0);
     txtAryWord.add("");
      int intValue =0;
      int intTemp =0;
      String strWord;
      String strCount;      
      for (Text val : values) 
      {  
    	  txtComparar = val.toString();
    	  int itoken = txtComparar.indexOf(strToken);
    	  strWord = txtComparar.substring(0,itoken);
    	  strCount = txtComparar.substring(itoken+strToken.length(),txtComparar.length());
			System.out.print(strCount);
    	  intValue = Integer.parseInt(strCount);

    	  for( int index=0;index<intWordCount ; index++)
    	  {

    		  if(index >=txtAryCount.size())
    			  break;
    		  
    		  intTemp = txtAryCount.get(index);
    		  
    		  if( intValue > intTemp)
    		  {
    			  txtAryCount.add(index, intValue);
    			  txtAryWord.add(index,strWord);
    			  break;
    		  }
    	  }
		  if(txtAryCount.size()>intWordCount)
		  {
			  txtAryCount.remove(intWordCount);
			  txtAryWord.remove(intWordCount);
		  }
        }
	   
      for(int j=0;j<txtAryWord.size();j++)
      {
          key.set(txtAryWord.get(j));
          result.set(Integer.toString(txtAryCount.get(j)) );  
          context.write((Text)key, result);
      }

   }
   
   
}
