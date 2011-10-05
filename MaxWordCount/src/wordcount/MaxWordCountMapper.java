package wordcount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MaxWordCountMapper extends Mapper<LongWritable , Text, Text, Text> {
 

	  
	  private Text word = new Text();                            
	  private Text tmpword = new Text();                           
	  private Text occurences = new Text();    
	  private String parametertext;
      List<String> txtAryWord = new ArrayList<String>();
      List<Integer> txtAryCount= new ArrayList<Integer>();
      public int intWordCount = 0;
      
	  // type of output key
	  public void map(LongWritable  key, Text value,  Context context 
	            ) throws IOException, InterruptedException { 
		  
		  
		Configuration conf = context.getConfiguration();
		parametertext = conf.get("parametertext");
		intWordCount = Integer.parseInt(conf.get("intWordCount"));
		
		
	    StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
	    String strWord;
		int intOccurrences = 0;
	    while (itr.hasMoreTokens()) {  
	    	
	    	 strWord =itr.nextToken();
		     word.set(strWord);    // set word as each input keyword
		     occurences.set(itr.nextToken());    
		     intOccurrences = Integer.parseInt(occurences.toString());
		     
			 if(!parametertext.equals("000000000000"))  
			 {
			    if(word.find(parametertext)==0)
			    {
			    	mxwordOccurence(strWord,intOccurrences);
			    }
			  }
			 else {
				    mxwordOccurence(strWord,intOccurrences);
			  }
			 }
		 }

	  protected void setup(Context context){
	      txtAryCount.add(0);
	      txtAryWord.add("");
	  }
	  
	  protected void  mxwordOccurence(String strword, int intoccurrences){
		  

		  int intTemp =0;

		  
    	  for( int index=0;index<intWordCount ; index++)
    	  {
    		  intTemp = txtAryCount.get(index);
    		  
    		  if( intoccurrences > intTemp)
    		  {
    			  txtAryCount.add(index, intoccurrences);
    			  txtAryWord.add(index,strword);
    			  break;
    		  }

    	  }
		  if(txtAryCount.size()>intWordCount)
		  {
			  txtAryCount.remove(intWordCount);
			  txtAryWord.remove(intWordCount);
		  }
    	  
	  }
	  protected void cleanup(Context context){
		  
		  
		  for(int index =0; index< txtAryCount.size() ; index++){
			  
			  
			  word.set(txtAryWord.get(index)+"@#$%h*&Q!0@%"+txtAryCount.get(index).toString());
			  tmpword.set("VALUE");
			  try {
				context.write(tmpword, word);
				System.out.print(txtAryCount.get(index));
				System.out.print(txtAryWord.get(index));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		  }
		  //context.write
	  }
}
