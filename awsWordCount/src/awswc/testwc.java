package awswc;


import org.junit.Test;



public class testwc 
{

	    
    @Test
    public void testAWSProcess() throws Exception
    {
	
    	AwsConsoleApp.init();
    	AwsConsoleApp.runJobFlow();
    }
}
