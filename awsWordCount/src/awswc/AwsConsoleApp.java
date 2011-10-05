package awswc;
/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowExecutionStatusDetail;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions.ConfigFile;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions.ConfigureDaemons;
import com.amazonaws.services.elasticmapreduce.util.BootstrapActions.Daemon;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;

/**
 * Welcome to your new AWS Java SDK based project!
 *
 * This class is meant as a starting point for your console-based application that
 * makes one or more calls to the AWS services supported by the Java SDK, such as EC2,
 * SimpleDB, and S3.
 *
 * In order to use the services in this sample, you need:
 *
 *  - A valid Amazon Web Services account. You can register for AWS at:
 *       https://aws-portal.amazon.com/gp/aws/developer/registration/index.html
 *
 *  - Your account's Access Key ID and Secret Access Key:
 *       http://aws.amazon.com/security-credentials
 *
 *  - A subscription to Amazon EC2. You can sign up for EC2 at:
 *       http://aws.amazon.com/ec2/
 *
 *  - A subscription to Amazon SimpleDB. You can sign up for Simple DB at:
 *       http://aws.amazon.com/simpledb/
 *
 *  - A subscription to Amazon S3. You can sign up for S3 at:
 *       http://aws.amazon.com/s3/
 */
public class AwsConsoleApp {

    /*
     * Important: Be sure to fill in your AWS access credentials in the
     *            AwsCredentials.properties file before you try to run this
     *            sample.
     * http://aws.amazon.com/security-credentials
     */

    static AmazonEC2      ec2;
    static AmazonS3       s3;
    static AmazonSimpleDB sdb;
    static AmazonElasticMapReduce emr;
    
    private static final String HADOOP_VERSION = "0.20";
    private static final int MASTER_INSTANCE_COUNT = 15;

    private static final UUID RANDOM_UUID = UUID.randomUUID();
    
    private static final String FLOW_NAME = "cloudburst-" + RANDOM_UUID.toString();
    private static final String BUCKET_NAME = "s3n://dswc7gb/";
    private static final String S3N_WORD_COUNT_JAR_ =BUCKET_NAME + "WC.jar";
    private static final String S3N_MAX_WORD_COUNT_JAR =BUCKET_NAME +"XWC.jar";
    
    private static final String CONFIG_HADOOP_BOOTSTRAP_ACTION =
            "s3://elasticmapreduce/bootstrap-actions/configure-hadoop";
    private static final String CONFIG_JVM_BOOTSTRAP_ACTION =
    "s3://elasticmapreduce/bootstrap-actions/configure-daemons";
    

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    public static void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                AwsConsoleApp.class.getResourceAsStream("AwsCredentials.properties"));
        ec2 = new AmazonEC2Client(credentials);
        s3  = new AmazonS3Client(credentials);
        sdb = new AmazonSimpleDBClient(credentials);
        emr = new AmazonElasticMapReduceClient(credentials);
   }

    public static void main(String[] args) throws Exception {

        System.out.println("===========================================");
        System.out.println("Welcome to the AWS Java SDK!");
        System.out.println("===========================================");
        init();
        runJobFlow();
 
    }
    static void runJobFlow() throws InterruptedException{
    	// Configure instances to use
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig();
        //********************************************************************//
        instances.setHadoopVersion(HADOOP_VERSION);
        instances.withEc2KeyName("ayuda-vp1");
        instances.setInstanceCount(MASTER_INSTANCE_COUNT);
        //instances.setInstanceGroups(instanceGroups)
        instances.setMasterInstanceType(InstanceType.M24xlarge.toString());
        instances.setSlaveInstanceType(InstanceType.M24xlarge.toString());
        //********************************************************************//
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
        .withJar(S3N_WORD_COUNT_JAR_) // This should be a full map reduce application.
        .withArgs(BUCKET_NAME+"inWC", BUCKET_NAME+"outWC");
        
        StepConfig stepConfig1 = new StepConfig()
        .withName("wordcount")
        .withHadoopJarStep(hadoopJarStep1)
        .withActionOnFailure("TERMINATE_JOB_FLOW");

        //********************************************************************//
        
        //********************************************************************//
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
        .withJar(S3N_MAX_WORD_COUNT_JAR) // This should be a full map reduce application.
        .withArgs(BUCKET_NAME+"outWC", BUCKET_NAME+"outXWC","hate","10");
        
        
        StepConfig stepConfig2 = new StepConfig()
        .withName("maxwordcount")
        .withHadoopJarStep(hadoopJarStep2)
        .withActionOnFailure("TERMINATE_JOB_FLOW");
        //********************************************************************//
        
        Collection<StepConfig> csc = new ArrayList<StepConfig>();
        csc.add(stepConfig1);
        csc.add(stepConfig2);
        
       // BootstrapActions bootstrapActions = new BootstrapActions();
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
        .withName(FLOW_NAME)
        .withInstances(instances)
        .withSteps(csc)
        .withLogUri(BUCKET_NAME+"debug")
        /*.withBootstrapActions(
              bootstrapActions.newRunIf(
                  "instance.isMaster=true",
                  bootstrapActions.newConfigureDaemons()
                      .withHeapSize(Daemon.JobTracker, 4096)
                      .build()),
                      bootstrapActions.newRunIf(
                              "instance.isRunningNameNode=true",
                              bootstrapActions.newConfigureDaemons()
                              .withHeapSize(Daemon.NameNode, 4096).build()),
              bootstrapActions.newRunIf(
                                             "instance.isRunningDataNode=true",
                                             bootstrapActions.newConfigureDaemons()
                                             .withHeapSize(Daemon.DataNode, 4096).build()),
              bootstrapActions.newRunIf(
                                             "instance.isRunningJobTracker=true",
                                             bootstrapActions.newConfigureDaemons()
                                             .withHeapSize(Daemon.JobTracker, 4096).build()),
              bootstrapActions.newRunIf(
                                                     "instance.isRunningTaskTracker=true",
                                                     bootstrapActions.newConfigureDaemons()
                                                     .withHeapSize(Daemon.TaskTracker, 4096).build())                                             
                                             
                                             /*,
                                             
              bootstrapActions.newRunIf(
                                                     "instance.isSlave=true",
              bootstrapActions.newConfigureHadoop()
                                               .withKeyValue(ConfigFile.Site,"mapred.tasktracker.map.tasks.maximum", "4"))                                            
                      )*/;
    
        
        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
	    
	    String jobFlowId = runJobFlowResult.getJobFlowId();
	    System.out.println("Ran job flow with id: " + jobFlowId);
	    
	    //wasFinished(runJobFlowResult);
	 
    }
    
    public static void wasFinished(RunJobFlowResult runJobFlowResult) throws InterruptedException{
        DescribeJobFlowsRequest describeJobFlowsRequest = new DescribeJobFlowsRequest().withJobFlowIds(runJobFlowResult.getJobFlowId());
        int secondsBetweenPolling = 120;
        String state = null;
        do {
           Thread.sleep(secondsBetweenPolling * 1000);
           DescribeJobFlowsResult jobFlowsResult = emr.describeJobFlows(describeJobFlowsRequest);
           JobFlowDetail detail = jobFlowsResult.getJobFlows().get(0);
           JobFlowExecutionStatusDetail executionStatusDetail = detail.getExecutionStatusDetail();
            state = executionStatusDetail.getState();
        } while (state != null && !state.equals("COMPLETE") && !state.equals("FAILED") && !state.equals("TERMINATED"));
    }

    

}

