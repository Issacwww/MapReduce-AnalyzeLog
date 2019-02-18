#### INFSCI 2750 Project1 @ Pitt

This project is a Hadoop MapReduce program to analyze a log file(get from this [link](https://drive.google.com/open?id=1O6hqRANESPg38seu_UyjvTkxoctkntMD)). 
You could either run it locally or upload the jar to the Hadoop cluster and execute.

##### Usage(Hadoop cluster)

Suppose you are in the directory of the jar file, and put the log file to HDFS file system, say `inputlog` then 

```bash
hadoop jar AnalyzeLog.jar Task<id> inputlog/ outputlog/task<id> 
```

Where the id is in the range of 1 to 4, corresponding to following tasks:

![image-20190218021329600](/Users/ssuchao/Library/Application Support/typora-user-images/image-20190218021329600.png)

Then use the following command to check the output:

```bash
hdfs dfs -cat outputlog/task<id>/*
```



