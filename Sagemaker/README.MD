# Connecting Sagemaker to EMR Cluster
(Refer: https://docs.aws.amazon.com/sagemaker/latest/dg/studio-notebooks-emr-cluster-connect.html)
## To connect Amazon EMR cluster with PySpark kernel selected

1. After you connect to Studio, if you have an existing Studio notebook instance, open that. Otherwise, to create a new notebook instance, select File, and then select New.

2. After you have an open Studio notebook instance, choose a kernel and instance.

To switch your kernel, select in the top right of the UI the currently selected kernel where a pop-up window appears. Then select a kernel of your choice from the kernel drop-down menu. Lastly, select the Select button to make your changes.

![image](https://user-images.githubusercontent.com/30530766/172654002-fcfc5f89-e2ef-4319-9728-e317f7d8723f.png)


3. After you have selected your kernel of choice, select Cluster.

![image](https://user-images.githubusercontent.com/30530766/172654090-17afe776-1b52-4c49-9917-41684654f88f.png)


4. A Connect to cluster UI screen will appear. Choose a cluster and select Connect. Not all Amazon EMR clusters can be connected to Studio. [More Info](http://aws.amazon.com/blogs/machine-learning/amazon-sagemaker-studio-notebooks-backed-by-spark-in-amazon-emr/)  
When you connect to a cluster, it adds a code block to an active cell to establish the connection.

