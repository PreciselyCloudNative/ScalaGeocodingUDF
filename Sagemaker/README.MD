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

5. If the cluster that you're connecting to does not use Kerberos or Lightweight Directory Access Protocol (LDAP) connection, you will be prompted to select the credential type. You can choose HTTP basic authentication or No credential.

![image](https://user-images.githubusercontent.com/30530766/172656395-c1a6ceba-ae89-45b9-92bd-4e35f79e86ec.png)

6. An active cell will populate. This will contain the connection information that you need for connecting to the Amazon EMR cluster that you selected.

    a. When the authentication type is Kerberos and HTTP Basic Auth, a widget will be created in an active cell for you to provide your Username and Password. The following screenshot shows a successful connection after entering these credentials.
    
    ![image](https://user-images.githubusercontent.com/30530766/172656589-44ff2e35-3618-408f-b2e2-d79c2d2d67d3.png)
    
    b. If the cluster that you are connecting to does not use Kerberos or LDAP, and you selected No credentials, you will automatically connect to an Amazon EMR cluster. The following screenshot shows the UI after credentials have been successfully entered.
    
    ![image](https://user-images.githubusercontent.com/30530766/172656697-f39eff09-c50b-4b59-839e-56f70b377eb7.png)

