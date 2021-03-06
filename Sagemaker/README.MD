# Setup
## Prerequisite
<h3> 1. Create Route table:</h3>
    
   ![image](https://user-images.githubusercontent.com/30530766/172730975-fd3c8bf0-4436-4ab1-a4cc-c0ad323873f9.png)
    
    
<h3> 2. Create Private Subnet</h3> <br>

   ![image](https://user-images.githubusercontent.com/30530766/172730674-5f374257-4be4-4456-abc1-b7e1fcb63221.png) 
    
   ![image](https://user-images.githubusercontent.com/30530766/172730779-0d8675de-ff6c-447c-9cee-4bab18b09a0c.png)

(<b>Note:</b> For  IPV4 CIDR BLOCK we use incremental IP, for example, 172.31.0.0/20, 172.31.16.0/20, 172.31.32.0/20, 172.31.48.0/20, 172.31.64.0/20, 172.31.80.0/20....<br> And we can create a subnet with the next incremental IP that is available, in our case, 172.31.92.0/20)
<h3> 3.Create S3 Endpoint </h3> <br>

![image](https://user-images.githubusercontent.com/30530766/172734298-87314acd-5f36-4336-9a3a-ee9128d51eee.png)
![image](https://user-images.githubusercontent.com/30530766/172734349-dcfdb8c1-3497-4740-a569-e5e7698596d7.png)
![image](https://user-images.githubusercontent.com/30530766/172734369-0257ca44-1521-4a5b-ad86-7c3f75f2b875.png)

<h3> 4. Create NAT Gateway </h3> <br>
	
![image](https://user-images.githubusercontent.com/30530766/172885800-806e580b-baf1-4ae4-9495-db10eabcb1b6.png)

<!-- ![image](https://user-images.githubusercontent.com/30530766/172885857-b0ce3d9c-6410-4a72-8720-0dfd2236faf4.png) -->

<h3> 5. Edit Route Table that we created in step 1 and add route to nat gateway that we created in previous step (emr-nat-gateway) </h3> <br>

![image](https://user-images.githubusercontent.com/30530766/172920663-4e9beebf-b107-465e-87fb-8df7ab06e8a1.png)

<h3> 6. Create EMR Cluster </h3>
	<h4> 6a. Configuration: </h4>
	
![image](https://user-images.githubusercontent.com/30530766/172920907-17bdc7b7-1952-47ec-9a86-bcdbaebd9ae6.png)

<h4> 6b. Networking: </h4>
- Select Private EC2 subnet (sagemaker-private)
	
![image](https://user-images.githubusercontent.com/30530766/172921345-b30510cd-5dc1-492a-b66f-88874e86ce8a.png)

<h4> 6c. Select/Create Security Groups: </h4>
i. ElasticMapReduce-Master-Private  <br>
ii. ElasticMapReduce-Slave-Private  <br>


![image](https://user-images.githubusercontent.com/30530766/172921584-9fd199ef-53cf-4470-8991-3f9fa68991e8.png)

<h3> 7. Create another endpoint named 'sagemaker-studio-endpoint': </h3>

![image](https://user-images.githubusercontent.com/30530766/172933446-bd9534b7-3c40-4b04-841a-2f5f2e25609f.png)

![image](https://user-images.githubusercontent.com/30530766/172933471-8767328b-bd03-4b80-87bd-c544f34dfc20.png)

![image](https://user-images.githubusercontent.com/30530766/172933502-1ad08e18-b572-4c15-b54b-6c89c4c2faab.png)

![image](https://user-images.githubusercontent.com/30530766/172933551-57c8ea70-12fa-46e2-a17b-16850c29d5bf.png)

![image](https://user-images.githubusercontent.com/30530766/172933574-0505fa72-fd97-4909-917b-c893db499e14.png)


## Sagemaker Studio

1. Open AWS Sagemaker from AWS Console.
2. Launch Sagemaker studio from the left pane. <br>
![MicrosoftTeams-image (1)](https://user-images.githubusercontent.com/30530766/172948525-21d0e8f2-bca4-4d46-a47b-0e835e78c009.png)

3. Select Standard setup and click on configure

![MicrosoftTeams-image (3)](https://user-images.githubusercontent.com/30530766/172949190-82efa207-db72-4dda-a66f-cbfb86b61688.png)

4. Create IAM role and give full access to S3

![MicrosoftTeams-image (4)](https://user-images.githubusercontent.com/30530766/172953554-32eb3262-d67a-427d-a48b-f0b334b441d2.png)

5. Network and security:<br>
5a. Select default VPC<br>
5b. Security group: ElasticMapReduce-Master-Private & ElasticMapReduce-Slave-Private<br>
5c. Choose VPC only <br>
![MicrosoftTeams-image (6)](https://user-images.githubusercontent.com/30530766/172953769-327fe80b-57a1-4f53-a862-39ce32c1866d.png)

<br>
<b>Keep rest of the setting deafult</b>

![MicrosoftTeams-image (7)](https://user-images.githubusercontent.com/30530766/172954010-1bd8f611-d1d9-432e-af65-9b6b1d299721.png)

![MicrosoftTeams-image (8)](https://user-images.githubusercontent.com/30530766/172954015-a4e3087a-1c2d-495b-b482-ed2cfd2fe37b.png)

![MicrosoftTeams-image (9)](https://user-images.githubusercontent.com/30530766/172954022-11d839ab-16b3-42f8-a376-bccb61474295.png)

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

