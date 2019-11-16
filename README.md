# CloudCluster

This guide will run you through setting up MongoDB on a cloud provider and connecting to the database instance remotely via C#.

<hr/>

## Resources
<i>
  
[MongoDB on Compute Engine](https://console.cloud.google.com/marketplace/details/click-to-deploy-images/mongodb)
  
[MongoDB Compute Engine Tutorial](https://blog.codecentric.de/en/2018/03/cloud-launcher-mongodb-google-compute-engine/)

[C# MongoDB Examples](https://www.codementor.io/pmbanugo/working-with-mongodb-in-net-1-basics-g4frivcvz)

[MongoDB Atlas](https://cloud.mongodb.com)
</i>

<hr/>

## Visual Studios setup steps
1. Set up the GCP/AWS/Atlas cluster and whitelist your IP (see section below)
1. Copy the connection string and put it in the connection_string variable in the program file
1. Via the nuget package manager in visual studios, install:
    - CsvHelper
    - MongoDB.Bson
    - MongoDB.Driver
    - NLog
    - NLog.Config
1. If importing .csv or .json files from a folder, edit the sources variable to point to the import folder location
    - Note: Do not have the imported file open in any other programs, or visual studio can't get a lock on the file

## GCP setup steps
From the first link, 'MongoDB on Compute Engine'...
1. Click 'Launch on Compute Engine'
    - It should bring you to a page with 'New MongoDB deployment'
1. Zone: **us-west1-x**

**Servers Tier**
1. Instances Count: 2
1. Machine type: small (1 shared vCPU)
1. Data disk size in GB (20)

**Arbiters Tier**
1. Instances Count: 1
1. Machine type: small (1 shared vCPU)`

**Wait for it to deploy...**
(You should get an e-mail when it is complete)

1. SSH connect to any node and run `mongo`; `rs.status()` to check the replica staus
1. Power off all machines when you are done

### Manage the GCP firewall for external access to the db
1. In GCP, expand the hanburger menu
1. Under Networking, VPC network > Firewall rules
1. Create firewall rule
1. Enter some name, like allow-psu
1. Direction of traffic > ingress
1. Targets > All instances in the network
1. Source filter > IP range
1. For PSU, enter **131.252.0.0/16** _(16 is the CIDR subnet mask for 255.255.0.0, 24 is for 255.255.255.0)_
    - Else, go to **https://who.is/** to see your public IP
1. Protocols and ports > Specified protocols and ports
1. In the box, enter: **tcp:27017**
1. Save

## Atlas setup steps
1. Navigate to the MongoDB Atlas website
1. Build a new cluster
1. Choose a cloud provider (AWS/GCP/Azure)
1. Choose a region that has FREE TIER AVAILABLE
1. Create Cluster

**Wait for it to deploy**
(You should get an e-mail when it is complete)

### Manage the Atlas firewall for external access to the db
1. In MongoDB Atlas, go to Security > Database Access
1. Add a new admin user, remember the password
1. Go to Security > Network Access
1. Add IP Address
1. Press 'Add Current IP Address'
1. If you want to accept a range of IPs, use a CIDR mask and refer to the lowest ip in the range
      - Ex: For anywhere on PSU, do 131.252.0.0/16

### Get the connection string
1. In MongoDB Atlas, go to Altas > Clusters
1. Press 'Connect'
1. For connection via cmd or shell:
      1. Select 'Connect with the Mongo Shell'
      1. Copy the string from step 3
      1. Paste into cmd or shell, enter password when prompted
1. For connection via C# or Python, select 'Connect Your Application'
      1. Select the appropriate Driver (C#; 2.5 or later)
      1. Copy the connection string
      1. Replace <password> with your cluster user's password
1. You do not need to turn on or turn off clusters, Atlas manages this for you
<hr/>

## TODO:
- Try and catch blocks for queries and db connection attempts
- Set up tests for TDD?
- Execute import & queries asynchronously
- Set up Atlas clusters on AWS, GCP, and Azure
- Add code for reporting query performance (ex. python's pychart library)
- Use cursors to parse through large query results
- Log errors onto a local file (set this up in the Nlog.Config file)
