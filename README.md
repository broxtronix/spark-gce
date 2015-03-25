Spark GCE
=========

Spark GCE allows you to create and manage [Spark](http://spark.apache.org/) on
Google Compute Engine. This script serves a similar function to the spark-ec2
script that comes bundled with Spark, but the Spark cluster environment it
creates is different in several key respects:

 - Cluster instances run Ubuntu 14.04 rather than Amazon Linux (CentOS)
 - The default python (and pyspark) environment is [Anaconda 2.1.0](http://continuum.io/) with python 2.7
 - At the moment, Hadoop, Shark, and Tachyon are not pre-installed (pull requests welcome)

This is a fork of the Spark GCE script originally written by [Sigmoid
Analytics](https://github.com/sigmoidanalytics/spark_gce), with many performance
enhancements and new additions.  The changes include:

- Update to Spark 1.3, and pyspark uses Anaconda 2.1.0 as its default Python interpreter
- Command syntax and option parsing now more closely follows the conventions used in the spark-ec2 script
- Script commands can now run parallel, so multiple operations can be performed simultaneously.  This greatly reduces the time it takes to launch, start, stop, and destroy clusters, especially when there are many slave nodes.
- Addition of a 'start' and 'stop' command, which allow a cluster to be temporarily suspened while preserving the contents of its root disks (data on scratch disks does not persist)
- Addition of a 'login' and 'mosh' command to log into a running cluster with ssh or mosh, respectively
- A ssh port forwarding mode that can be optionally used with 'spark-gce login' by adding the flag: --ssh-port-forwarding <local_port>:<remote_port> to
- Ganglia cluster monitoring at http://<master_node_ip>:5080/ganglia
- Faster (SSD) scratch drives

Getting Started
---------------

In order to use this script, you must first install the [Google Cloud
SDK](https://cloud.google.com/sdk/). After installing, be sure to authenticate
with

```
gcloud auth login
```

I also recommend configuring a default project, region, and zone for all
`gcloud` commands, which saves you from having to include these additional
command line flages when calling `spark-gce`.  

```
gcloud config set project <project-id>
gcloud config set compute/region <region>
gcloud config set compute/zone <zone>
```

Having done this, you should be able to start a new cluster with this short command

```
spark-gce start <cluster_name> -s <num_slaves>
```

Once the cluster is up and running, you can ssh into it either using:

```
spark-gce login <cluster_name> 
```
or
```
gcloud compute ssh <cluster_name>-master
```

You can temporarily suspend your cluster while preserving the contents of its root disks, and then start it back up again.

```
spark-gce stop <cluster_name>
spark-gce start <cluster_name>
```

Finally, you can terminate your cluster entirely, which permenantly shuts down your instances and deletes all cluster disks.

```
spark-gce destroy <cluster_name>
```

Want to help?
-------------

This script is very much a work in progress. It supports my current use case, but will
hopefully be made more flexible down the road. Pull requests are welcome. Here
are some contributions that would be particularly helpful:

- Better testing and support for different GCE instance types, regions, and zones
- Allow the user to specify which Spark version to install
- Support for local NVMe SSDs (already partially implemented)
- Inclusion of Hadoop, Shark, Tachyon, and other mainstays of the Apache open source ecosystem.
- The option to create a larger root disk.  The current root disk size is 10GB, but this can be (expanded)[http://stackoverflow.com/questions/24021214/how-to-get-a-bigger-boot-disk-on-google-compute-engine]
- More aesthetically pleasing cluster monitoring using [Graphite](http://graphite.wikidot.com/) and [Grafana](http://grafana.org/)
