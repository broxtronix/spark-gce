spark-gce
=========

This script helps you create a [Spark](http://spark.apache.org/) cluster on
Google Compute Engine. It serves a similar function to the spark-ec2
script that comes bundled with Spark, but the Spark cluster environment it
creates is different in several key respects:

 - Instances run on Google Compute Engine rather than Amazon's EC2
 - Instances run Ubuntu 14.04 rather than Amazon Linux (CentOS)
 - The default pyspark environment is [Anaconda 2.1.0](http://continuum.io/) with python 2.7
 - At the moment Shark and Tachyon are not pre-installed (pull requests welcome)

This is a fork of the Spark GCE script originally written by [Sigmoid
Analytics](https://github.com/sigmoidanalytics/spark_gce).  This fork
has been significantly re-architected in order to enable the following 
performance enhancements and new additions:

- Command syntax and option parsing now more closely follows the conventions used in the spark-ec2 script.
- Script commands can now run parallel, so multiple operations can be performed simultaneously.  This greatly reduces the time it takes to launch, start, stop, and destroy clusters, especially when there are many slave nodes.
- Spark, Hadoop, and other tools are installed under /opt, which allows multiple user accounts to access and run these tools from the same master node.
- Addition of a 'start' and 'stop' command, which allow a cluster to be temporarily suspened while preserving the contents of its root disks (data on scratch disks does not persist).
- Addition of a 'login' and 'mosh' command to log into a running cluster with ssh or [mosh](https://mosh.mit.edu/), respectively
- Root drives on the instances can be larger that 10GB.  (The current default is 50GB)
- Availability of the familiar 'copy-dir' and 'parallel-ssh' (pssh from spark-ec2) commands
- Support for ssh port forwarding (useful for connecting to ipython notebook without having to configure SSL) via the `spark-gce login` flag: --ssh-port-forwarding  <local_port>:<remote_port>
- The Google Storage adaptor for Hadoop is installed, making the "gs://..." namespace available in HDFS
- Ganglia cluster monitoring
- Mount the home directory on your master node locally using [sshfs](http://fuse.sourceforge.net/sshfs.html). (Useful when editing source files and debugging code on remote clusters...)
- Faster (SSD) scratch drives attached to /mnt with configurable sizes up to 1TB


Getting Started
---------------

In order to use this script, you must first install the [Google Cloud
SDK](https://cloud.google.com/sdk/). After installing, be sure to authenticate
with

```
gcloud auth login
```

I also recommend configuring a default project, region, and zone that applies for all
`gcloud` commands. This saves you from having to type additional command line flags when calling `spark-gce`.  

```
gcloud config set project <project-id>
gcloud config set compute/region <region>
gcloud config set compute/zone <zone>
```

Having done this, you should be able to create a new cluster with this succinct command

```
spark-gce launch <cluster_name> -s <num_slaves>
```

Once the cluster is up and running, you can ssh into it either using:

```
spark-gce login <cluster_name> 
```
or
```
gcloud compute ssh <cluster_name>-master
```
or if you prefer to use [mosh](https://mosh.mit.edu/)
```
gcloud compute mosh <cluster_name>-master
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

Configuring Spark
-----------------

The spark_gce script sets up a Spark environment with reasonable defaults for
most basic settings. However, you may need to customize your Spark settings for
your application. To do this, edit the `$HOME/spark/conf/spark-env.sh` and
`$HOME/spark/conf/spark-defaults.conf` files on your master node. Information
about various settings can be found in the
[Spark documentation](https://spark.apache.org/docs/1.3.0/configuration.html).

Once you have changed your settings, you must re-deploy the configuration files
to the slave nodes and restart spark.  On your master node, run:
```
copy-dir $HOME/spark/conf
$HOME/spark/sbin/stop-all.sh
$HOME/spark/sbin/start-all.sh
```
Note that your Spark configuration will persist even if you 'stop' or 'start'
your cluster. Your settings will remain is place until until your cluster is destroyed.

Want to help?
-------------

This script is very much a work in progress. It supports my current use case, but I hope it will be made more flexible down the road.  You are welcome to take this script and do whatever you please.  Pull requests are also welcome. Here are some contributions that would be particularly helpful:

- Better testing and support for different GCE instance types, regions, and zones
- Allow the user to specify which Spark version to install
- Support for local NVMe SSDs (already partially implemented)
- Inclusion of Shark, Tachyon, and other mainstays of the Apache open source ecosystem.
- The option to create a larger root disk.  The current root disk size is 10GB, but this can be [expanded](http://stackoverflow.com/questions/24021214/how-to-get-a-bigger-boot-disk-on-google-compute-engine)
- More aesthetically pleasing cluster monitoring using [Graphite](http://graphite.wikidot.com/) and [Grafana](http://grafana.org/)
