#!/usr/bin/env python

###
# This script sets up a Spark cluster on Google Compute Engine
#
# This script originally was written by @SigmoidAnalytics, and then later
# modified by @broxtronix.
###

import os
import sys
import subprocess
import time
import commands
from sys import stderr
import shlex
import threading
import json

# ----------------------------------------------------------------------------------------
# run() can be used to execute multiple command line processes simultaneously.
# This is particularly useful for long-running, network-bound processes like
# starting and stopping nodes with the 'gcloud' command.
#

def run_subprocess(cmds, result_queue):
	"""
	Call a sub-process, returning a tuple: (return code, merged_stdout_stderr).  

	If cmds is a list of commands, each command is run serially.   
	"""
	import subprocess

	# Convert non-list commands to a list with just one element so that the code
	# below can be simpler, handling only this one case.
	if not isinstance(cmds, list):
		cmds = [ cmds ]

	# Execute commands in serial
	for cmd in cmds:
		child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		stdout = child.communicate()[0]
		return_code = child.returncode

		# Check for errors. Only store the output for failed commands
		if return_code != 0:
			result_queue.put( (return_code, cmd, stdout) )
		else:
			result_queue.put( (return_code, cmd, None) )

			
def run(cmds, parallelize = False, verbose = False):
	"""Run commands in serial or in parallel.

	If cmds is a single command, it will run the command.
	
	If cmds is a list of commands, the commands will be run in serial or
	parallel depending on the 'parallelize' flag.

	If cmds is a list of lists of commands, the inner lists of commands will be executed in
	serial within parallel threads.  (i.e. parallelism on the outer list)

	"""

	import multiprocessing
	manager = multiprocessing.Manager()
	result_queue = manager.Queue()

	# Convert non-list commands to a list with just one element so that the code
	# below can be simpler, handling only this one case.
	if not isinstance(cmds, list):
		parallelize = False
		cmds = [ cmds ]
	
	# List of lists forces parallelism to occur on the level of the outermost list.
	if isinstance(cmds[0], list):
		parallelize = True
		
	# For debugging purposes (if verbose is True), print the commands we are
	# about to execute.
	if verbose:
		if isinstance(cmds[0], list):
			for cmd_group in cmds:
				print "  [ Command Group ]"
				for cmd in cmd_group:
					print "       CMD\t", cmd
		else:
			for cmd in cmds:
				print "      CMD\t", cmd

		
	if not parallelize:

		# Run commands serially
		run_subprocess(cmds, result_queue)
				
	else: 

		# Start worker threads.
		jobs = []
		for cmd in cmds:
			if verbose: print "  [ CMD ] ", cmd
				
			p = multiprocessing.Process(target=run_subprocess, args=(cmd, result_queue))
			jobs.append(p)
			p.start()

		# Wait for all the sub-processes to finish processing their queries
		for p in jobs:
			p.join()

	# Check the results. Any failed commands are noted here, and their output is
	# printed to stdout.
	return_vals = []
	num_failed = 0
	failed_stdout = []
	failed_cmds = []
	while not result_queue.empty():
		return_val, cmd, stdout = result_queue.get()

		if return_val != 0:
			num_failed += 1
			failed_stdout.append(stdout)
			failed_cmds.append(cmd)
	
	if num_failed > 0:
		print "\n******************************************************************************************"
		print "\nCall to subprocess failed.  %d commands in this set returned a non-zero exit status." % (num_failed)
		print "\nFirst failed command:\n"
		print failed_cmds[0]
		print "\nCommand output:\n"
		print failed_stdout[0]
		print "******************************************************************************************"
		sys.exit(1)

# -------------------------------------------------------------------------------------

def get_command_prefix(cluster_name, opts):
	command_prefix = 'gcloud compute'
	if opts.project:
		command_prefix += ' --project ' + opts.project
	return command_prefix

def fetch_instance_data(cluster_name, opts):
	command_prefix = get_command_prefix(cluster_name, opts)
	command = command_prefix + ' instances list --format json'
	try: 
		output = subprocess.check_output(command, shell=True)
	except subprocess.CalledProcessError:
		print "An error occured listing instance attributes.  Are you certain that the cluster \'%s\' exists?" % (cluster_name)
		sys.exit(1)
		
	return json.loads(output)

def setup_network(cluster_name, opts):

	print '[ Setting up Network & Firewall Entries ]'
	command_prefix = get_command_prefix(cluster_name, opts)
	cmds = []

	cmds.append( command_prefix + ' networks create "' + cluster_name + '-network" --range "10.240.0.0/16"' )
	
	# Uncomment the above and comment the below section if you don't want to open all ports for public.
	# cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal' )
	cmds.append( command_prefix + ' firewall-rules create ' + cluster_name + '-internal --network ' + cluster_name + '-network --allow tcp udp icmp' )
	run(cmds)

def delete_network(cluster_name, opts):
	print '[ Deleting Network & Firewall Entries ]'
	command_prefix = get_command_prefix(cluster_name, opts)
	cmds = []

	# Uncomment the above and comment the below section if you don't want to open all ports for public.
	# cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal --quiet' )
	cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-internal --quiet' )
	cmds.append( command_prefix + ' networks delete "' + cluster_name + '-network" --quiet' )
	run(cmds)


def launch_cluster(cluster_name, opts):
	"""
	Create a new cluster. 
	"""
	print '[ Launching cluster: %s ]' % cluster_name
	command_prefix = get_command_prefix(cluster_name, opts)

	if opts.zone:
		zone_str = ' --zone ' + opts.zone
	else:
		zone_str = ''

	# Set up the network
	#setup_network(cluster_name, opts)
 
	# Start master nodes & slave nodes
	cmds = []
	cmds.append( command_prefix + ' instances create "' + cluster_name + '-master" --machine-type "' + opts.master_instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "https://www.googleapis.com/compute/v1/projects/gce-nvme/global/images/nvme-backports-debian-7-wheezy-v20141108" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-md" --local-ssd interface=nvme' + zone_str )
	for i in xrange(opts.slaves):
		cmds.append( command_prefix + ' instances create "' + cluster_name + '-slave' + str(i) + '" --machine-type "' + opts.instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "https://www.googleapis.com/compute/v1/projects/gce-nvme/global/images/nvme-backports-debian-7-wheezy-v20141108" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-s' + str(i) + 'd" --local-ssd interface=nvme' + zone_str )
	# run(cmds, parallelize = True)

	# Set up the cluster by installing all the necessary software!
	setup_new_cluster(cluster_name, opts)
	

def destroy_cluster(cluster_name, opts):
	"""
	Delete a cluster permanently.  All state will be lost.
	"""
	print '[ Destroying cluster: %s ]'  % (cluster_name)
	command_prefix = get_command_prefix(cluster_name, opts)

	cmds = []
	for instance in fetch_instance_data(cluster_name, opts):
		host_name = instance['name']
		zone = instance['zone']
		if host_name == cluster_name + '-master':
			cmds.append( command_prefix + ' instances delete ' + host_name + ' --zone ' + zone + ' --quiet' )
		elif cluster_name + '-slave' in host_name:
			cmds.append( command_prefix + ' instances delete ' + host_name + ' --zone ' + zone + ' --quiet' )

	proceed = raw_input('Cluster %s with %d nodes will be deleted PERMANENTLY.  Proceed? (y/N) : ' % (cluster_name, len(cmds)))
	if proceed == 'y' or proceed == 'Y':
		run(cmds, parallelize = True)

		# Delete the network
		delete_network(cluster_name, opts)
	else:
		print "Exiting without deleting cluster %s." % (cluster_name)
		sys.exit(0)

		
def stop_cluster(cluster_name, opts):
	"""
	Stop a running cluster. The cluster can later be restarted with the 'start'
	command. All the data on the root drives of these instances will
	persist across reboots, but any data on attached drives will be lost.
	"""
	print '[ Stopping cluster: %s ]' % cluster_name
	command_prefix = get_command_prefix(cluster_name, opts)

	cmds = []
	for instance in fetch_instance_data(cluster_name, opts):
		host_name = instance['name']
		zone = instance['zone']
		if host_name == cluster_name + '-master':
			cmds.append( command_prefix + ' instances stop ' + host_name + ' --zone ' + zone )
		elif cluster_name + '-slave' in host_name:
			coms.append( command_prefix + ' instances stop ' + host_name + ' --zone ' + zone )

	run(cmds, parallelize = True)

	
def start_cluster(cluster_name, opts):
	"""
	Start a cluster that is in the stopped state.
	"""
	print '[ Starting cluster: %s ]' % (cluster_name)

	cmds = []
	for instance in fetch_instance_data(cluster_name, opts):
		host_name = instance['name']
		zone = instance['zone']
		if host_name == cluster_name + '-master':
			cmds.append( command_prefix + ' instances start ' + host_name + ' --zone ' + zone )
		elif cluster_name + '-slave' in host_name:
			cmds.append( command_prefix + ' instances start ' + host_name + ' --zone ' + zone )

	run(cmds, parallelize = True)

def check_gcloud():
	
	myexec = "gcloud"
	print '[ Verifying gcloud ]'
	try:
		subprocess.call([myexec, 'info'])
		
	except OSError:
		print "%s executable not found. \n# Make sure gcloud is installed and authenticated\nPlease follow https://cloud.google.com/compute/docs/gcloud-compute/" % myexec
		sys.exit(1)

def get_cluster_ips(cluster_name, opts):
	command_prefix = get_command_prefix(cluster_name, opts)
		
	command = command_prefix + ' instances list --format json'
	output = subprocess.check_output(command, shell=True)
	data = json.loads(output)
	master_nodes=[]
	slave_nodes=[]

	for instance in data:
		host_name = instance['name']
		host_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
		if host_name == cluster_name + '-master':
			master_nodes.append( (host_name, host_ip) )
		elif cluster_name + '-slave' in host_name:
			slave_nodes.append( (host_name, host_ip) )
	
	# Return all the instances
	return (master_nodes, slave_nodes)

def ssh_wrap(host, identity_file, cmds, verbose = False):
	''' 
	Given a command to run on a remote host, this function wraps the command in
	the appropriate ssh invocation that can be run on the local host to achieve
	the desired action on the remote host. This can then be passed to the run()
	function to execute the command on the remote host.

	This function can take a single command or a list of commands, and will
	return a list of ssh-wrapped commands.
	'''
	if not isinstance(cmds, list):
		cmds = [ cmds ]

	username = os.environ["USER"]
	result = []
	for cmd in cmds:
		if verbose: print '  SSH: ' + host[0] + '\t', cmd
		result.append( "ssh -i " + identity_file + " -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no " + username + "@" + host[1] + " '" + cmd + "'" )
	return result


def deploy_keys(cluster_name, opts, master_nodes, slave_nodes):

	print '[ Generating SSH keys on master and deploying to slave nodes ]'
	master = master_nodes[0]

	cmds = [ "rm -f ~/.ssh/id_rsa && ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa",
			 "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
			 "tar czf .ssh.tgz .ssh"]

	for slave in slave_nodes:
		cmds.append("scp -oStrictHostKeyChecking=no .ssh.tgz " + slave[1]  + ":")

	cmds.append("rm .ssh.tgz")

	# Execute commands on the master node
	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))

	# Execute commands on the slave nodes
	slave_ssh_cmds = [ ssh_wrap(slave, opts.identity_file, "tar xzf .ssh.tgz && rm .ssh.tgz", verbose = opts.verbose) for slave in slave_nodes ]
	run(slave_ssh_cmds, parallelize = True)


def attach_ssd(cluster_name, opts, master_nodes, slave_nodes):

	print '[ Attaching 350GB NVME SSD drive to each node under /mnt/ssd0 ]'
	master = master_nodes[0]

	cmds = [ 'if [ ! -d /mnt/ssd0 ]; then sudo mkdir /mnt/ssd0; fi',
			 'sudo /usr/share/google/safe_format_and_mount -m "mkfs.ext4 -F" /dev/disk/by-id/google-local-ssd-0 /mnt/ssd0',
			 'sudo chmod a+w /mnt/ssd0']

	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))

	slave_cmds = [ ssh_wrap(slave, opts.identity_file, cmds, verbose = opts.verbose) for slave in slave_nodes ]
	run(slave_cmds, parallelize = True)


def install_spark(cluster_name, opts, master_nodes, slave_nodes):
	print '[ Initializing cluster environment and installing spark (this will take several minutes) ]'

	master = master_nodes[0]
	cmds = [ 'sudo apt-get update -q -y',
			 'sudo apt-get install -q -y screen htop g++ gfortran openjdk-7-jdk python-pip python-dev libatlas-dev liblapack-dev libzmq-dev libfreetype6-dev libpng-dev',
			 'sudo pip-2.7 install --upgrade distribute',
			 'sudo pip-2.7 install --upgrade numpy==1.9.2',
			 'sudo pip-2.7 install --upgrade scipy==0.15.1',
			 'sudo pip-2.7 install --upgrade boto==2.36.0']
	#'wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.0-bin-hadoop2.4.tgz',
#			 'tar xvf spark-1.3.0-bin-hadoop2.4.tgz && rm spark-1.3.0-bin-hadoop2.4.tgz',
#			 'ln -s spark-1.3.0-bin-hadoop2.4 spark']
	#'echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.local.dir=/mnt/ssd0/spark \\\"\" >> $HOME/spark/conf/spark-env.sh"',
	#			 '']

	master_cmds = [ ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose) ]
	slave_cmds = [ ssh_wrap(slave, opts.identity_file, cmds, verbose = opts.verbose) for slave in slave_nodes ]
	
	run(master_cmds + slave_cmds, parallelize = True)

		
def setup_spark(master_nodes,slave_nodes):

	print '[ Downloading Binaries ]'
	
	master = master_nodes[0]
	
	ssh_command(master,"rm -fr sigmoid")
	ssh_command(master,"mkdir sigmoid")
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/1.2.0/spark-1.2.0-bin-cdh4.tgz")
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/scala.tgz")
	ssh_command(master,"cd sigmoid;tar zxf spark-1.2.0-bin-cdh4.tgz;rm spark-1.2.0-bin-cdh4.tgz")
	ssh_command(master,"cd sigmoid;tar zxf scala.tgz;rm scala.tgz")
	

	print '[ Updating Spark Configurations ]'
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;cp spark-env.sh.template spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SCALA_HOME=\"/home/`whoami`/sigmoid/scala\"' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SPARK_MEM=2454m' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.local.dir=/mnt/spark \\\"\" >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SPARK_JAVA_OPTS' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SPARK_MASTER_IP=PUT_MASTER_IP_HERE' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export MASTER=spark://PUT_MASTER_IP_HERE:7077' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.75.x86_64' >> spark-env.sh")
	

	for slave in slave_nodes:
		ssh_command(master,"echo " + slave + " >> sigmoid/spark-1.2.0-bin-cdh4/conf/slaves")

	
	ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" sigmoid/spark-1.2.0-bin-cdh4/conf/spark-env.sh")
	
	ssh_command(master,"chmod +x sigmoid/spark-1.2.0-bin-cdh4/conf/spark-env.sh")

	print '[ Rsyncing Spark to all slaves ]'

	#Change permissions
	enable_sudo(master,"sudo chown " + username + ":" + username + " /mnt")
	i=1
	for slave in slave_nodes:			
		enable_sudo(slave,"sudo chown " + username + ":" + username + " /mnt")


	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/sigmoid " + slave + ":")
		ssh_command(slave,"mkdir /mnt/spark")

	ssh_command(master,"mkdir /mnt/spark")
	print '[ Starting Spark Cluster ]'
	ssh_command(master,"sigmoid/spark-1.2.0-bin-cdh4/sbin/start-all.sh")
	

	#setup_shark(master_nodes,slave_nodes)
	
	setup_hadoop(master_nodes,slave_nodes)


	print "\n\nSpark Master Started, WebUI available at : http://" + master + ":8080"

def setup_hadoop(master_nodes,slave_nodes):

	master = master_nodes[0]
	print '[ Downloading hadoop ]'
	
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/sigmoidanalytics-builds/hadoop/hadoop-2.0.0-cdh4.2.0.tar.gz")
	ssh_command(master,"cd sigmoid;tar zxf hadoop-2.0.0-cdh4.2.0.tar.gz")
	ssh_command(master,"cd sigmoid;rm hadoop-2.0.0-cdh4.2.0.tar.gz")

	print '[ Configuring Hadoop ]'
	
	#Configure .bashrc
	ssh_command(master,"echo '#HADOOP_CONFS' >> .bashrc")
	ssh_command(master,"echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.75.x86_64' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_INSTALL=/home/`whoami`/sigmoid/hadoop-2.0.0-cdh4.2.0' >> .bashrc")
	ssh_command(master,"echo 'export PATH=$PATH:\$HADOOP_INSTALL/bin' >> .bashrc")
	ssh_command(master,"echo 'export PATH=$PATH:\$HADOOP_INSTALL/sbin' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_MAPRED_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_COMMON_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_HDFS_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export YARN_HOME=\$HADOOP_INSTALL' >> .bashrc")

	#Remove *-site.xmls
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0;rm etc/hadoop/core-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0;rm etc/hadoop/yarn-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0;rm etc/hadoop/hdfs-site.xml")
	#Download Our Confs
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/core-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/hdfs-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/mapred-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/yarn-site.xml")

	#Config Core-site
	ssh_command(master,"sed -i \"s/PUT-MASTER-IP/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/core-site.xml")
	
	#Create data/node dirs
	ssh_command(master,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
	#Config slaves
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;rm slaves")
	for slave in slave_nodes:
		ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;echo " + slave + " >> slaves")

	print '[ Rsyncing with Slaves ]'
	#Rsync everything
	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/sigmoid " + slave + ":")
		ssh_command(slave,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
		ssh_command(master,"rsync -za /home/" + username + "/.bashrc " + slave + ":")

	print '[ Formating namenode ]'
	#Format namenode
	ssh_command(master,"sigmoid/hadoop-2.0.0-cdh4.2.0/bin/hdfs namenode -format")
	
	print '[ Starting DFS ]'
	#Start dfs
	ssh_command(master,"sigmoid/hadoop-2.0.0-cdh4.2.0/sbin/start-dfs.sh")

def setup_shark(master_nodes,slave_nodes):

	master = master_nodes[0]
	print '[ Downloading Shark binaries ]'
	
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/spark-ui/hive-0.11.0-bin.tgz")
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/spark-ui/shark-0.9-hadoop-2.0.0-mr1-cdh4.2.0.tar.gz")
	ssh_command(master,"cd sigmoid;tar zxf hive-0.11.0-bin.tgz")
	ssh_command(master,"cd sigmoid;tar zxf shark-0.9-hadoop-2.0.0-mr1-cdh4.2.0.tar.gz")
	ssh_command(master,"rm sigmoid/hive-0.11.0-bin.tgz")
	ssh_command(master,"rm sigmoid/shark-0.9-hadoop-2.0.0-mr1-cdh4.2.0.tar.gz")
	
	print '[ Configuring Shark ]'
	ssh_command(master,"cd sigmoid/shark/;echo \"export SHARK_MASTER_MEM=1g\" > conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.kryoserializer.buffer.mb=10 \\\"\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export SPARK_JAVA_OPTS\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export HIVE_HOME=/home/`whoami`/sigmoid/hive-0.11.0-bin\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export SPARK_JAVA_OPTS\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export MASTER=spark://PUT_MASTER_IP_HERE:7077\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export SPARK_HOME=/home/`whoami`/sigmoid/spark-0.9.1-bin-cdh4\" >> conf/shark-env.sh")
	ssh_command(master,"mkdir /mnt/tachyon")
	ssh_command(master,"cd sigmoid/shark/;echo \"export TACHYON_MASTER=PUT_MASTER_IP_HERE:19998\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export TACHYON_WAREHOUSE_PATH=/mnt/tachyon\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"source /home/`whoami`/sigmoid/spark-0.9.1-bin-cdh4/conf/spark-env.sh\" >> conf/shark-env.sh")	
	ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" sigmoid/shark/conf/shark-env.sh")

	ssh_command(master,"chmod +x sigmoid/shark/conf/shark-env.sh")
	
	print '[ Rsyncing Shark on slaves ]'
	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/sigmoid " + slave + ":")

	print '[ Starting Shark Server ]'
	ssh_command(master,"cd sigmoid/shark/;./bin/shark --service sharkserver 10000 > log.txt 2>&1 &")


def parse_args():
	
	import os
	homedir = os.environ['HOME']
        
	from optparse import OptionParser
	parser = OptionParser(
		usage="spark_gce [options] <action> <cluster_name>"
		+ "\n\n<action> can be: launch, destroy, login, stop, start",
		add_help_option=False)
	parser.add_option(
		"-h", "--help", action="help",
		help="Show this help message and exit")
	parser.add_option(
		"-s", "--slaves", type="int", default=1,
		help="Number of slaves to launch (default: %default)")
	parser.add_option(
		"-k", "--key-pair",
		help="Key pair to use on instances")
	parser.add_option(
		"-i", "--identity-file", default = os.path.join(homedir, ".ssh", "google_compute_engine"),
		help="SSH private key file to use for logging into instances")
	parser.add_option(
		"-t", "--instance-type", default="n1-highmem-16",
		help="Type of instance to launch (default: n1-highmen-16).")
	parser.add_option(
		"-m", "--master-instance-type", default="n1-highmem-16",
		help="Master instance type (leave empty for same as instance-type)")
	parser.add_option(
		"--boot-disk-type", default="pd-standard",
		help="Boot disk type.  Run \'gcloud compute disk-types list\' to see your options.")
	parser.add_option(
		"--boot-disk-size", default="10GB",
		help="The size of the boot disk.  Run \'gcloud compute disk-types list\' to see your options.")
	parser.add_option(
		"-p", "--project",
		help="GCE project to target when launching instances ( you can omit this argument if you set a default with \'gcloud config set project [project-name]\'")
	parser.add_option(
		"-z", "--zone", default="us-central1-f",
		help="GCE zone to target when launching instances ( you can omit this argument if you set a default with \'gcloud config set compute/zone [zone-name]\'")
	parser.add_option("--verbose",
					  action="store_true", dest="verbose", default=False,
					  help="Show verbose output.")
	
	(opts, args) = parser.parse_args()
	if len(args) != 2:
		parser.print_help()
		sys.exit(1)

	(action, cluster_name) = args
	return (opts, action, cluster_name)


def	setup_new_cluster(cluster_name, opts):
	# #Wait some time for machines to bootup
	# print '[ Waiting 120 Seconds for Machines to start up ]'
	# time.sleep(120)

	# Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips(cluster_name, opts)

	# Attach a new empty drive and format it
	# attach_ssd(cluster_name, opts, master_nodes, slave_nodes)

	# Generate SSH keys and deploy
	deploy_keys(cluster_name, opts, master_nodes, slave_nodes)

	# Install Spark and its dependencies
	install_spark(cluster_name, opts, master_nodes, slave_nodes)

	# #Set up Spark/Shark/Hadoop
	# setup_spark(master_nodes,slave_nodes)


def real_main():

	print "Spark Google Compute Engine v0.2"
	print ""
	print "[ Script Started ]"	

	# Read the arguments
	(opts, action, cluster_name) = parse_args()

	# Make sure gcloud is accessible.
	check_gcloud()

	# Launch the cluster
	if action == "launch":
		launch_cluster(cluster_name, opts)

	elif action == "start":
		start_cluster(cluster_name, opts)

	elif action == "stop":
		stop_cluster(cluster_name, opts)

	elif action == "destroy":
		destroy_cluster(cluster_name, opts)

	elif action == "login":
		login_cluster(cluster_name, opts)

	else:
		print >> stderr, "Invalid action: %s" % action
		sys.exit(1)


def main():
	try:
		real_main()
	except Exception as e:
		print >> stderr, "\nError:\n", e
		sys.exit(1)

if __name__ == "__main__":
	main()
