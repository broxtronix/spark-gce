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

			
def run(cmds, parallelize = False, verbose = False, terminate_on_failure = True):
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
	
	if num_failed > 0 and terminate_on_failure:
		print "\n******************************************************************************************"
		print "\nCall to subprocess failed.  %d commands in this set returned a non-zero exit status." % (num_failed)
		print "\nFirst failed command:\n"
		print failed_cmds[0]
		print "\nCommand output:\n"
		print failed_stdout[0]
		print "******************************************************************************************"
		sys.exit(1)
	elif num_failed > 0 and not terminate_on_failure:
		print "%d jobs failed, but were not set to terminate on failure.  The script will continue." % (num_failed)
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
	cmds.append( command_prefix + ' firewall-rules create ' + cluster_name + '-spark-external --network ' + cluster_name + '-network --allow tcp:8080 tcp:4040' )
	run(cmds)

def delete_network(cluster_name, opts):
	print '[ Deleting Network & Firewall Entries ]'
	command_prefix = get_command_prefix(cluster_name, opts)
	cmds = []

	# Uncomment the above and comment the below section if you don't want to open all ports for public.
	# cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal --quiet' )
	cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-internal --quiet' )
	cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-spark-external --quiet' )
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
	setup_network(cluster_name, opts)
 
	# Start master nodes & slave nodes
	cmds = []
	cmds.append( command_prefix + ' instances create "' + cluster_name + '-master" --machine-type "' + opts.master_instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150316" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-md"' + zone_str )
	for i in xrange(opts.slaves):
		cmds.append( command_prefix + ' instances create "' + cluster_name + '-slave' + str(i) + '" --machine-type "' + opts.instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150316" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-s' + str(i) + 'd"' + zone_str )

	print '[ Launching nodes ]'
	run(cmds, parallelize = True)

	# Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips(cluster_name, opts)

	# Generate SSH keys and deploy to workers and slaves
	deploy_ssh_keys(cluster_name, opts, master_nodes, slave_nodes)

	# Attach a new empty drive and format it
	attach_persistent_scratch_disks(cluster_name, opts, master_nodes, slave_nodes)
	#attach_ssd(cluster_name, opts, master_nodes, slave_nodes)

	# Initialize the cluster, installing important dependencies
	initialize_cluster(cluster_name, opts, master_nodes, slave_nodes)

	# Install Spark
	install_spark(cluster_name, opts, master_nodes, slave_nodes)
	
	# Configure and start Spark
	configure_and_start_spark(cluster_name, opts, master_nodes, slave_nodes)


def destroy_cluster(cluster_name, opts):
	"""
	Delete a cluster permanently.  All state will be lost.
	"""
	print '[ Destroying cluster: %s ]'  % (cluster_name)
	command_prefix = get_command_prefix(cluster_name, opts)

	# Get cluster machines
	(master_nodes, slave_nodes) = get_cluster_machines(cluster_name, opts)
	if len(master_nodes) == 0 and len(slave_nodes) == 0:
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		return

	cmds = []
	for instance in master_nodes + slave_nodes:
		cmds.append( command_prefix + ' instances delete ' + instance[0] + ' --zone ' + instance[1] + ' --quiet' )

	proceed = raw_input('Cluster %s with %d nodes will be deleted PERMANENTLY.  All data will be lost.  Proceed? (y/N) : ' % (cluster_name, len(cmds)))
	if proceed == 'y' or proceed == 'Y':

		# Clean up scratch disks
		cleanup_scratch_disks(cluster_name, opts, detach_first = True)

		# Terminate the nodes
		print '[ Destroying %d nodes ]' % len(cmds)
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

	# Get cluster machines
	(master_nodes, slave_nodes) = get_cluster_machines(cluster_name, opts)
	if len(master_nodes) == 0 and len(slave_nodes) == 0:
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		return

	cmds = []
	for instance in master_nodes + slave_nodes:
		cmds.append( command_prefix + ' instances stop ' + instance[0] + ' --zone ' + instance[1] )

	proceed = raw_input('Cluster %s with %d nodes will be stopped.  Data on scratch drives will be lost, but root disks will be preserved.  Are you sure you want to proceed? (y/N) : ' % (cluster_name, len(cmds)))
	if proceed == 'y' or proceed == 'Y':

		print '[ Stopping nodes ]'
		run(cmds, parallelize = True)

		# Clean up scratch disks
		cleanup_scratch_disks(cluster_name, opts, detach_first = True)

	else:
		print "Exiting without stopping cluster %s." % (cluster_name)
		sys.exit(0)

	
def start_cluster(cluster_name, opts):
	"""
	Start a cluster that is in the stopped state.
	"""
	print '[ Starting cluster: %s ]' % (cluster_name)
	command_prefix = get_command_prefix(cluster_name, opts)

	cmds = []
	for instance in fetch_instance_data(cluster_name, opts):
		host_name = instance['name']
		zone = instance['zone']
		if host_name == cluster_name + '-master':
			cmds.append( command_prefix + ' instances start ' + host_name + ' --zone ' + zone )
		elif cluster_name + '-slave' in host_name:
			cmds.append( command_prefix + ' instances start ' + host_name + ' --zone ' + zone )

	run(cmds, parallelize = True)

	# Attach scratch disks
	(master_nodes, slave_nodes) = get_cluster_ips(cluster_name, opts)

	# Set up ssh keys
	deploy_ssh_keys(cluster_name, opts, master_nodes, slave_nodes)

	# Re-attach brand new scratch disks
	attach_persistent_scratch_disks(cluster_name, opts, master_nodes, slave_nodes)

	# Configure and start Spark
	configure_and_start_spark(cluster_name, opts, master_nodes, slave_nodes)

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
	try:
		output = subprocess.check_output(command, shell=True)
	except subprocess.CalledProcessError:
		print "An error occured listing cluster IP address data."
		sys.exit(1)
		
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

def get_cluster_machines(cluster_name, opts):
	command_prefix = get_command_prefix(cluster_name, opts)
	
	command = command_prefix + ' instances list --format json'
	try:
		output = subprocess.check_output(command, shell=True)
	except subprocess.CalledProcessError:
		print "An error occured listing instance data."
		sys.exit(1)
		
	data = json.loads(output)
	master_nodes=[]
	slave_nodes=[]

	for instance in data:
		host_name = instance['name']
		zone = instance['zone']
		if host_name == cluster_name + '-master':
			master_nodes.append( (host_name, zone) )
		elif cluster_name + '-slave' in host_name:
			slave_nodes.append( (host_name, zone) )
	
	# Return all the instances
	return (master_nodes, slave_nodes)

def get_cluster_scratch_disks(cluster_name, opts):
	command_prefix = get_command_prefix(cluster_name, opts)
	
	command = command_prefix + ' disks list --format json'
	try:
		output = subprocess.check_output(command, shell=True)
	except subprocess.CalledProcessError:
		print "An error occured listing disks."
		sys.exit(1)
		
	data = json.loads(output)
	scratch_disks=[]

	for instance in data:
		disk_name = instance['name']
		if (cluster_name in disk_name) and ("scratch" in disk_name):
			scratch_disks.append( disk_name )
	
	# Return all the scratch disks
	return scratch_disks

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


def deploy_ssh_keys(cluster_name, opts, master_nodes, slave_nodes):

	print '[ Generating SSH keys on master and deploying to slave nodes ]'
	master = master_nodes[0]

	cmds = [ "rm -f ~/.ssh/id_rsa && rm -f ~/.ssh/known_hosts",
			 "ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa",
			 "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
			 "ssh-keyscan -H " + master[0] + " >> ~/.ssh/known_hosts" ]
	for slave in slave_nodes:
		cmds.append( "ssh-keyscan -H " + slave[0] + " >> ~/.ssh/known_hosts"  )
	cmds.append("tar czf .ssh.tgz .ssh");

	# Create keys on the master node, add them into authorized_keys, and then pack up the archive
	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))

	# Copy the ssh keys locally, and then upload to the slaves.  
	cmds = [ "gcloud compute copy-files " + cluster_name + "-master:.ssh.tgz /tmp/spark_gce_ssh.tgz" ]
	for slave in slave_nodes:
		cmds.append( "gcloud compute copy-files /tmp/spark_gce_ssh.tgz " + slave[0] + ":" )
	cmds.append("rm -f /tmp/spark_gce_ssh.tgz")
	run(cmds)
	
	# Unpack the ssh keys on the slave nodes
	run( [ssh_wrap(slave,
				   opts.identity_file,
				   "rm -rf ~/.ssh && tar xzf spark_gce_ssh.tgz && rm spark_gce_ssh.tgz",
				   verbose = opts.verbose) for slave in slave_nodes], parallelize = True)

	# Clean up the archive on the master node
	run(ssh_wrap(master, opts.identity_file, "rm -f .ssh.tgz", verbose = opts.verbose))


def attach_local_ssd(cluster_name, opts, master_nodes, slave_nodes):

	print '[ Attaching 350GB NVME SSD drive to each node under /mnt ]'
	master = master_nodes[0]

	cmds = [ 'if [ ! -d /mnt ]; then sudo mkdir /mnt; fi',
			 'sudo /usr/share/google/safe_format_and_mount -m "mkfs.ext4 -F" /dev/disk/by-id/google-local-ssd-0 /mnt',
			 'sudo chmod a+w /mnt']

	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))

	slave_cmds = [ ssh_wrap(slave, opts.identity_file, cmds, verbose = opts.verbose) for slave in slave_nodes ]
	run(slave_cmds, parallelize = True)

def cleanup_scratch_disks(cluster_name, opts, detach_first = True):
	cmd_prefix = get_command_prefix(cluster_name, opts)
	if opts.zone:
		zone_str = ' --zone ' + opts.zone
	else:
		zone_str = ''

	# Get cluster ips
	(master_nodes, slave_nodes) = get_cluster_machines(cluster_name, opts)

	# Detach drives
	print '[ Detaching and deleting cluster scratch disks ]'
	if detach_first: 
		cmds = [ cmd_prefix + ' instances detach-disk ' + cluster_name + '-master --disk ' + cluster_name + '-m-scratch' + zone_str ]
		for i, slave in enumerate(slave_nodes):
			cmds.append( cmd_prefix + ' instances detach-disk ' + cluster_name + '-slave' + str(i) + ' --disk ' + cluster_name + '-s' + str(i) + '-scratch' + zone_str )
		run(cmds, parallelize = True)

	# Delete drives
	cmds = [ cmd_prefix + ' disks delete "' + cluster_name + '-m-scratch" --quiet' + zone_str ] 
	for i, slave in enumerate(slave_nodes):
		cmds.append( cmd_prefix + ' disks delete "' + cluster_name + '-s' + str(i) + '-scratch" --quiet' + zone_str )
	run(cmds, parallelize = True)


def attach_persistent_scratch_disks(cluster_name, opts, master_nodes, slave_nodes):
	cmd_prefix = get_command_prefix(cluster_name, opts)
	if opts.zone:
		zone_str = ' --zone ' + opts.zone
	else:
		zone_str = ''

	print '[ Adding new ' + opts.scratch_disk_size + ' drive of type "' + opts.scratch_disk_type + '" to each cluster node ]'
	master = master_nodes[0]

	cmds = [ [cmd_prefix + ' disks create "' + cluster_name + '-m-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '"' + zone_str,
			  cmd_prefix + ' instances attach-disk ' + cluster_name + '-master --device-name "' + cluster_name + '-m-scratch" --disk ' + cluster_name + '-m-scratch' + zone_str,
			  ssh_wrap(master, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-m-scratch " + " -F < /dev/null", verbose = opts.verbose ),
			  ssh_wrap(master, opts.identity_file, "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-scratch /mnt", verbose = opts.verbose ),
			  ssh_wrap(master, opts.identity_file, 'sudo chown "$USER":"$USER" /mnt', verbose = opts.verbose ) ] ]

	for i, slave in enumerate(slave_nodes):
		cmds.append( [ cmd_prefix + ' disks create "' + cluster_name + '-s' + str(i) + '-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '"' + zone_str,
					   cmd_prefix + ' instances attach-disk ' + cluster_name + '-slave' +  str(i) + ' --disk ' + cluster_name + '-s' + str(i) + '-scratch --device-name "' + cluster_name + '-s' + str(i) + '-scratch"' + zone_str,
					   ssh_wrap(slave, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-s" + str(i) + "-scratch " + " -F < /dev/null", verbose = opts.verbose ),
					   ssh_wrap(slave, opts.identity_file, "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(i) + "-scratch /mnt", verbose = opts.verbose ),
					   ssh_wrap(slave, opts.identity_file, 'sudo chown "$USER":"$USER" /mnt', verbose = opts.verbose ) ] )

	run(cmds, parallelize = True)
	print '[ All volumns mounted, will be available at /mnt ]'
			
	
def initialize_cluster(cluster_name, opts, master_nodes, slave_nodes):
	print '[ Installing software dependencies (this will take several minutes) ]'
	master = master_nodes[0]

	cmds = [ 'sudo apt-get update -q -y',
			 'sudo apt-get install -q -y screen less mosh bzip2 htop g++ openjdk-7-jdk',
			 'wget http://09c8d0b2229f813c1b93-c95ac804525aac4b6dba79b00b39d1d3.r79.cf1.rackcdn.com/Anaconda-2.1.0-Linux-x86_64.sh',
			 'bash Anaconda-2.1.0-Linux-x86_64.sh -b && rm Anaconda-2.1.0-Linux-x86_64.sh',
			 'echo \'export PATH=\$PATH:\$HOME/spark/bin:\$HOME/anaconda/bin\' >> $HOME/.bashrc']

	master_cmds = [ ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose) ]
	slave_cmds = [ ssh_wrap(slave, opts.identity_file, cmds, verbose = opts.verbose) for slave in slave_nodes ]
	
	run(master_cmds + slave_cmds, parallelize = True)


def configure_and_start_spark(cluster_name, opts, master_nodes, slave_nodes):
	print '[ Configuring Spark ]'
	master = master_nodes[0]

	# Populate the file containing the list of all current slave nodes
	run(ssh_wrap(master, opts.identity_file, 'rm -f $HOME/spark/conf/slaves', verbose = opts.verbose))
	for slave in slave_nodes:
		run(ssh_wrap(master, opts.identity_file, 'echo ' + slave[1] + ' >> $HOME/spark/conf/slaves', verbose = opts.verbose))
		
	# Create the Spark scratch directory on the local SSD
	run(ssh_wrap(master, opts.identity_file, 'if [ ! -d /mnt/spark ]; then mkdir /mnt/spark; fi', verbose = opts.verbose))
	run([ ssh_wrap(slave, opts.identity_file, 'if [ ! -d /mnt/spark ]; then mkdir /mnt/spark; fi', verbose = opts.verbose) for slave in slave_nodes ], parallelize = True)

	# Copy the spark directory from the master node to the slave nodes
	cmds = []
	for slave in slave_nodes:
		cmds.append( ssh_wrap(master, opts.identity_file, 'rsync -e \"ssh -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no\" -za $HOME/packages/spark-1.3.0-bin-cdh4 ' + slave[1] + ':packages/', verbose = opts.verbose) )
		run(cmds, parallelize = True)

	print '[ Starting Spark ]'
	run(ssh_wrap(master, opts.identity_file, '$HOME/spark/sbin/start-all.sh', verbose = opts.verbose) )

	print "\n\nSpark Master Started, WebUI available at : http://" + master[1] + ":8080\n\n"
		
		
def install_spark(cluster_name, opts, master_nodes, slave_nodes):
	print '[ Installing Spark ]'
	master = master_nodes[0]

	# Install Spark and Scala
	cmds = [ 'if [ ! -d $HOME/packages ]; then mkdir $HOME/packages; fi',
			 'cd $HOME/packages && wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.0-bin-cdh4.tgz',
			 'cd $HOME/packages && tar xvf spark-1.3.0-bin-cdh4.tgz && rm spark-1.3.0-bin-cdh4.tgz',
			 'ln -s $HOME/packages/spark-1.3.0-bin-cdh4 $HOME/spark',
			 'cd $HOME/packages && wget http://downloads.typesafe.com/scala/2.11.6/scala-2.11.6.tgz',
			 'cd $HOME/packages && tar xvzf scala-2.11.6.tgz && rm -rf scala-2.11.6.tgz']
	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))

	# Set up the spark-env.conf file
	cmds = ['cd $HOME/spark/conf && cp spark-env.sh.template spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_LOCAL_DIRS="/mnt/spark"\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_WORKER_INSTANCES=1\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_WORKER_CORES=16\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export HADOOP_HOME="\$HOME/ephemeral-hdfs"\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_MASTER_IP=PUT_INTERNAL_MASTER_IP_HERE\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export MASTER=spark://PUT_INTERNAL_MASTER_IP_HERE:7077\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'# Bind Spark\'s web UIs to this machine\'s public EC2 hostname:\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_PUBLIC_DNS=\\\\\`wget -q -O - http://icanhazip.com/\\\\\`\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_DRIVER_MEMORY=20g\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export PYSPARK_PYTHON=\$HOME/anaconda/bin/ipython\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export PYSPARK_DRIVER_PYTHON=\$HOME/anaconda/bin/python\' >> spark-env.sh',
			'cd $HOME/spark/conf && echo \'export SPARK_DRIVER_MEMORY=20g\' >> spark-env.sh',
			'cd $HOME/spark/conf && chmod +x spark-env.sh']
	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))
	run(ssh_wrap(master, opts.identity_file, 'sed -i "s/PUT_INTERNAL_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g" $HOME/spark/conf/spark-env.sh', verbose = opts.verbose) )

	# Create a symlink on the slaves
	run([ ssh_wrap(slave, opts.identity_file, 'ln -s $HOME/packages/spark-1.3.0-bin-cdh4 $HOME/spark', verbose = opts.verbose) for slave in slave_nodes ], parallelize = True)

def install_ganglia(cluster_name, opts, master_nodes, slave_nodes):
	print '[ Installing Ganglia ]'
	master = master_nodes[0]
	
	# Install Spark and Scala
	cmds = [ 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -q -y ganglia-monitor',
			 'rm -rf /var/lib/ganglia/rrds/* && rm -rf /mnt/ganglia/rrds/*',
			 'mkdir -p /mnt/ganglia/rrds',
			 'sudo chown -R nobody:root /mnt/ganglia/rrds',
			 'sudo rm -rf /var/lib/ganglia/rrds',
			 'sudo ln -s /mnt/ganglia/rrds /var/lib/ganglia/rrds' ]
	run(ssh_wrap(master, opts.identity_file, cmds, verbose = opts.verbose))
	# Install on slaves as well

	# Install gmetad and the ganglia web front-end
	run(ssh_wrap(master, opts.identity_file, 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -q -y gmetad ganglia-webfrontend', verbose = opts.verbose))


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
		"--boot-disk-type", default="pd-ssd",
		help="Boot disk type.  Run \'gcloud compute disk-types list\' to see your options.")
	parser.add_option(
		"--scratch-disk-type", default="pd-ssd",
		help="Boot disk type.  Run \'gcloud compute disk-types list\' to see your options.")
	parser.add_option(
		"--boot-disk-size", default="10GB",
		help="The size of the boot disk.  Run \'gcloud compute disk-types list\' to see your options.")
	parser.add_option(
		"--scratch-disk-size", default="256GB",
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

	
def mosh_cluster(cluster_name, opts):
	import subprocess

	(master_nodes, slave_nodes) = get_cluster_ips(cluster_name, opts)
	
	cmd = 'mosh --ssh="ssh -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no\" ' + str(master_nodes[0][1])
	subprocess.check_call(shlex.split(cmd))

	
def ssh_cluster(cluster_name, opts):
	import subprocess

	(master_nodes, slave_nodes) = get_cluster_ips(cluster_name, opts)
	
	cmd = 'ssh -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no ' + str(master_nodes[0][1])
	subprocess.check_call(shlex.split(cmd))

	
def real_main():

	print "Spark for Google Compute Engine v0.2"
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

	elif action == "ssh":
		ssh_cluster(cluster_name, opts)

	elif action == "mosh":
		mosh_cluster(cluster_name, opts)

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
