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
				print "  [ Command Group (will execute in parallel) ]"
				for cmd in cmd_group:
					print "  CMD: ", cmd
		else:
			for cmd in cmds:
				print "  CMD: ", cmd
		
	if not parallelize:

		# Run commands serially
		run_subprocess(cmds, result_queue)
				
	else: 

		# Start worker threads.
		jobs = []
		for cmd in cmds:
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
		return num_failed

	return 0

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
		if verbose: print '  SSH: ' + host['host_name'] + '\t', cmd

		if host['external_ip'] is None:
			print "Error: attempting to ssh into machine instance \"%s\" without a public IP address.  Exiting." % (host["host_name"])
			sys.exit(1)
			
		result.append( "ssh -i " + identity_file + " -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no -o LogLevel=quiet " + username + "@" + host['external_ip'] + " '" + cmd + "'" )
	return result

# -------------------------------------------------------------------------------------

def get_command_prefix(cluster_name, opts):
	command_prefix = 'gcloud compute'
	if opts.project:
		command_prefix += ' --project ' + opts.project
	return command_prefix

def check_gcloud(cluster_name, opts):
	
	cmd = "gcloud info"
	try:
		output = subprocess.check_output(cmd, shell=True)
		if opts.verbose:
			print '[ Verifying gcloud ]'
			print output
		
	except OSError:
		print "%s executable not found. \n# Make sure gcloud is installed and authenticated\nPlease follow https://cloud.google.com/compute/docs/gcloud-compute/" % myexec
		sys.exit(1)

def wait_for_cluster(cluster_name, opts, retry_delay = 10, num_retries = 12):
	import time
	
	# Query for instance info a second time.  Once instances start, we should get IP addresses.
	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)

	retries = 0
	cluster_is_ready = False
	while True:
		cluster_is_ready = True
		for node in [master_node] + slave_nodes:
			# Check if the node has a public IP assigned yet. If not, the node
			# is still launching.
			if node['external_ip'] is None:
				cluster_is_ready = False
			else:
				# If the cluster node has an IP, then let's try to connect over
				# ssh.  If that fails, the node is still not ready.
				ssh_command = ssh_wrap(node, opts.identity_file, 'echo')
				try:
					output = subprocess.check_output(ssh_command, shell=True)
				except subprocess.CalledProcessError:
					cluster_is_ready = False
	
		if cluster_is_ready:
			return (master_node, slave_nodes)
		elif retries < num_retries:
			print "  cluster was not ready.  retrying..."
			time.sleep(retry_delay)
			retries += 1
		else:
			print "Error: cluster took too long to start."
			sys.exit(1)

def get_cluster_info(cluster_name, opts):
	command_prefix = get_command_prefix(cluster_name, opts)
		
	command = command_prefix + ' instances list --format json'
	try:
		output = subprocess.check_output(command, shell=True)
	except subprocess.CalledProcessError:
		print "An error occured listing cluster data."
		sys.exit(1)
		
	data = json.loads(output)
	master_node = None
	slave_nodes = []

	for instance in data:
		host_name = instance['name']
		zone = instance['zone']
		try:
			external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
			internal_ip = instance['networkInterfaces'][0]['networkIP']
		except:
			external_ip = None  # Stopped instances don't have any IP addresses
			internal_ip = None  # Stopped instances don't have any IP addresses
			
		if host_name == cluster_name + '-master':
			master_node = { 'host_name': host_name, 'external_ip': external_ip, 'zone': zone }
		elif cluster_name + '-slave' in host_name:
			slave_id = int(host_name[host_name.rfind("-slave")+6:])
			slave_nodes.append( {'host_name': host_name, 'external_ip': external_ip, 'zone': zone, 'slave_id': slave_id } )
	
	# Return all the instances
	return (master_node, slave_nodes)

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


# -------------------------------------------------------------------------------------
#                                 NETWORK SETUP
# -------------------------------------------------------------------------------------

def setup_network(cluster_name, opts):

	print '[ Setting up Network & Firewall Entries ]'
	command_prefix = get_command_prefix(cluster_name, opts)
	cmds = []

	cmds.append( command_prefix + ' networks create "' + cluster_name + '-network" --range "10.240.0.0/16"' )
	
	# Uncomment the above and comment the below section if you don't want to open all ports for public.
	# cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal' )
	cmds.append( command_prefix + ' firewall-rules create ' + cluster_name + '-internal --network ' + cluster_name + '-network --allow tcp udp icmp' )
	cmds.append( command_prefix + ' firewall-rules create ' + cluster_name + '-spark-external --network ' + cluster_name + '-network --allow tcp:8080 tcp:4040 tcp:5080' )
	run(cmds, verbose = opts.verbose)

def delete_network(cluster_name, opts):
	print '[ Deleting Network & Firewall Entries ]'
	command_prefix = get_command_prefix(cluster_name, opts)
	cmds = []

	# Uncomment the above and comment the below section if you don't want to open all ports for public.
	# cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal --quiet' )
	cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-internal --quiet' )
	cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-spark-external --quiet' )
	cmds.append( command_prefix + ' networks delete "' + cluster_name + '-network" --quiet' )
	run(cmds, verbose = opts.verbose)

# -------------------------------------------------------------------------------------
#                      CLUSTER LAUNCH, DESTROY, START, STOP
# -------------------------------------------------------------------------------------

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
	cmds.append( command_prefix + ' instances create "' + cluster_name + '-master" --machine-type "' + opts.master_instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.full_control" --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150316" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-md" --metadata startup-script-url=https://raw.githubusercontent.com/broxtronix/spark_gce/master/growroot.sh' + zone_str )
	for i in xrange(opts.slaves):
		cmds.append( command_prefix + ' instances create "' + cluster_name + '-slave' + str(i) + '" --machine-type "' + opts.instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.full_control" --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150316" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-s' + str(i) + 'd" --metadata startup-script-url=https://raw.githubusercontent.com/broxtronix/spark_gce/master/growroot.sh' + zone_str )

	print '[ Launching nodes ]'
	run(cmds, parallelize = True, verbose = opts.verbose)

	# Wait some time for machines to bootup. We consider the cluster ready when
	# all hosts have been assigned an IP address.
	print '[ Waiting for cluster to enter into SSH-ready state ]'
	(master_node, slave_nodes) = wait_for_cluster(cluster_name, opts)
		
	# Generate SSH keys and deploy to workers and slaves
	deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes)
 
	# Attach a new empty drive and format it
	attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes)

	# Initialize the cluster, installing important dependencies
	initialize_cluster(cluster_name, opts, master_node, slave_nodes)

	# Install, configure, and start ganglia
	configure_ganglia(cluster_name, opts, master_node, slave_nodes)

	# Install and configure Hadoop
	install_hadoop(cluster_name, opts, master_node, slave_nodes)
	configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes)
	
	# Install, configure and start Spark
	install_spark(cluster_name, opts, master_node, slave_nodes)
	configure_and_start_spark(cluster_name, opts, master_node, slave_nodes)

	print "\n\n======================================================="
	print "              Cluster \"%s\" is running" % (cluster_name)
	print ""
	print "   Spark UI: http://" + master_node['external_ip'] + ":8080"
	print "   Ganglia : http://" + master_node['external_ip'] + ":5080/ganglia"
	print "========================================================\n"

	
def destroy_cluster(cluster_name, opts):
	"""
	Delete a cluster permanently.  All state will be lost.
	"""
	print '[ Destroying cluster: %s ]'  % (cluster_name)
	command_prefix = get_command_prefix(cluster_name, opts)

	# Get cluster machines
	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
	if (master_node is None) and (len(slave_nodes) == 0):
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		sys.exit(1)

	cmds = []
	for instance in [master_node] + slave_nodes:
		cmds.append( command_prefix + ' instances delete ' + instance['host_name'] + ' --zone ' + instance['zone'] + ' --quiet' )

	print 'Cluster %s with %d nodes will be deleted PERMANENTLY.  All data will be lost.' % (cluster_name, len(cmds))
	proceed = raw_input('Are you sure you want to proceed? (y/N) : ')
	if proceed == 'y' or proceed == 'Y':

		# Clean up scratch disks
		cleanup_scratch_disks(cluster_name, opts, detach_first = True)

		# Terminate the nodes
		print '[ Destroying %d nodes ]' % len(cmds)
		run(cmds, parallelize = True, verbose = opts.verbose)

		# Delete the network
		delete_network(cluster_name, opts)
	else:
		print "\nExiting without deleting cluster %s." % (cluster_name)
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
	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
	if (master_node is None) and (len(slave_nodes) == 0):
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		sys.exit(1)

	cmds = []
	for instance in [master_node] + slave_nodes:
		cmds.append( command_prefix + ' instances stop ' + instance['host_name'] + ' --zone ' + instance['zone'] )

	# I can't decide if it is important to check with the user when stopping the cluster.  Seems
	# more convenient not to.  But I'm leaving this code in for now in case time shows that
	# it is important to include this confirmation step. -broxton
	#
	#print 'Cluster %s with %d nodes will be stopped.  Data on scratch drives will be lost, but root disks will be preserved.' % (cluster_name, len(cmds))
	#proceed = raw_input('Are you sure you want to proceed? (y/N) : ')
	#if proceed == 'y' or proceed == 'Y':

	print '[ Stopping nodes ]'
	run(cmds, parallelize = True, verbose = opts.verbose, terminate_on_failure = False)

	# Clean up scratch disks
	cleanup_scratch_disks(cluster_name, opts, detach_first = True)

	#else:
	#	print "\nExiting without stopping cluster %s." % (cluster_name)
	#	sys.exit(0)

			
def start_cluster(cluster_name, opts):
	"""
	Start a cluster that is in the stopped state.
	"""
	print '[ Starting cluster: %s ]' % (cluster_name)
	command_prefix = get_command_prefix(cluster_name, opts)

	# Get cluster machines
	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
	if (master_node is None) and (len(slave_nodes) == 0):
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		sys.exit(1)

	cmds = []
	for instance in [master_node] + slave_nodes:
		cmds.append( command_prefix + ' instances start ' + instance['host_name'] + ' --zone ' + instance['zone'] )

	print '[ Starting nodes ]'
	run(cmds, parallelize = True, verbose = opts.verbose)

	# Wait some time for machines to bootup. We consider the cluster ready when
	# all hosts have been assigned an IP address.
	print '[ Waiting for cluster to enter into SSH-ready state ]'
	(master_node, slave_nodes) = wait_for_cluster(cluster_name, opts)

	# Set up ssh keys
	deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes)

	# Re-attach brand new scratch disks
	attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes)

	# Install, configure, and start ganglia
	configure_ganglia(cluster_name, opts, master_node, slave_nodes)

	# Configure and start Hadoop
	configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes)

	# Configure and start Spark
	configure_and_start_spark(cluster_name, opts, master_node, slave_nodes)

	print "\n\n---------------------------------------------------------------------------"
	print "                       Cluster %s is running" % (cluster_name)
	print ""
	print " Spark UI: http://" + master_node['external_ip'] + ":8080"
	print " Ganglia : http://" + master_node['external_ip'] + ":5080/ganglia"
	print "\n\n---------------------------------------------------------------------------"




def deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes):

	print '[ Generating SSH keys on master and deploying to slave nodes ]'

	cmds = [ "rm -f ~/.ssh/id_rsa && rm -f ~/.ssh/known_hosts",
			 "ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa",
			 "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
			 "ssh-keyscan -H " + master_node['host_name'] + " >> ~/.ssh/known_hosts" ]
	for slave in slave_nodes:
		cmds.append( "ssh-keyscan -H " + slave['host_name'] + " >> ~/.ssh/known_hosts"  )
	cmds.append("tar czf .ssh.tgz .ssh");

	# Create keys on the master node, add them into authorized_keys, and then pack up the archive
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	# Copy the ssh keys locally, and then upload to the slaves.  
	cmds = [ "gcloud compute copy-files " + cluster_name + "-master:.ssh.tgz /tmp/spark_gce_ssh.tgz" ]
	for slave in slave_nodes:
		cmds.append( "gcloud compute copy-files /tmp/spark_gce_ssh.tgz " + slave['host_name'] + ":" )
	cmds.append("rm -f /tmp/spark_gce_ssh.tgz")
	run(cmds, verbose = opts.verbose)
	
	# Unpack the ssh keys on the slave nodes
	run( [ssh_wrap(slave,
				   opts.identity_file,
				   "rm -rf ~/.ssh && tar xzf spark_gce_ssh.tgz && rm spark_gce_ssh.tgz",
				   verbose = opts.verbose) for slave in slave_nodes], parallelize = True)

	# Clean up the archive on the master node
	run(ssh_wrap(master_node, opts.identity_file, "rm -f .ssh.tgz", verbose = opts.verbose))


def attach_local_ssd(cluster_name, opts, master_node, slave_nodes):

	print '[ Attaching 350GB NVME SSD drive to each node under /mnt ]'

	cmds = [ 'if [ ! -d /mnt ]; then sudo mkdir /mnt; fi',
			 'sudo /usr/share/google/safe_format_and_mount -m "mkfs.ext4 -F" /dev/disk/by-id/google-local-ssd-0 /mnt',
			 'sudo chmod a+w /mnt']

	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	slave_cmds = [ ssh_wrap(slave, opts.identity_file, cmds, verbose = opts.verbose) for slave in slave_nodes ]
	run(slave_cmds, parallelize = True)

def cleanup_scratch_disks(cluster_name, opts, detach_first = True):
	cmd_prefix = get_command_prefix(cluster_name, opts)

	# Get cluster ips
	print '[ Detaching and deleting cluster scratch disks ]'
	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)

	# Detach drives
	if detach_first:
		cmds = []
		cmds = [ cmd_prefix + ' instances detach-disk ' + cluster_name + '-master --disk ' + cluster_name + '-m-scratch --zone ' + master_node['zone'] ]
		for i, slave in enumerate(slave_nodes):
			cmds.append( cmd_prefix + ' instances detach-disk ' + cluster_name + '-slave' + str(slave['slave_id']) + ' --disk ' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch --zone ' + slave['zone'] )
		run(cmds, parallelize = True, verbose = opts.verbose, terminate_on_failure = False)

	# Delete drives
	cmds = [ cmd_prefix + ' disks delete "' + cluster_name + '-m-scratch" --quiet --zone ' + master_node['zone'] ] 
	for i, slave in enumerate(slave_nodes):
		cmds.append( cmd_prefix + ' disks delete "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --quiet --zone ' + slave['zone'] )
	run(cmds, parallelize = True, verbose = opts.verbose, terminate_on_failure = False)


def attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes):
	cmd_prefix = get_command_prefix(cluster_name, opts)

	print '[ Adding new ' + opts.scratch_disk_size + ' drive of type "' + opts.scratch_disk_type + '" to each cluster node ]'

	cmds = []
	cmds = [ [cmd_prefix + ' disks create "' + cluster_name + '-m-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '" --zone ' + master_node['zone'],
			  cmd_prefix + ' instances attach-disk ' + cluster_name + '-master --device-name "' + cluster_name + '-m-scratch" --disk ' + cluster_name + '-m-scratch --zone ' + master_node['zone'],
			  ssh_wrap(master_node, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-m-scratch " + " -F < /dev/null", verbose = opts.verbose ),
			  ssh_wrap(master_node, opts.identity_file, "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-scratch /mnt", verbose = opts.verbose ),
			  ssh_wrap(master_node, opts.identity_file, 'sudo chown "$USER":"$USER" /mnt', verbose = opts.verbose ) ] ]

	for slave in slave_nodes:
		cmds.append( [ cmd_prefix + ' disks create "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '" --zone ' + slave['zone'],
					   cmd_prefix + ' instances attach-disk ' + cluster_name + '-slave' +  str(slave['slave_id']) + ' --disk ' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch --device-name "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --zone ' + slave['zone'],
					   ssh_wrap(slave, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-s" + str(slave['slave_id']) + "-scratch " + " -F < /dev/null", verbose = opts.verbose ),
					   ssh_wrap(slave, opts.identity_file, "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(slave['slave_id']) + "-scratch /mnt", verbose = opts.verbose ),
					   ssh_wrap(slave, opts.identity_file, 'sudo chown "$USER":"$USER" /mnt', verbose = opts.verbose ) ] )

	run(cmds, parallelize = True)
	print '[ All volumns mounted, will be available at /mnt ]'
			
	
def initialize_cluster(cluster_name, opts, master_node, slave_nodes):
	print '[ Installing software dependencies (this will take several minutes) ]'

	cmds = [ 'sudo apt-get update -q -y',
			 'sudo apt-get install -q -y screen less git mosh pssh emacs bzip2 htop g++ openjdk-7-jdk',
			 'wget http://09c8d0b2229f813c1b93-c95ac804525aac4b6dba79b00b39d1d3.r79.cf1.rackcdn.com/Anaconda-2.1.0-Linux-x86_64.sh',
			 'rm -rf $HOME/anaconda && bash Anaconda-2.1.0-Linux-x86_64.sh -b && rm Anaconda-2.1.0-Linux-x86_64.sh',
			 'echo \'export PATH=\$HOME/anaconda/bin:\$PATH:\$HOME/spark/bin:\$HOME/ephemeral-hdfs/bin\' >> $HOME/.bashrc'
		 ]

	master_cmds = [ ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose) ]
	slave_cmds = [ ssh_wrap(slave, opts.identity_file, cmds, verbose = opts.verbose) for slave in slave_nodes ]
	
	run(master_cmds + slave_cmds, parallelize = True)

def install_spark(cluster_name, opts, master_node, slave_nodes):
	print '[ Installing Spark ]'

	# Install Spark and Scala
	cmds = [ 'if [ ! -d $HOME/packages ]; then mkdir $HOME/packages; fi',
			 'cd $HOME/packages && wget http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-1.3.0/spark-1.3.0-bin-cdh4.tgz',
			 'cd $HOME/packages && tar xvf spark-1.3.0-bin-cdh4.tgz && rm spark-1.3.0-bin-cdh4.tgz',
			 'rm -f $HOME/spark && ln -s $HOME/packages/spark-1.3.0-bin-cdh4 $HOME/spark',
			 'cd $HOME/packages && wget http://downloads.typesafe.com/scala/2.11.6/scala-2.11.6.tgz',
			 'cd $HOME/packages && tar xvzf scala-2.11.6.tgz && rm -rf scala-2.11.6.tgz']
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	# Set up the spark-env.conf and spark-defaults.conf files
	cmds = ['cd $HOME/spark/conf && rm -f spark-env.sh && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/spark/spark-env.sh',
			'cd $HOME/spark/conf && rm -f spark-defaults.conf && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/spark/spark-defaults.conf',
			'cd $HOME/spark/conf && rm -f core-site.xml && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/spark/core-site.xml',
			'cd $HOME/spark/conf && chmod +x spark-env.sh',
			'echo \'export SPARK_HOME=\$HOME:spark\' >> $HOME/.bashrc',
			'echo \'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64\' >> $HOME/.bashrc']
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))
	run(ssh_wrap(master_node, opts.identity_file, 'sed -i "s/{{active_master}}/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g" $HOME/spark/conf/spark-env.sh', verbose = opts.verbose) )
	run(ssh_wrap(master_node, opts.identity_file, 'sed -i "s/{{active_master}}/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g" $HOME/spark/conf/core-site.xml', verbose = opts.verbose) )

	# Populate the file containing the list of all current slave nodes 
	import tempfile
	slave_file = tempfile.NamedTemporaryFile(delete = False)
	for slave in slave_nodes:
		slave_file.write(slave['host_name'] + '\n')
	slave_file.close()
	cmds = [ "gcloud compute copy-files " + slave_file.name + " " + cluster_name + "-master:spark/conf/slaves"]
	run(cmds, verbose = opts.verbose)
	os.unlink(slave_file.name)
	
	# Install the copy-dir script
	cmds = ['cd $HOME/spark/bin && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/copy-dir',
			'chmod 755 $HOME/spark/bin/copy-dir']
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))
	
	# Copy spark to the slaves and create a symlink to $HOME/spark
	run(ssh_wrap(master_node, opts.identity_file, '$HOME/spark/bin/copy-dir $HOME/packages/spark-1.3.0-bin-cdh4', verbose = opts.verbose))
	run([ ssh_wrap(slave, opts.identity_file, 'rm -f $HOME/spark && ln -s $HOME/packages/spark-1.3.0-bin-cdh4 $HOME/spark', verbose = opts.verbose) for slave in slave_nodes ], parallelize = True)


def configure_and_start_spark(cluster_name, opts, master_node, slave_nodes):
	print '[ Configuring Spark ]'

	# Create the Spark scratch directory on the local SSD
	run(ssh_wrap(master_node, opts.identity_file, 'if [ ! -d /mnt/spark ]; then mkdir /mnt/spark; fi', verbose = opts.verbose))
	run([ ssh_wrap(slave, opts.identity_file, 'if [ ! -d /mnt/spark ]; then mkdir /mnt/spark; fi', verbose = opts.verbose) for slave in slave_nodes ], parallelize = True)

	print '[ Starting Spark ]'
	run(ssh_wrap(master_node, opts.identity_file, '$HOME/spark/sbin/start-all.sh', verbose = opts.verbose) )

	
def configure_ganglia(cluster_name, opts, master_node, slave_nodes):
	print '[ Configuring Ganglia ]'

	# Install gmetad and the ganglia web front-end on the master node
	cmds = [ 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -q -y ganglia-webfrontend gmetad ganglia-monitor',
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/ports.conf',
			 'sudo mv ports.conf /etc/apache2/ && rm -f ports.conf',
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/000-default.conf',
			 'sudo mv 000-default.conf /etc/apache2/sites-enabled/ && rm -f 000-default.conf',
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/ganglia.conf',
			 'sudo mv ganglia.conf /etc/apache2/sites-enabled/ && rm -f ganglia.conf',

			 # Set up ganglia to log data to the scratch drive, since the root drive is small
			 'sudo rm -rf /var/lib/ganglia/rrds/* && sudo rm -rf /mnt/ganglia/rrds/*',
			 'sudo mkdir -p /mnt/ganglia/rrds',
			 'sudo chown -R nobody:root /mnt/ganglia/rrds',
			 'sudo rm -rf /var/lib/ganglia/rrds',
			 'sudo ln -s /mnt/ganglia/rrds /var/lib/ganglia/rrds',

			 # Configure gmond and gmetad on the master node
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/gmetad.conf',
			 'sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" gmetad.conf',
			 'sudo mv gmetad.conf /etc/ganglia/ && rm -f gmetad.conf',
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/gmond.conf',
			 'sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" gmond.conf', 
			 'sudo mv gmond.conf /etc/ganglia/ && rm -f gmond.conf',
			 'sudo service gmetad restart && sudo service ganglia-monitor restart && sudo service apache2 restart'
	]
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	# Sleep for a sec
	import time
	time.sleep(2)

	# Install and configure gmond everywhere
	cmds = [ 'sudo apt-get install -q -y ganglia-monitor gmetad',
			 'sudo rm -rf /var/lib/ganglia/rrds/* && sudo rm -rf /mnt/ganglia/rrds/*',
			 'sudo mkdir -p /mnt/ganglia/rrds',
			 'sudo chown -R nobody:root /mnt/ganglia/rrds',
			 'sudo rm -rf /var/lib/ganglia/rrds',
			 'sudo ln -s /mnt/ganglia/rrds /var/lib/ganglia/rrds',
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/gmond.conf',
			 'sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" gmond.conf', 
			 'sudo mv gmond.conf /etc/ganglia/ && rm -f gmond.conf',
			 'wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/ganglia/gmetad.conf',
			 'sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" gmetad.conf',
			 'sudo mv gmetad.conf /etc/ganglia/ && rm -f gmetad.conf',
	         'sudo service ganglia-monitor restart']
	run([ssh_wrap(node, opts.identity_file, cmds, verbose = opts.verbose) for node in slave_nodes])


def install_hadoop(cluster_name, opts, master_node, slave_nodes):

	print '[ Installing hadoop (this will take several minutes) ]'

	cmds = [
		# Build native hadoop libaries
		'cd /tmp && wget "http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1-src.tar.gz"',
		'cd /tmp && tar xvzf hadoop-2.4.1-src.tar.gz && rm hadoop-2.4.1-src.tar.gz',
		'sudo apt-get install -q -y protobuf-compiler cmake libssl-dev maven pkg-config',
		'cd /tmp/hadoop-2.4.1-src && mvn package -Pdist,native -DskipTests -Dmaven.javadoc.skip=true -Dtar',
		'mkdir -p $HOME/hadoop-native && sudo mv /tmp/hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1/lib/native/* $HOME/hadoop-native',
		'rm -rf /tmp/hadoop-2.4.1-src',
		
		# Install Hadoop 2.0
		'wget http://s3.amazonaws.com/spark-related-packages/hadoop-2.0.0-cdh4.2.0.tar.gz',
		'tar xvzf hadoop-*.tar.gz > /tmp/spark-ec2_hadoop.log',
		'rm hadoop-*.tar.gz',
		'rm -rf ephemeral-hdfs && mv hadoop-2.0.0-cdh4.2.0 ephemeral-hdfs',
		'rm -rf $HOME/ephemeral-hdfs/etc/hadoop/',   # Use a single conf directory
		'mkdir -p $HOME/ephemeral-hdfs/conf && ln -s $HOME/ephemeral-hdfs/conf $HOME/ephemeral-hdfs/etc/hadoop',
		'cp $HOME/hadoop-native/* $HOME/ephemeral-hdfs/lib/native/',
		'cd ephemeral-hdfs && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/setup-slave.sh && chmod 755 setup-slave.sh',
		'cd ephemeral-hdfs && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/setup-auth.sh && chmod 755 setup-auth.sh',

		# Install the Google Storage adaptor
		'cd $HOME/ephemeral-hdfs/share/hadoop/hdfs/lib && rm -f gcs-connector-latest-hadoop2.jar && wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar',
	]
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	# Set up Hadoop configuration files from the templates
	cmds = [
		'cd $HOME/ephemeral-hdfs/conf && rm -f core-site.xml && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/conf/core-site.xml',
		'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/core-site.xml',
		
		'cd $HOME/ephemeral-hdfs/conf && rm -f hadoop-env.sh && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/conf/hadoop-env.sh && chmod 755 hadoop-env.sh',
		'sed -i "s/{{java_home}}/\/usr\/lib\/jvm\/java-1.7.0-openjdk-amd64/g" $HOME/ephemeral-hdfs/conf/hadoop-env.sh',

		'cd $HOME/ephemeral-hdfs/conf && rm -f hadoop-metrics2.properties && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/conf/hadoop-metrics2.properties',
		'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/hadoop-metrics2.properties',

		'cd $HOME/ephemeral-hdfs/conf && rm -f hdfs-site.xml && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/conf/hdfs-site.xml',
		'sed -i "s/{{hdfs_data_dirs}}/\/mnt\/hadoop\/dfs\/data/g" $HOME/ephemeral-hdfs/conf/hdfs-site.xml',
				
		'cd $HOME/ephemeral-hdfs/conf && rm -f mapred-site.xml && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/conf/mapred-site.xml',
		'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/mapred-site.xml',

		'cd $HOME/ephemeral-hdfs/conf && rm -f masters && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/templates/hadoop/conf/masters',
		'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/masters',
	]
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	# Set up authentication for the Hadoop Google Storage adaptor
	run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/setup-auth.sh', opts.verbose))

	# Populate the file containing the list of all current slave nodes 
	import tempfile
	slave_file = tempfile.NamedTemporaryFile(delete = False)
	for slave in slave_nodes:
		slave_file.write(slave['host_name'] + '\n')
	slave_file.close()
	cmds = [ "gcloud compute copy-files " + slave_file.name + " " + cluster_name + "-master:ephemeral-hdfs/conf/slaves"]
	run(cmds, verbose = opts.verbose)
	os.unlink(slave_file.name)

	# Install the copy-dir script
	cmds = ['cd $HOME/ephemeral-hdfs/bin && wget https://raw.githubusercontent.com/broxtronix/spark_gce/master/copy-dir',
			'chmod 755 $HOME/ephemeral-hdfs/bin/copy-dir']
	run(ssh_wrap(master_node, opts.identity_file, cmds, verbose = opts.verbose))

	# Copy the hadoop directory from the master node to the slave nodes
	run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/bin/copy-dir $HOME/ephemeral-hdfs', opts.verbose))

	
def configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes):

	print '[ Configuring hadoop ]'

	# Set up the ephemeral HDFS directories
	cmds = [ ssh_wrap(node, opts.identity_file,'$HOME/ephemeral-hdfs/setup-slave.sh', opts.verbose) for node in [master_node] + slave_nodes ]
	run(cmds, parallelize = True)

	# Format the ephemeral HDFS
	run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/bin/hadoop namenode -format', opts.verbose))

	# Start Hadoop HDFS
	run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/sbin/start-dfs.sh', opts.verbose))

	
def parse_args():
	
	import os
	homedir = os.environ['HOME']
        
	from optparse import OptionParser
	parser = OptionParser(
		usage="spark_gce [options] <action> <cluster_name>"
		+ "\n\n<action> can be: launch, destroy, stop, start, ssh, mosh, sshfs",
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
		"--scratch-disk-type", default="pd-ssd",
		help="Boot disk type.  Run \'gcloud compute disk-types list\' to see your options.")
	parser.add_option(
		"--boot-disk-size", default="50GB",
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
	parser.add_option("--ssh-port-forwarding", default=None,
					  help="Set up ssh port forwarding when you login to the cluster.  " +
					  "This provides a convenient alternative to connecting to iPython " +
					  "notebook over an open port using SSL.  You must supply an argument " +
					  "of the form \"local_port:remote_port\".")

	(opts, args) = parser.parse_args()
	if len(args) != 2:
		parser.print_help()
		sys.exit(1)

	(action, cluster_name) = args
	return (opts, action, cluster_name)

	
def mosh_cluster(cluster_name, opts):
	import subprocess

	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
	if (master_node is None) and (len(slave_nodes) == 0):
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		sys.exit(1)

	cmd = 'mosh --ssh="ssh -A -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no\" ' + str(master_node['external_ip'])
	subprocess.check_call(shlex.split(cmd))

	
def ssh_cluster(cluster_name, opts):
	import subprocess

	(master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
	if (master_node is None) and (len(slave_nodes) == 0):
		print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
		sys.exit(1)

	# SSH tunnels are a convenient, zero-configuration
	# alternative to opening a port using the EC2 security
	# group settings and using iPython notebook over SSL.
	#
	# If the user has requested ssh port forwarding, we set
	# that up here.
	if opts.ssh_port_forwarding is not None:
		ssh_ports = opts.ssh_port_forwarding.split(":")
		if len(ssh_ports) != 2:
			print "\nERROR: Could not parse arguments to \'--ssh-port-forwarding\'."
			print "       Be sure you use the syntax \'local_port:remote_port\'"
			sys.exit(1)
		print ("\nSSH port forwarding requested.  Remote port " + ssh_ports[1] +
			   " will be accessible at http://localhost:" + ssh_ports[0] + '\n')
		try:
			cmd = ('ssh -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no -L ' +
				   ssh_ports[0] + ':127.0.0.1:' + ssh_ports[1] + ' -o ExitOnForwardFailure=yes ' + str(master_node['external_ip']))
			subprocess.check_call(shlex.split(cmd))
		except subprocess.CalledProcessError:
			print "\nERROR: Could not establish ssh connection with port forwarding."
			print "       Check your Internet connection and make sure that the"
			print "       ports you have requested are not already in use."
			sys.exit(1)

	else:
		cmd = 'ssh -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no ' + str(master_node['external_ip'])
		subprocess.check_call(shlex.split(cmd))
	
def real_main():

	print "Spark for Google Compute Engine v0.2"
	print ""

	# Read the arguments
	(opts, action, cluster_name) = parse_args()

	# Make sure gcloud is accessible.
	check_gcloud(cluster_name, opts)

	# Launch the cluster
	if action == "launch":
		launch_cluster(cluster_name, opts)

	elif action == "start":
		start_cluster(cluster_name, opts)

	elif action == "stop":
		stop_cluster(cluster_name, opts)

	elif action == "destroy":
		destroy_cluster(cluster_name, opts)

	elif action == "login" or action == "ssh":
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
