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

# Global Variables
#
# We use just a few of these to keep the code below clean and simple
VERBOSE = 0

# Determine the path of the spark_gce.py file. We assume that all of the
# templates and auxillary shell scripts are located here as well.
SPARK_GCE_PATH =  os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(SPARK_GCE_PATH, "templates")):
    raise Exception("There was an error locating installation support files. Spark GCE is not installed properly.  Please re-install.")


# Instance info lookup table
#
# This information is used to automatically set and tune various Spark
# parameters based on the cluster instance type.
instance_info = {
    "n1-standard-4"  : { "num_cpus":  4, "gb_mem": 15,   "spark_master_memory": "10g" , "spark_slave_memory": "11g" },
    "n1-standard-8"  : { "num_cpus":  8, "gb_mem": 30,   "spark_master_memory": "20g" , "spark_slave_memory": "25g" },
    "n1-standard-16" : { "num_cpus": 16, "gb_mem": 60,   "spark_master_memory": "40g" , "spark_slave_memory": "50g" },
    "n1-standard-32" : { "num_cpus": 32, "gb_mem": 120,  "spark_master_memory": "50g" , "spark_slave_memory": "95g" },
    "n1-highmem-4"   : { "num_cpus":  4, "gb_mem": 26,   "spark_master_memory": "15g" , "spark_slave_memory": "22g" },
    "n1-highmem-8"   : { "num_cpus":  8, "gb_mem": 52,   "spark_master_memory": "30g" , "spark_slave_memory": "45g" },
    "n1-highmem-16"  : { "num_cpus": 16, "gb_mem": 104,  "spark_master_memory": "50g" , "spark_slave_memory": "90g" },
    "n1-highmem-32"  : { "num_cpus": 32, "gb_mem": 208,  "spark_master_memory": "80g" , "spark_slave_memory": "190g" },
    "n1-highcpu-8"   : { "num_cpus":  8, "gb_mem": 7.2,  "spark_master_memory": "4g"  , "spark_slave_memory": "6g"  },
    "n1-highcpu-16"  : { "num_cpus": 16, "gb_mem": 14.4, "spark_master_memory": "10g" , "spark_slave_memory": "11g" },
    "n1-highcpu-32"  : { "num_cpus": 32, "gb_mem": 28.8, "spark_master_memory": "20g" , "spark_slave_memory": "25g" },
}


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
            
def run(cmds, parallelize = False, terminate_on_failure = True):
    """Run commands in serial or in parallel.

    If cmds is a single command, it will run the command.
    
    If cmds is a list of commands, the commands will be run in serial or
    parallel depending on the 'parallelize' flag.

    If cmds is a list of lists of commands, the inner lists of commands will be executed in
    serial within parallel threads.  (i.e. parallelism on the outer list)

    """
    global VERBOSE

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

    # SSH commands don't take many local machine resources, so we can safely
    # fire off a ridiculous number of them (up to 64, and then we relent
    # and start to allow some commands to finish before starting others).
    num_threads = min(len(cmds), 64)
        
    # For debugging purposes (if VERBOSE is True), print the commands we are
    # about to execute.
    if VERBOSE >= 2:
        if isinstance(cmds[0], list):
            for cmd_group in cmds:
                print "[ Command Group (will execute in parallel on %d threads) ]" % (num_threads)
                for cmd in cmd_group:
                    print "  CMD: ", cmd
        else:
            for cmd in cmds:
                print "  CMD: ", cmd
        
    if not parallelize:

        # Run commands serially
        run_subprocess(cmds, result_queue)
                
    else:
        pool = multiprocessing.Pool(processes = num_threads)
        
        # Start worker threads.
        for cmd in cmds:
            pool.apply_async(run_subprocess, args=(cmd, result_queue))
        pool.close()
        pool.join()

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
    else:
        return num_failed
    

def ssh_wrap(host, identity_file, cmds, group = False):
    '''Given a command to run on a remote host, this function wraps the command in
    the appropriate ssh invocation that can be run on the local host to achieve
    the desired action on the remote host. This can then be passed to the run()
    function to execute the command on the remote host.

    This function can take a single command or a list of commands, and will
    return a list of ssh-wrapped commands. However, if group = True, the list
    of commands will be combined via the && shell operator and sent in a single
    ssh command.
    '''
    global VERBOSE

    if not isinstance(cmds, list):
        cmds = [ cmds ]

    for cmd in cmds:
        if VERBOSE >= 1: print '  SSH: ' + host['host_name'] + '\t', cmd

    # Group commands using && to reduce the number of SSH commands that are
    # executed.  This can speed things up on high latency connections.
    if group == True:
        cmds = [ ' && '.join(cmds) ]


    username = os.environ["USER"]
    result = []
    for cmd in cmds:
        if host['external_ip'] is None:
            print "Error: attempting to ssh into machine instance \"%s\" without a public IP address.  Exiting." % (host["host_name"])
            sys.exit(1)
            
        result.append( "ssh -i " + identity_file + " -o ConnectTimeout=900 -o \"BatchMode yes\" -o ServerAliveInterval=60 -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no -o LogLevel=quiet " + username + "@" + host['external_ip'] + " '" + cmd + "'" )
    return result

def deploy_template(opts, node, template_name):
    global VERBOSE
    command_prefix = get_command_prefix(opts)

    cmd = command_prefix + " copy-files " + SPARK_GCE_PATH + "/templates/" + template_name + " " + node['host_name'] + ":" + template_name + " --zone " + node['zone']
    if VERBOSE >= 1:
        print "  TEMPLATE: ", template_name

    # Run the command
    run(cmd)

# -------------------------------------------------------------------------------------

def get_command_prefix(opts):
    command_prefix = 'gcloud compute'
    if opts.project:
        command_prefix += ' --project ' + opts.project
    return command_prefix

def check_gcloud(cluster_name, opts):
    global VERBOSE
    
    cmd = "gcloud info"
    try:
        output = subprocess.check_output(cmd, shell=True)
        if VERBOSE >= 1:
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
        cmds = []
        for node in [master_node] + slave_nodes:
            # Check if the node has a public IP assigned yet. If not, the node
            # is still launching.
            if node['external_ip'] is None:
                cluster_is_ready = False
                break
            else:
                # If the cluster node has an IP, then let's try to connect over
                # ssh.  If that fails, the node is still not ready.
                cmds.append(ssh_wrap(node, opts.identity_file, 'echo'))
                
        num_failed = run(cmds, terminate_on_failure = False)
        if num_failed > 0:
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
    command_prefix = get_command_prefix(opts)
        
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
    command_prefix = get_command_prefix(opts)
    
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
    command_prefix = get_command_prefix(opts)
    cmds = []

    cmds.append( command_prefix + ' networks create "' + cluster_name + '-network" --range "10.240.0.0/16"' )
    
    # Uncomment the above and comment the below section if you don't want to open all ports for public.
    # cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal' )
    cmds.append( command_prefix + ' firewall-rules create ' + cluster_name + '-internal --network ' + cluster_name + '-network --allow tcp udp icmp' )
    cmds.append( command_prefix + ' firewall-rules create ' + cluster_name + '-spark-external --network ' + cluster_name + '-network --allow tcp:8080 tcp:4040 tcp:5080' )
    run(cmds)

def delete_network(cluster_name, opts):
    print '[ Deleting Network & Firewall Entries ]'
    command_prefix = get_command_prefix(opts)
    cmds = []

    # Uncomment the above and comment the below section if you don't want to open all ports for public.
    # cmds.append( command_prefix + ' compute firewall-rules delete ' + cluster_name + '-internal --quiet' )
    cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-internal --quiet' )
    cmds.append(  command_prefix + ' firewall-rules delete ' + cluster_name + '-spark-external --quiet' )
    cmds.append( command_prefix + ' networks delete "' + cluster_name + '-network" --quiet' )
    run(cmds)

# -------------------------------------------------------------------------------------
#                      CLUSTER LAUNCH, DESTROY, START, STOP
# -------------------------------------------------------------------------------------

def launch_cluster(cluster_name, opts):
    """
    Create a new cluster. 
    """
    print '[ Launching cluster: %s ]' % cluster_name
    command_prefix = get_command_prefix(opts)

    if opts.zone:
        zone_str = ' --zone ' + opts.zone
    else:
        zone_str = ''

    # Set up the network
    setup_network(cluster_name, opts)
 
    # Start master nodes & slave nodes
    cmds = []
    cmds.append( command_prefix + ' instances create "' + cluster_name + '-master" --machine-type "' + opts.master_instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.full_control" --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150316" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-md" --metadata startup-script-url=http://storage.googleapis.com/spark-gce/growroot.sh' + zone_str )
    for i in xrange(opts.slaves):
        cmds.append( command_prefix + ' instances create "' + cluster_name + '-slave' + str(i) + '" --machine-type "' + opts.slave_instance_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.full_control" --image "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-1404-trusty-v20150316" --boot-disk-type "' + opts.boot_disk_type + '" --boot-disk-size ' + opts.boot_disk_size + ' --boot-disk-device-name "' + cluster_name + '-s' + str(i) + 'd" --metadata startup-script-url=http://storage.googleapis.com/spark-gce/growroot.sh' + zone_str )

    print '[ Launching nodes ]'
    run(cmds, parallelize = True)

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
    command_prefix = get_command_prefix(opts)

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
        run(cmds, parallelize = True)

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
    command_prefix = get_command_prefix(opts)

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
    run(cmds, parallelize = True, terminate_on_failure = False)

    # Clean up scratch disks
    cleanup_scratch_disks(cluster_name, opts, detach_first = True)

    #else:
    #   print "\nExiting without stopping cluster %s." % (cluster_name)
    #   sys.exit(0)

            
def start_cluster(cluster_name, opts):
    """
    Start a cluster that is in the stopped state.
    """
    print '[ Starting cluster: %s ]' % (cluster_name)
    command_prefix = get_command_prefix(opts)

    # Get cluster machines
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
    if (master_node is None) and (len(slave_nodes) == 0):
        print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
        sys.exit(1)

    cmds = []
    for instance in [master_node] + slave_nodes:
        cmds.append( command_prefix + ' instances start ' + instance['host_name'] + ' --zone ' + instance['zone'] )

    print '[ Starting nodes ]'
    run(cmds, parallelize = True)

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


# -------------------------------------------------------------------------------------
#                      INSTALLATION AND CONFIGURATION HELPERS
# -------------------------------------------------------------------------------------
    
def deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes):
    command_prefix = get_command_prefix(opts)

    print '[ Generating SSH keys on master and deploying to slave nodes ]'

    # Create keys on the master node, add them into authorized_keys, and then
    # pack up the archive. We also also pre-scan the ssh host identities of the
    # slaves so that we can avoid KnownHostErrors.
    cmds = [ "rm -f ~/.ssh/id_rsa && rm -f ~/.ssh/known_hosts",
             "ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa",
             "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
             "ssh-keyscan -H " + master_node['host_name'] + " >> ~/.ssh/known_hosts" ]
    for slave in slave_nodes:
        cmds.append( "ssh-keyscan -H " + slave['host_name'] + " >> ~/.ssh/known_hosts"  )
    cmds.append("tar czf .ssh.tgz .ssh");
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Copy the ssh keys locally, and Clean up the archive on the master node.
    run(command_prefix + " copy-files " + cluster_name + "-master:.ssh.tgz /tmp/spark_gce_ssh.tgz --zone " + master_node['zone'])
    run(ssh_wrap(master_node, opts.identity_file, "rm -f .ssh.tgz"))

    # Upload to the slaves, and unpack the ssh keys on the slave nodes.
    run([command_prefix + " copy-files /tmp/spark_gce_ssh.tgz " + slave['host_name'] + ": --zone " + slave['zone'] for slave in slave_nodes], parallelize = True)
    run( [ssh_wrap(slave,
                   opts.identity_file,
                   "rm -rf ~/.ssh && tar xzf spark_gce_ssh.tgz && rm spark_gce_ssh.tgz")
          for slave in slave_nodes], parallelize = True)

    # Clean up: delete the local copy of the ssh keys
    run("rm -f /tmp/spark_gce_ssh.tgz")


def attach_local_ssd(cluster_name, opts, master_node, slave_nodes):

    print '[ Attaching 350GB NVME SSD drive to each node under /mnt ]'

    cmds = [ 'if [ ! -d /mnt ]; then sudo mkdir /mnt; fi',
             'sudo /usr/share/google/safe_format_and_mount -m "mkfs.ext4 -F" /dev/disk/by-id/google-local-ssd-0 /mnt',
             'sudo chmod a+w /mnt']

    cmds = [ ssh_wrap(slave, opts.identity_file, cmds, group = True) for slave in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

def cleanup_scratch_disks(cluster_name, opts, detach_first = True):
    cmd_prefix = get_command_prefix(opts)

    # Get cluster ips
    print '[ Detaching and deleting cluster scratch disks ]'
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)

    # Detach drives
    if detach_first:
        cmds = []
        cmds = [ cmd_prefix + ' instances detach-disk ' + cluster_name + '-master --disk ' + cluster_name + '-m-scratch --zone ' + master_node['zone'] ]
        for i, slave in enumerate(slave_nodes):
            cmds.append( cmd_prefix + ' instances detach-disk ' + cluster_name + '-slave' + str(slave['slave_id']) + ' --disk ' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch --zone ' + slave['zone'] )
        run(cmds, parallelize = True, terminate_on_failure = False)

    # Delete drives
    cmds = [ cmd_prefix + ' disks delete "' + cluster_name + '-m-scratch" --quiet --zone ' + master_node['zone'] ] 
    for i, slave in enumerate(slave_nodes):
        cmds.append( cmd_prefix + ' disks delete "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --quiet --zone ' + slave['zone'] )
    run(cmds, parallelize = True, terminate_on_failure = False)


def attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes):
    cmd_prefix = get_command_prefix(opts)

    print '[ Adding new ' + opts.scratch_disk_size + ' drive of type "' + opts.scratch_disk_type + '" to each cluster node ]'

    cmds = []
    cmds.append( [cmd_prefix + ' disks create "' + cluster_name + '-m-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '" --zone ' + master_node['zone'],
                  cmd_prefix + ' instances attach-disk ' + cluster_name + '-master --device-name "' + cluster_name + '-m-scratch" --disk ' + cluster_name + '-m-scratch --zone ' + master_node['zone'],
                  ssh_wrap(master_node, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-m-scratch " + " -F < /dev/null && " + 
                           "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-scratch /mnt && " + 
                           'sudo chown "$USER":"$USER" /mnt') ] )

    for slave in slave_nodes:
        cmds.append( [ cmd_prefix + ' disks create "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '" --zone ' + slave['zone'],
                       cmd_prefix + ' instances attach-disk ' + cluster_name + '-slave' +  str(slave['slave_id']) + ' --disk ' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch --device-name "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --zone ' + slave['zone'],
                       ssh_wrap(slave, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-s" + str(slave['slave_id']) + "-scratch " + " -F < /dev/null && " + 
                                "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(slave['slave_id']) + "-scratch /mnt && " + 
                                'sudo chown "$USER":"$USER" /mnt' ) ] )

    run(cmds, parallelize = True)
    print '[ All volumes mounted, will be available at /mnt ]'
            
    
def initialize_cluster(cluster_name, opts, master_node, slave_nodes):
    print '[ Installing software dependencies (this will take several minutes) ]'
    command_prefix = get_command_prefix(opts)

    # Install the copy-dir script
    run(command_prefix + " copy-files " + SPARK_GCE_PATH + "/copy-dir " + cluster_name + "-master: --zone " + master_node['zone'])
    cmds = ['mkdir -p $HOME/spark/bin && mkdir -p $HOME/spark/conf',
            'mv $HOME/copy-dir $HOME/spark/bin',
            'chmod 755 $HOME/spark/bin/copy-dir']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Create a file containing the list of all current slave nodes 
    import tempfile
    slave_file = tempfile.NamedTemporaryFile(delete = False)
    for slave in slave_nodes:
        slave_file.write(slave['host_name'] + '\n')
    slave_file.close()
    run(command_prefix + " copy-files " + slave_file.name + " " + cluster_name + "-master:spark/conf/slaves --zone " + master_node['zone'])
    os.unlink(slave_file.name)

    # Download Anaconda, and copy to slave nodes
    cmds = [ 'wget http://storage.googleapis.com/spark-gce/packages/Anaconda-2.1.0-Linux-x86_64.sh',
             '$HOME/spark/bin/copy-dir Anaconda-2.1.0-Linux-x86_64.sh']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))
        
    cmds = [ 'sudo apt-get update -q -y',

             # Install basic packages
             'sudo apt-get install -q -y screen less git mosh pssh emacs bzip2 dstat iotop strace sysstat htop g++ openjdk-7-jdk',

             # Rspark dependencies
             'sudo apt-get install -q -y R-base realpath',  # Realpath is used by R to find java installations

             # PySpark dependencies
             'rm -rf $HOME/anaconda && bash Anaconda-2.1.0-Linux-x86_64.sh -b && rm Anaconda-2.1.0-Linux-x86_64.sh',

             # Set system path
             'echo \'export PATH=\$HOME/anaconda/bin:\$PATH:\$HOME/spark/bin:\$HOME/ephemeral-hdfs/bin\' >> $HOME/.bashrc'
         ]
    cmds = [ ssh_wrap(node, opts.identity_file, cmds, group = True) for node in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

def install_spark(cluster_name, opts, master_node, slave_nodes):
    command_prefix = get_command_prefix(opts)
    print '[ Installing Spark ]'

    # Install Spark and Scala
    cmds = [ 'mkdir -p $HOME/packages',
             'cd $HOME/packages && wget http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-1.3.0/spark-1.3.0-bin-cdh4.tgz',
             'cd $HOME/packages && tar xvf spark-1.3.0-bin-cdh4.tgz && rm spark-1.3.0-bin-cdh4.tgz',
             'rm -rf $HOME/spark && ln -s $HOME/packages/spark-1.3.0-bin-cdh4 $HOME/spark',
             'cd $HOME/packages && wget http://downloads.typesafe.com/scala/2.11.6/scala-2.11.6.tgz',
             'cd $HOME/packages && tar xvzf scala-2.11.6.tgz && rm -rf scala-2.11.6.tgz']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Set up the spark-env.conf and spark-defaults.conf files
    deploy_template(opts, master_node, "spark/conf/spark-env.sh")
    deploy_template(opts, master_node, "spark/conf/core-site.xml")
    deploy_template(opts, master_node, "spark/conf/spark-defaults.conf")
    deploy_template(opts, master_node, "spark/setup-auth.sh")
    cmds = ['echo \'export SPARK_HOME=\$HOME/spark\' >> $HOME/.bashrc',
            'echo \'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64\' >> $HOME/.bashrc',
            '$HOME/spark/setup-auth.sh',

            # spark-env.conf
            'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/spark/conf/spark-env.sh',
            'sed -i "s/{{worker_cores}}/' + str(instance_info[opts.slave_instance_type]["num_cpus"]) + '/g" $HOME/spark/conf/spark-env.sh',
            'sed -i "s/{{spark_master_memory}}/' + instance_info[opts.master_instance_type]["spark_master_memory"] + '/g" $HOME/spark/conf/spark-env.sh',
            'sed -i "s/{{spark_slave_memory}}/' + instance_info[opts.slave_instance_type]["spark_slave_memory"] + '/g" $HOME/spark/conf/spark-env.sh',

            # core-site.xml
            'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/spark/conf/core-site.xml',

            # spark-defaults.conf
            'sed -i "s/{{spark_master_memory}}/' + instance_info[opts.master_instance_type]["spark_master_memory"] + '/g" $HOME/spark/conf/spark-defaults.conf',
            'sed -i "s/{{spark_slave_memory}}/' + instance_info[opts.slave_instance_type]["spark_slave_memory"] + '/g" $HOME/spark/conf/spark-defaults.conf',

    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # (Re-)populate the file containing the list of all current slave nodes 
    import tempfile
    slave_file = tempfile.NamedTemporaryFile(delete = False)
    for slave in slave_nodes:
        slave_file.write(slave['host_name'] + '\n')
    slave_file.close()
    cmds = [ command_prefix + " copy-files " + slave_file.name + " " + cluster_name + "-master:spark/conf/slaves --zone " + master_node['zone']]
    run(cmds)
    os.unlink(slave_file.name)
    
    # Install the copy-dir script
    run(command_prefix + " copy-files " + SPARK_GCE_PATH + "/copy-dir " + cluster_name + "-master:spark/bin --zone " + master_node['zone'])
    
    # Copy spark to the slaves and create a symlink to $HOME/spark
    run(ssh_wrap(master_node, opts.identity_file, '$HOME/spark/bin/copy-dir $HOME/packages/spark-1.3.0-bin-cdh4'))
    run([ ssh_wrap(slave, opts.identity_file, 'rm -f $HOME/spark && ln -s $HOME/packages/spark-1.3.0-bin-cdh4 $HOME/spark') for slave in slave_nodes ], parallelize = True)


def configure_and_start_spark(cluster_name, opts, master_node, slave_nodes):
    print '[ Configuring Spark ]'

    # Create the Spark scratch directory on the local SSD
    run([ ssh_wrap(node, opts.identity_file, 'if [ ! -d /mnt/spark ]; then mkdir /mnt/spark; fi') for node in [master_node] + slave_nodes ], parallelize = True)

    # Patches to address system performance issues. These mimic settings used in
    # the spark-ec2 scripts.

    # Disable Transparent Huge Pages (THP)
    # THP can result in system thrashing (high sys usage) due to frequent defrags of memory.
    # Most systems recommends turning THP off.
    run([ ssh_wrap(node, opts.identity_file, 'if [[ -e /sys/kernel/mm/transparent_hugepage/enabled ]]; then sudo sh -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"; fi') for node in [master_node] + slave_nodes ], parallelize = True)

    # Allow memory to be over committed. Helps in pyspark where we fork
    run([ ssh_wrap(node, opts.identity_file, 'sudo sh -c "echo 1 > /proc/sys/vm/overcommit_memory"') for node in [master_node] + slave_nodes ], parallelize = True)

    # Start Spark
    print '[ Starting Spark ]'
    run(ssh_wrap(master_node, opts.identity_file, '$HOME/spark/sbin/start-all.sh') )

    
def configure_ganglia(cluster_name, opts, master_node, slave_nodes):
    print '[ Configuring Ganglia ]'
    
    run(ssh_wrap(master_node, opts.identity_file, "mkdir -p ganglia", group = True))

    deploy_template(opts, master_node, "ganglia/ports.conf")
    deploy_template(opts, master_node, "ganglia/ganglia.conf")
    deploy_template(opts, master_node, "ganglia/gmetad.conf")
    deploy_template(opts, master_node, "ganglia/gmond.conf")
    deploy_template(opts, master_node, "ganglia/000-default.conf")
    
    # Install gmetad and the ganglia web front-end on the master node
    cmds = [ 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -q -y ganglia-webfrontend gmetad ganglia-monitor',
             'sudo cp $HOME/ganglia/ports.conf /etc/apache2/',
             'sudo cp $HOME/ganglia/000-default.conf /etc/apache2/sites-enabled/',
             'sudo cp $HOME/ganglia/ganglia.conf /etc/apache2/sites-enabled/'
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))
    
    # Set up ganglia to log data to the scratch drive, since the root drive is small
    cmds = ['sudo rm -rf /var/lib/ganglia/rrds/* && sudo rm -rf /mnt/ganglia/rrds/*',
             'sudo mkdir -p /mnt/ganglia/rrds',
             'sudo chown -R nobody:root /mnt/ganglia/rrds',
             'sudo rm -rf /var/lib/ganglia/rrds',
             'sudo ln -s /mnt/ganglia/rrds /var/lib/ganglia/rrds'
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Configure gmond and gmetad on the master node
    cmds = ['sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" $HOME/ganglia/gmetad.conf',
            'sudo cp $HOME/ganglia/gmetad.conf /etc/ganglia/',
            'sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" $HOME/ganglia/gmond.conf', 
            'sudo cp $HOME/ganglia/gmond.conf /etc/ganglia/',
            'sudo service gmetad restart && sudo service ganglia-monitor restart && sudo service apache2 restart'
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Sleep for a sec
    import time
    time.sleep(2)

    # Install and configure gmond everywhere
    run(ssh_wrap(master_node, opts.identity_file, "$HOME/spark/bin/copy-dir $HOME/ganglia", group = True))
    cmds = [ 'sudo apt-get install -q -y ganglia-monitor gmetad',
             'sudo rm -rf /var/lib/ganglia/rrds/* && sudo rm -rf /mnt/ganglia/rrds/*',
             'sudo mkdir -p /mnt/ganglia/rrds',
             'sudo chown -R nobody:root /mnt/ganglia/rrds',
             'sudo rm -rf /var/lib/ganglia/rrds',
             'sudo ln -s /mnt/ganglia/rrds /var/lib/ganglia/rrds',

             'sudo cp $HOME/ganglia/gmond.conf /etc/ganglia/',
             'sudo cp $HOME/ganglia/gmetad.conf /etc/ganglia/',
             'sudo rm -rf $HOME/ganglia',
             'sudo service ganglia-monitor restart']
    cmds = [ssh_wrap(node, opts.identity_file, cmds, group = True) for node in slave_nodes]
    run(cmds, parallelize = True)


def install_hadoop(cluster_name, opts, master_node, slave_nodes):

    print '[ Installing hadoop (this will take several minutes) ]'

    # Download pre-built Hadoop Native libraries
    cmds = ['sudo apt-get install -q -y protobuf-compiler cmake libssl-dev maven pkg-config libsnappy-dev',
            'wget http://storage.googleapis.com/spark-gce/packages/hadoop-native.tgz',
            'tar xvzf hadoop-native.tgz && rm -f hadoop-native.tgz']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))
    
    # Builds native hadoop libaries from source (slow!)
    # cmds = [
    #     'cd /tmp && wget "http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1-src.tar.gz"',
    #     'cd /tmp && tar xvzf hadoop-2.4.1-src.tar.gz && rm -f hadoop-2.4.1-src.tar.gz',
    #     'sudo apt-get install -q -y protobuf-compiler cmake libssl-dev maven pkg-config libsnappy-dev',
    #     'cd /tmp/hadoop-2.4.1-src && mvn package -Pdist,native -DskipTests -Dmaven.javadoc.skip=true -Dtar',
    #     'mkdir -p $HOME/hadoop-native && sudo mv /tmp/hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1/lib/native/* $HOME/hadoop-native',
    #     'cp /usr/lib/libsnappy.so.1 $HOME/hadoop-native',
    #     'rm -rf /tmp/hadoop-2.4.1-src',
    # ]
    # run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))
    
    # Install Hadoop 2.0
    cmds = ['wget http://s3.amazonaws.com/spark-related-packages/hadoop-2.0.0-cdh4.2.0.tar.gz',
        'tar xvzf hadoop-*.tar.gz > /tmp/spark-ec2_hadoop.log',
        'rm hadoop-*.tar.gz',
        'rm -rf ephemeral-hdfs && mv hadoop-2.0.0-cdh4.2.0 ephemeral-hdfs',
        'rm -rf $HOME/ephemeral-hdfs/etc/hadoop/',   # Use a single conf directory
        'mkdir -p $HOME/ephemeral-hdfs/conf && ln -s $HOME/ephemeral-hdfs/conf $HOME/ephemeral-hdfs/etc/hadoop',
        'cp $HOME/hadoop-native/* $HOME/ephemeral-hdfs/lib/native/',
        'cp $HOME/spark/conf/slaves $HOME/ephemeral-hdfs/conf/',

        # Install the Google Storage adaptor
        'cd $HOME/ephemeral-hdfs/share/hadoop/hdfs/lib && rm -f gcs-connector-latest-hadoop2.jar && wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar',
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))
        
    # Set up Hadoop configuration files from the templates
    deploy_template(opts, master_node, "ephemeral-hdfs/conf/core-site.xml")
    deploy_template(opts, master_node, "ephemeral-hdfs/conf/hadoop-env.sh")
    deploy_template(opts, master_node, "ephemeral-hdfs/conf/hadoop-metrics2.properties")
    deploy_template(opts, master_node, "ephemeral-hdfs/conf/hdfs-site.xml")
    deploy_template(opts, master_node, "ephemeral-hdfs/conf/mapred-site.xml")
    deploy_template(opts, master_node, "ephemeral-hdfs/conf/masters")

    cmds = [
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/core-site.xml',
        'sed -i "s/{{java_home}}/\/usr\/lib\/jvm\/java-1.7.0-openjdk-amd64/g" $HOME/ephemeral-hdfs/conf/hadoop-env.sh',
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/hadoop-metrics2.properties',
        'sed -i "s/{{hdfs_data_dirs}}/\/mnt\/hadoop\/dfs\/data/g" $HOME/ephemeral-hdfs/conf/hdfs-site.xml',
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/mapred-site.xml',
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" $HOME/ephemeral-hdfs/conf/masters',
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Set up authentication for the Hadoop Google Storage adaptor
    deploy_template(opts, master_node, "ephemeral-hdfs/setup-auth.sh")
    deploy_template(opts, master_node, "ephemeral-hdfs/setup-slave.sh")
    run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/setup-auth.sh'))

    # Copy the hadoop directory from the master node to the slave nodes
    run(ssh_wrap(master_node, opts.identity_file,'$HOME/spark/bin/copy-dir $HOME/ephemeral-hdfs'))

    
def configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes):

    print '[ Configuring hadoop ]'

    # Set up the ephemeral HDFS directories
    cmds = [ ssh_wrap(node, opts.identity_file,'$HOME/ephemeral-hdfs/setup-slave.sh') for node in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

    # Format the ephemeral HDFS
    run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/bin/hadoop namenode -format'))

    # Start Hadoop HDFS
    run(ssh_wrap(master_node, opts.identity_file,'$HOME/ephemeral-hdfs/sbin/start-dfs.sh'))

    
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
        "-t", "--slave-instance-type", default="n1-highmem-16",
        help="Type of instance to launch (default: n1-highmem-16).")
    parser.add_option(
        "-m", "--master-instance-type", default="n1-highmem-16",
        help="Master instance type (default: n1-highmem-16)")
    parser.add_option(
        "--boot-disk-type", default="pd-standard",
        help="Boot disk type.  Run \'gcloud compute disk-types list\' to see your options.")
    parser.add_option(
        "--scratch-disk-type", default="pd-standard",
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
        "-z", "--zone", default="us-central1-b",
        help="GCE zone to target when launching instances ( you can omit this argument if you set a default with \'gcloud config set compute/zone [zone-name]\'")
    parser.add_option("--verbose", type = int, default = 0,
                      help="Set debugging level (0 - minimal, 1 - some, 2 - a lot).")
    parser.add_option("--ssh-tunnel", default=None,
                      help="Set up ssh port forwarding when you login to the cluster.  " +
                      "This provides a convenient alternative to connecting to iPython " +
                      "notebook over an open port using SSL.  You must supply an argument " +
                      "of the form \"local_port:remote_port\".")

    (opts, args) = parser.parse_args()
    if (len(args) < 2) or (len(args) > 3):
        parser.print_help()
        sys.exit(1)

    if not opts.master_instance_type in instance_info.keys():
        print 'Error: the selected master_instance_type was not recognized, or was an instance type without enough RAM to be a Spark master node.  Select from the list of instances below:'
        print instance_info.keys()
        sys.exit(1)

    if not opts.slave_instance_type in instance_info.keys():
        print 'Error: the selected slave_instance_type was not recognized, or was an instance type without enough RAM to be a Spark slave node.  Select from the list of instances below:'
        print instance_info.keys()
        sys.exit(1)

    global VERBOSE
    VERBOSE = opts.verbose
        
    try:
        (action, cluster_name, optional_arg) = args
        return (opts, action, cluster_name, optional_arg)
    except ValueError:
        (action, cluster_name) = args
        return (opts, action, cluster_name, None)

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
    if opts.ssh_tunnel is not None:
        ssh_ports = opts.ssh_tunnel.split(":")
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

def sshfs_cluster(cluster_name, opts, optional_arg):

    if optional_arg is None:
        print "\nCommand failed.  You must specify a local mount point."
        sys.exit(1)

    # Attempt to create the local directory if it doesn't already exist.
    os.makedirs(optional_arg)
    
    import subprocess
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
    if (master_node is None) and (len(slave_nodes) == 0):
        print 'Command failed.  Could not find a cluster named "' + cluster_name + '".'
        sys.exit(1)

        
    cmd = 'sshfs -o auto_cache,idmap=user,noappledouble,noapplexattr,ssh_command="ssh -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no" ' + str(master_node['external_ip'] + ': ' + optional_arg)
    subprocess.check_call(shlex.split(cmd))
    print 'Mounted your home directory on ' + cluster_name + '-master under local directory \"' + optional_arg + '\" using SSHFS.'
        
def real_main():

    print "Spark for Google Compute Engine v0.2"
    print ""

    # Read the arguments
    (opts, action, cluster_name, optional_arg) = parse_args()

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

    elif action == "sshfs":
        sshfs_cluster(cluster_name, opts, optional_arg)

    else:
        print >> stderr, "Invalid action: %s" % action
        sys.exit(1)


def main():
    real_main()

if __name__ == "__main__":
    main()
