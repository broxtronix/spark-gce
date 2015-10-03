# Copyright 2015 Michael Broxton
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

###
# This script sets up a Spark cluster on Google Compute Engine
#
# This script originally was written by @SigmoidAnalytics, and then later
# modified by @broxtronix.
###

import os
import sys
import time
import shlex
import json
import itertools
from datetime import datetime
from sys import stderr
from collections import namedtuple

import boto
import boto.ec2

# Global Variables
#
# We use just a few of these to keep the code below clean and simple
VERBOSE = 0
COMMAND_PREFIX = None

# Determine the path of the ec2.py file. We assume that all of the
# templates and auxillary shell scripts are located here as well.
SPARK_EC2_PATH =  os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(SPARK_EC2_PATH, os.path.join("support_files_ec2", "templates"))):
    raise Exception("There was an error locating installation support files. Spark EC2 is not installed properly.  Please re-install.")


# Instance info lookup table
#
# This information is used to automatically set and tune various Spark
# parameters based on the cluster instance type.
instance_info = {
    'm3.large'    : { "num_cpus":   2, "gb_mem": 7.5,  "spark_master_memory": "4g" , "spark_slave_memory": "5g" },
    "g2.8xlarge"  : { "num_cpus":  32, "gb_mem": 60,   "spark_master_memory": "30g" , "spark_slave_memory": "40g" },
}

# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2015-06-19
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c1.medium":   1,
        "c1.xlarge":   4,
        "c3.large":    2,
        "c3.xlarge":   2,
        "c3.2xlarge":  2,
        "c3.4xlarge":  2,
        "c3.8xlarge":  2,
        "c4.large":    0,
        "c4.xlarge":   0,
        "c4.2xlarge":  0,
        "c4.4xlarge":  0,
        "c4.8xlarge":  0,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "cr1.8xlarge": 2,
        "d2.xlarge":   3,
        "d2.2xlarge":  6,
        "d2.4xlarge":  12,
        "d2.8xlarge":  24,
        "g2.2xlarge":  1,
        "g2.8xlarge":  2,
        "hi1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "i2.xlarge":   1,
        "i2.2xlarge":  2,
        "i2.4xlarge":  4,
        "i2.8xlarge":  8,
        "m1.small":    1,
        "m1.medium":   1,
        "m1.large":    2,
        "m1.xlarge":   4,
        "m2.xlarge":   1,
        "m2.2xlarge":  1,
        "m2.4xlarge":  2,
        "m3.medium":   1,
        "m3.large":    1,
        "m3.xlarge":   2,
        "m3.2xlarge":  2,
        "m4.large":    0,
        "m4.xlarge":   0,
        "m4.2xlarge":  0,
        "m4.4xlarge":  0,
        "m4.10xlarge": 0,
        "r3.large":    1,
        "r3.xlarge":   1,
        "r3.2xlarge":  1,
        "r3.4xlarge":  1,
        "r3.8xlarge":  2,
        "t1.micro":    0,
        "t2.micro":    0,
        "t2.small":    0,
        "t2.medium":   0,
        "t2.large":    0,
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print("WARNING: Don't know number of disks on instance type %s; assuming 1"
              % instance_type, file=stderr)
        return 1

def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in range(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
               all(s.system_status.status == 'ok' for s in statuses) and \
               all(s.instance_status.status == 'ok' for s in statuses) and \
               is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


# Gets a list of zones to launch instances in
def get_zones(conn, opts):
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones

# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
    num_slaves_this_zone = total // num_partitions
    if (total % num_partitions) - current_partitions > 0:
        num_slaves_this_zone += 1
    return num_slaves_this_zone


# Gets the IP address, taking into account the --private-ips flag
def get_ip_address(instance, private_ips=False):
    ip = instance.ip_address if not private_ips else \
         instance.private_ip_address
    return ip


# Gets the DNS name, taking into account the --private-ips flag
def get_dns_name(instance, private_ips=False):
    dns = instance.public_dns_name if not private_ips else \
          instance.private_ip_address
    return dns
    
# Open a connection to the specified EC2 region using boto.
# Returns the (open) connection object.
#
def open_ec2_connection(opts):
    from boto import ec2
    try:
        conn = ec2.connect_to_region(opts.region)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)

    # Select an AZ at random if it was not specified.
    if opts.zone == "":
        import random
        opts.zone = random.choice(conn.get_all_zones()).name

    return conn


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
        if VERBOSE >= 3:
            print("[CMD] " + cmd)
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
        
    # For debugging purposes (if VERBOSE is >= 2), print the commands we are
    # about to execute.
    if VERBOSE >= 2:
        if isinstance(cmds[0], list):
            for cmd_group in cmds:
                print("[ Command Group (will execute in parallel on %d threads) ]" % (num_threads))
                for cmd in cmd_group:
                    print("  CMD: ", cmd)
        else:
            for cmd in cmds:
                print("  CMD: ", cmd)
        
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
        print("\n******************************************************************************************")
        print("\nCall to subprocess failed.  %d commands in this set returned a non-zero exit status." % (num_failed))
        print("\nFirst failed command:\n")
        print(failed_cmds[0])
        print("\nCommand output:\n")
        print(failed_stdout[0])
        print("******************************************************************************************")
        sys.exit(1)
    else:
        return num_failed

def check_ssh_config(cluster_name, opts):
    '''This function checks to make sure that the user's ssh keys are set up to
    access EC2 instances without an interactive password prompt.
    '''
    
    # First, check to make sure the user's ssh keys for Google compute engine
    # exist and ssh-agent is running.
    if not os.path.exists(os.path.expanduser("~/.ssh/google_compute_engine")):
        print("ERROR: Your SSH keys for google compute engine (~/.ssh/google_compute_engine) do not exist.  Please generate them using \"ssh-keygen -t rsa -f $HOME/.ssh/google_compute_engine\"")
        sys.exit(1)

    # Check to see if the key is encrypted, and whether it has been added to the
    # user's ssh keychain.
    import subprocess
    is_encrypted = len(subprocess.Popen("grep ENCRYPTED $HOME/.ssh/google_compute_engine", shell=True, stdout=subprocess.PIPE).stdout.read()) > 0
    is_active = len(subprocess.Popen("ssh-add -L | grep $HOME/.ssh/google_compute_engine", shell=True, stdout=subprocess.PIPE).stdout.read()) > 0
        
    if VERBOSE > 0:
        print("[ SSH keys (checking ~/.ssh/google_compute_engine) ]")
        print("  Encrypted: ", is_encrypted)
        print("  Added to ssh keychain: ", is_active)
        print("")

    # For encrypted keys, we need to make sure ssh-agent is running, and the key has been added.
    if is_encrypted:

        # Check whether ssh-agent is running.  We check both for the environment variable, and
        # we make sure that the variable points to a valid SSH auth socket. 
        ssh_auth_sock = os.environ.get("SSH_AUTH_SOCK")
        if not ssh_auth_sock or not os.path.exists(ssh_auth_sock):
            print("ERROR: You do not appear to have ssh-agent running.  You must be running ssh-agent in order to use spark-gce.  You can start it and then try again:")
            print("")
            print("  eval $(ssh-agent)")
            print("  ssh-add ~/.ssh/google_compute_engine")
            print("")
            sys.exit(1)
        
        if not is_active:
            print("ERROR: Your google compute engine key (~/.ssh/google_compute_engine) appears to be password protected, but has not been added to your active ssh keychain.  Please add it and try again:")
            print("")
            print("  ssh-add ~/.ssh/google_compute_engine")
            print("")
            sys.exit(1)

            
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

    if not isinstance(cmds, list):
        cmds = [ cmds ]

    for cmd in cmds:
        if VERBOSE >= 1: print('  SSH: ' + host['host_name'] + '\t', cmd)

    # Group commands using && to reduce the number of SSH commands that are
    # executed.  This can speed things up on high latency connections.
    if group == True:
        cmds = [ ' && '.join(cmds) ]


    username = os.environ["USER"]
    result = []
    for cmd in cmds:
        if host['external_ip'] is None:
            print("Error: attempting to ssh into machine instance \"%s\" without a public IP address.  Exiting." % (host["host_name"]))
            sys.exit(1)
        result.append( COMMAND_PREFIX + ' ssh --ssh-flag="-o ConnectTimeout=900" --ssh-flag="-o BatchMode=yes" --ssh-flag="-o ServerAliveInterval=60" --ssh-flag="-o UserKnownHostsFile=/dev/null" --ssh-flag="-o CheckHostIP=no" --ssh-flag="-o StrictHostKeyChecking=no" --ssh-flag="-o LogLevel=quiet" ' + host['host_name'] + ' --command \'' + cmd + '\'' + " --zone " + host['zone'])
    return result

def deploy_template(opts, node, template_name, root = "/opt"):

    cmd = COMMAND_PREFIX + " copy-files " + SPARK_EC2_PATH + "/support_files/templates/" + template_name + " " + node['host_name'] + ":" + root + "/" + template_name + " --zone " + node['zone']
    if VERBOSE >= 1:
        print("  TEMPLATE: ", template_name)

    # Run the command
    run(cmd)

# -------------------------------------------------------------------------------------

def get_command_prefix(opts):
    command_prefix = 'gcloud compute'
    if opts.project:
        command_prefix += ' --project ' + opts.project
    return command_prefix

def check_aws(cluster_name, opts):
    cmd = "aws configure list"
    try:
        import subprocess
        output = subprocess.check_output(cmd, shell=True)
        if VERBOSE >= 1:
            print('[ Verifying aws command line tools ]')
            print(output)
        
    except OSError:
        print("%s executable not found. \n# Make sure aws command line tools are installed and authenticated\nPlease follow instructions at https://aws.amazon.com/cli/" % myexec)
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
            print("  cluster was not ready.  retrying...")
            time.sleep(retry_delay)
            retries += 1
        else:
            print("Error: cluster took too long to start.")
            sys.exit(1)

def get_cluster_info(cluster_name, opts):
        
    command = COMMAND_PREFIX + ' instances list --format json'
    try:
        output = subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError:
        print("An error occured listing cluster data.")
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
    
    command = COMMAND_PREFIX + ' disks list --format json'
    try:
        output = subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError:
        print("An error occured listing disks.")
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

# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name, vpc_id):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "Spark EC2 group", vpc_id)

# def get_or_create_security_groups(cluster_name, vpc_id) -> 'List[boto.ec2.securitygroup.SecurityGroup]':
#     """
#     If they do not already exist, create all the security groups needed for a
#     Flintrock cluster.
#     """
#     SecurityGroupRule = namedtuple(
#         'SecurityGroupRule', [
#             'ip_protocol',
#             'from_port',
#             'to_port',
#             'src_group',
#             'cidr_ip'])
#     # TODO: Make these into methods, since we need this logic (though simple)
#     #       in multiple places. (?)
#     flintrock_group_name = 'flintrock'
#     cluster_group_name = 'flintrock-' + cluster_name

#     search_results = connection.get_all_security_groups(
#         filters={
#             'group-name': [flintrock_group_name, cluster_group_name]
#         })
#     flintrock_group = next((sg for sg in search_results if sg.name == flintrock_group_name), None)
#     cluster_group = next((sg for sg in search_results if sg.name == cluster_group_name), None)

#     if not flintrock_group:
#         flintrock_group = connection.create_security_group(
#             name=flintrock_group_name,
#             description="flintrock base group",
#             vpc_id=vpc_id)

#     # Rules for the client interacting with the cluster.
#     flintrock_client_ip = (urllib.request.urlopen('http://checkip.amazonaws.com/').read().decode('utf-8').strip())
#     flintrock_client_cidr = '{ip}/32'.format(ip=flintrock_client_ip)

#     client_rules = [
#         SecurityGroupRule(
#             ip_protocol='tcp',
#             from_port=22,
#             to_port=22,
#             cidr_ip=flintrock_client_cidr,
#             src_group=None),
#         SecurityGroupRule(
#             ip_protocol='tcp',
#             from_port=8080,
#             to_port=8081,
#             cidr_ip=flintrock_client_cidr,
#             src_group=None),
#         SecurityGroupRule(
#             ip_protocol='tcp',
#             from_port=4040,
#             to_port=4040,
#             cidr_ip=flintrock_client_cidr,
#             src_group=None)
#     ]

#     # TODO: Don't try adding rules that already exist.
#     # TODO: Add rules in one shot.
#     for rule in client_rules:
#         try:
#             flintrock_group.authorize(**vars(rule))
#         except boto.exception.EC2ResponseError as e:
#             if e.error_code != 'InvalidPermission.Duplicate':
#                 print("Error adding rule: {r}".format(r=rule))
#                 raise

#     # Rules for internal cluster communication.
#     if not cluster_group:
#         cluster_group = connection.create_security_group(
#             name=cluster_group_name,
#             description="Flintrock cluster group",
#             vpc_id=vpc_id)

#     cluster_rules = [
#         SecurityGroupRule(
#             ip_protocol='icmp',
#             from_port=-1,
#             to_port=-1,
#             src_group=cluster_group,
#             cidr_ip=None),
#         SecurityGroupRule(
#             ip_protocol='tcp',
#             from_port=0,
#             to_port=65535,
#             src_group=cluster_group,
#             cidr_ip=None),
#         SecurityGroupRule(
#             ip_protocol='udp',
#             from_port=0,
#             to_port=65535,
#             src_group=cluster_group,
#         cidr_ip=None)
#     ]

#     # TODO: Don't try adding rules that already exist.
#     # TODO: Add rules in one shot.
#     for rule in cluster_rules:
#         try:
#             cluster_group.authorize(**vars(rule))
#         except boto.exception.EC2ResponseError as e:
#             if e.error_code != 'InvalidPermission.Duplicate':
#                 print("Error adding rule: {r}".format(r=rule))
#                 raise

#     return [flintrock_group, cluster_group]


    
def setup_network(conn, cluster_name, opts):
    print('[ Setting up Network & Firewall Entries ]')
    cmds = []

    master_group = get_or_make_group(conn, cluster_name + "-master", opts.vpc_id)
    slave_group = get_or_make_group(conn, cluster_name + "-slaves", opts.vpc_id)
    authorized_address = opts.authorized_address
    if master_group.rules == []:  # Group was just now created
        if opts.vpc_id is None:
            master_group.authorize(src_group=master_group)
            master_group.authorize(src_group=slave_group)
        else:
            master_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=master_group)
            master_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=master_group)
            master_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=master_group)
            master_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                   src_group=slave_group)
            master_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                   src_group=slave_group)
            master_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                   src_group=slave_group)
        master_group.authorize('tcp', 22, 22, authorized_address)
        master_group.authorize('tcp', 8080, 8081, authorized_address)
        master_group.authorize('tcp', 18080, 18080, authorized_address)
        master_group.authorize('tcp', 19999, 19999, authorized_address)
        master_group.authorize('tcp', 50030, 50030, authorized_address)
        master_group.authorize('tcp', 50070, 50070, authorized_address)
        master_group.authorize('tcp', 60070, 60070, authorized_address)
        master_group.authorize('tcp', 4040, 4045, authorized_address)
        # Rstudio (GUI for R) needs port 8787 for web access
        master_group.authorize('tcp', 8787, 8787, authorized_address)
        # HDFS NFS gateway requires 111,2049,4242 for tcp & udp
        master_group.authorize('tcp', 111, 111, authorized_address)
        master_group.authorize('udp', 111, 111, authorized_address)
        master_group.authorize('tcp', 2049, 2049, authorized_address)
        master_group.authorize('udp', 2049, 2049, authorized_address)
        master_group.authorize('tcp', 4242, 4242, authorized_address)
        master_group.authorize('udp', 4242, 4242, authorized_address)
        # RM in YARN mode uses 8088
        master_group.authorize('tcp', 8088, 8088, authorized_address)
        master_group.authorize('tcp', 5080, 5080, authorized_address)
    if slave_group.rules == []:  # Group was just now created
        if opts.vpc_id is None:
            slave_group.authorize(src_group=master_group)
            slave_group.authorize(src_group=slave_group)
        else:
            slave_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                  src_group=master_group)
            slave_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                  src_group=master_group)
            slave_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                  src_group=master_group)
            slave_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1,
                                  src_group=slave_group)
            slave_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535,
                                  src_group=slave_group)
            slave_group.authorize(ip_protocol='udp', from_port=0, to_port=65535,
                                  src_group=slave_group)
        slave_group.authorize('tcp', 22, 22, authorized_address)
        slave_group.authorize('tcp', 8080, 8081, authorized_address)
        slave_group.authorize('tcp', 50060, 50060, authorized_address)
        slave_group.authorize('tcp', 50075, 50075, authorized_address)
        slave_group.authorize('tcp', 60060, 60060, authorized_address)
        slave_group.authorize('tcp', 60075, 60075, authorized_address)

    return (master_group, slave_group)

        
# -------------------------------------------------------------------------------------
#                      CLUSTER LAUNCH, DESTROY, START, STOP
# -------------------------------------------------------------------------------------

def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
          c=cluster_name, r=opts.region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    master_instances = get_instances([cluster_name + "-master"])
    slave_instances = get_instances([cluster_name + "-slaves"])

    if any((master_instances, slave_instances)):
        print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
              m=len(master_instances),
              plural_m=('' if len(master_instances) == 1 else 's'),
              s=len(slave_instances),
              plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
              c=cluster_name, r=opts.region), file=sys.stderr)
        sys.exit(1)

    return (master_instances, slave_instances)


def launch_nodes(conn, cluster_name, master_group, slave_group, opts):
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    user_data_content = None
    if opts.user_data:
        with open(opts.user_data) as user_data_file:
            user_data_content = user_data_file.read()
        
    # Check if instances are already running in our groups
    existing_masters, existing_slaves = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
    if existing_slaves or (existing_masters and not opts.use_existing_master):
        print("ERROR: There are already instances running in group %s or %s" %
              (master_group.name, slave_group.name), file=stderr)
        sys.exit(1)
    
    # Use group ids to work around https://github.com/boto/boto/issues/350
    additional_group_ids = []
    if opts.additional_security_group:
        additional_group_ids = [sg.id
                                for sg in conn.get_all_security_groups()
                                if opts.additional_security_group in (sg.name, sg.id)]

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print("Could not find AMI " + opts.ami, file=stderr)
        sys.exit(1)

    # Create block device mapping so that we can add EBS volumes if asked to.
    # The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
    from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
    block_map = BlockDeviceMapping()
    if opts.ebs_vol_size > 0:
        for i in range(opts.ebs_vol_num):
            device = EBSBlockDeviceType()
            device.size = opts.ebs_vol_size
            device.volume_type = opts.ebs_vol_type
            device.delete_on_termination = True
            block_map["/dev/sd" + chr(ord('s') + i)] = device

    # AWS ignores the AMI-specified block device mapping for M3 (see SPARK-3342).
    if opts.slave_instance_type.startswith('m3.'):
        for i in range(get_num_disks(opts.slave_instance_type)):
            dev = BlockDeviceType()
            dev.ephemeral_name = 'ephemeral%d' % i
            # The first ephemeral drive is /dev/sdb.
            name = '/dev/sd' + string.letters[i + 1]
            block_map[name] = dev

    # Launch slaves
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d slaves as spot instances with price $%.3f" %
              (opts.slaves, opts.spot_price))
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        my_req_ids = []
        for zone in zones:
            num_slaves_this_zone = get_partition(opts.slaves, num_zones, i)
            slave_reqs = conn.request_spot_instances(
                price=opts.spot_price,
                image_id=opts.ami,
                launch_group="launch-group-%s" % cluster_name,
                placement=zone,
                count=num_slaves_this_zone,
                key_name=opts.key_pair,
                security_group_ids=[slave_group.id] + additional_group_ids,
                instance_type=opts.slave_instance_type,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_profile_name=opts.slave_instance_profile_name)
            my_req_ids += [req.id for req in slave_reqs]
            i += 1

        print("Waiting for spot instances to be granted...")
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == opts.slaves:
                    print("All %d slaves granted" % opts.slaves)
                    reservations = conn.get_all_reservations(active_instance_ids)
                    slave_nodes = []
                    for r in reservations:
                        slave_nodes += r.instances
                    break
                else:
                    print("%d of %d slaves granted, waiting longer" % (
                        len(active_instance_ids), opts.slaves))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            running = len(master_nodes) + len(slave_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        # Launch non-spot instances
        zones = get_zones(conn, opts)
        num_zones = len(zones)
        i = 0
        slave_nodes = []
        for zone in zones:
            num_slaves_this_zone = get_partition(opts.slaves, num_zones, i)
            if num_slaves_this_zone > 0:
                slave_res = image.run(
                    key_name=opts.key_pair,
                    security_group_ids=[slave_group.id] + additional_group_ids,
                    instance_type=opts.slave_instance_type,
                    placement=zone,
                    min_count=num_slaves_this_zone,
                    max_count=num_slaves_this_zone,
                    block_device_map=block_map,
                    subnet_id=opts.subnet_id,
                    placement_group=opts.placement_group,
                    user_data=user_data_content,
                    instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                    instance_profile_name=opts.instance_profile_name)
                slave_nodes += slave_res.instances
                print("Launched {s} slave{plural_s} in {z}, regid = {r}".format(
                      s=num_slaves_this_zone,
                      plural_s=('' if num_slaves_this_zone == 1 else 's'),
                      z=zone,
                      r=slave_res.id))
            i += 1

    # Launch or resume masters
    if existing_masters:
        print("Starting master...")
        for inst in existing_masters:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        master_nodes = existing_masters
    else:
        master_type = opts.master_instance_type
        if master_type == "":
            master_type = opts.slave_instance_type
        if opts.zone == 'all':
            opts.zone = random.choice(conn.get_all_zones()).name
        master_res = image.run(
            key_name=opts.key_pair,
            security_group_ids=[master_group.id] + additional_group_ids,
            instance_type=master_type,
            placement=opts.zone,
            min_count=1,
            max_count=1,
            block_device_map=block_map,
            subnet_id=opts.subnet_id,
            placement_group=opts.placement_group,
            user_data=user_data_content,
            instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
            instance_profile_name=opts.instance_profile_name)
        
        print("Launched master in %s, regid = %s" % (zone, master_res.id))


    # This wait time corresponds to SPARK-4983
    print("Waiting for AWS to propagate instance metadata...")
    time.sleep(10)

    # Give the instances descriptive names and set additional tags
    additional_tags = {}
    if opts.additional_tags.strip():
        additional_tags = dict(
            map(str.strip, tag.split(':', 1)) for tag in opts.additional_tags.split(',')
        )

    for master in master_nodes:
        master.add_tags(
            dict(additional_tags, Name='{cn}-master-{iid}'.format(cn=cluster_name, iid=master.id))
        )

    for slave in slave_nodes:
        slave.add_tags(
            dict(additional_tags, Name='{cn}-slave-{iid}'.format(cn=cluster_name, iid=slave.id))
        )

    # Return all the instances
    return (master_nodes, slave_nodes)
        

# def launch_ec2(connection, cluster_name, opts, security_groups):
#     try:

#         try:
#             image = connection.get_all_images(image_ids=[opts.ami])[0]
#         except:
#             print("Could not find AMI " + opts.ami, file=stderr)
#             sys.exit(1)

#         # Create block device mapping so that we can add EBS volumes if asked to.
#         # The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
#         from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
#         block_map = BlockDeviceMapping()
#         if opts.ebs_vol_size > 0:
#             for i in range(opts.ebs_vol_num):
#                 device = EBSBlockDeviceType()
#                 device.size = opts.ebs_vol_size
#                 device.volume_type = opts.ebs_vol_type
#                 device.delete_on_termination = True
#                 block_map["/dev/sd" + chr(ord('s') + i)] = device

#         # AWS ignores the AMI-specified block device mapping for M3 (see SPARK-3342).
#         if opts.slave_instance_type.startswith('m3.'):
#             for i in range(get_num_disks(opts.slave_instance_type)):
#                 dev = BlockDeviceType()
#                 dev.ephemeral_name = 'ephemeral%d' % i
#                 # The first ephemeral drive is /dev/sdb.
#                 name = '/dev/sd' + string.letters[i + 1]
#                 block_map[name] = dev

#         reservation = connection.run_instances(
#             image_id=opts.ami,
#             min_count=(opts.slaves + 1),
#             max_count=(opts.slaves + 1),
#             block_device_map=block_map,
#             key_name=opts.key_pair,
#             instance_type=opts.master_instance_type,
#             placement=opts.zone,
#             security_group_ids=[sg.id for sg in security_groups],
#             subnet_id=opts.subnet_id,
#             placement_group=opts.placement_group,
#             tenancy=tenancy,
#             ebs_optimized=ebs_optimized,
#             instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
#             instance_profile_name=opts.instance_profile_name)
            

#         time.sleep(10)  # AWS metadata eventual consistency tax.

#         while True:
#             for instance in reservation.instances:
#                 if instance.state == 'running':
#                     continue
#                 else:
#                     instance.update()
#                     time.sleep(3)
#                     break
#             else:
#                 print("All {c} instances now running.".format(
#                     c=len(reservation.instances)))
#                 break

#         master_instance = reservation.instances[0]
#         slave_instances = reservation.instances[1:]

#         connection.create_tags(
#             resource_ids=[master_instance.id],
#             tags={
#                 'flintrock-role': 'master',
#                 'Name': '{c}-master'.format(c=cluster_name)})
#         connection.create_tags(
#             resource_ids=[i.id for i in slave_instances],
#             tags={
#                 'flintrock-role': 'slave',
#                 'Name': '{c}-slave'.format(c=cluster_name)})

#         cluster_info = ClusterInfo(
#             name=cluster_name,
#             ssh_key_pair=generate_ssh_key_pair(),
#             master_host=master_instance.public_dns_name,
#             slave_hosts=[instance.public_dns_name for instance in slave_instances],
#             spark_scratch_dir='/mnt/spark',
#             spark_master_opts="")

#         # TODO: Abstract away. No-one wants to see this async shite here.
#         loop = asyncio.get_event_loop()

#         tasks = []
#         for instance in reservation.instances:
#             task = loop.run_in_executor(
#                 executor=None,
#                 callback=functools.partial(
#                     provision_ec2_node,
#                     modules=modules,
#                     host=instance.ip_address,
#                     identity_file=identity_file,
#                     cluster_info=cluster_info))
#             tasks.append(task)
#         loop.run_until_complete(asyncio.wait(tasks))
#         loop.close()

#         print("All {c} instances provisioned.".format(
#             c=len(reservation.instances)))

#         # --- This stuff here runs after all the nodes are provisioned. ---
#         with paramiko.client.SSHClient() as client:
#             client.load_system_host_keys()
#             client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

#             client.connect(
#                 username="ec2-user",
#                 hostname=master_instance.public_dns_name,
#                 key_filename=identity_file,
#                 timeout=3)

#             for module in modules:
#                 module.configure_master(
#                     ssh_client=client,
#                     cluster_info=cluster_info)

#             # Login to the master for manual inspection.
#             # TODO: Move to master_login() method.
#             # ret = subprocess.call(
#             #     """
#             #     set -x
#             #     ssh -o "StrictHostKeyChecking=no" \
#             #         -i {identity_file} \
#             #         ec2-user@{host}
#             #     """.format(
#             #         identity_file=shlex.quote(identity_file),
#             #         host=shlex.quote(master_instance.public_dns_name)),
#             #     shell=True)

#     except KeyboardInterrupt as e:
#         print("Exiting...")
#         sys.exit(1)
    


def launch_cluster(cluster_name, opts):
    """
    Create a new cluster. 
    """

    print('[ Launching cluster: %s ]' % cluster_name)
    conn = open_ec2_connection(opts)

    # Set up the network
    #security_groups = get_or_create_security_groups(cluster_name=opts.cluster_name, vpc_id=vpc_id)
    (master_group, slave_group) = setup_network(conn, cluster_name, opts)

    # Check if instances are already running in our groups
    print('[ Launching nodes ]')
    #launch_ec2(conn, cluster_name, opts, security_groups)
    launch_nodes(conn, cluster_name, master_group, slave_group, opts)
    return
 
    # Wait some time for machines to bootup. We consider the cluster ready when
    # all hosts have been assigned an IP address.
    print('[ Waiting for cluster to enter into SSH-ready state ]')
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

    print("\n\n=======================================================")
    print("              Cluster \"%s\" is running" % (cluster_name))
    print("")
    print("   Spark UI: http://" + master_node['external_ip'] + ":8080")
    print("   Ganglia : http://" + master_node['external_ip'] + ":5080/ganglia")
    print("========================================================\n")

    
def destroy_cluster(cluster_name, opts):
    """
    Delete a cluster permanently.  All state will be lost.
    """
    print('[ Destroying cluster: %s ]'  % (cluster_name))

    conn = open_ec2_connection(opts)
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
    if any(master_nodes + slave_nodes):
        print("The following instances will be terminated:")
        for inst in master_nodes + slave_nodes:
            print("> %s" % get_dns_name(inst, opts.private_ips))
        print("ALL DATA ON ALL NODES WILL BE LOST!!")

    msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
    response = input(msg)

    if response == "y":
        print("Terminating master...")
        for inst in master_nodes:
            inst.terminate()
            print("Terminating slaves...")
        for inst in slave_nodes:
            inst.terminate()

        # Delete security groups as well
        if opts.delete_groups:
            group_names = [cluster_name + "-master", cluster_name + "-slaves"]
            wait_for_cluster_state(
                conn=conn,
                opts=opts,
                cluster_instances=(master_nodes + slave_nodes),
                cluster_state='terminated'
            )
            print("Deleting security groups (this will take some time)...")
            attempt = 1
            while attempt <= 3:
                print("Attempt %d" % attempt)
                groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
                success = True
                # Delete individual rules in all groups before deleting groups to
                # remove dependencies between them
                for group in groups:
                    print("Deleting rules in security group " + group.name)
                    for rule in group.rules:
                        for grant in rule.grants:
                            success &= group.revoke(ip_protocol=rule.ip_protocol,
                                                    from_port=rule.from_port,
                                                    to_port=rule.to_port,
                                                    src_group=grant)

                # Sleep for AWS eventual-consistency to catch up, and for instances
                # to terminate
                time.sleep(30)  # Yes, it does have to be this long :-(
                for group in groups:
                    try:
                        # It is needed to use group_id to make it work with VPC
                        conn.delete_security_group(group_id=group.id)
                        print("Deleted security group %s" % group.name)
                    except boto.exception.EC2ResponseError:
                        success = False
                        print("Failed to delete security group %s" % group.name)

                # Unfortunately, group.revoke() returns True even if a rule was not
                # deleted, so this needs to be rerun if something fails
                if success:
                    break

                attempt += 1

            if not success:
                print("Failed to delete all security groups after 3 tries.")
                print("Try re-running in a few minutes.")
            

    else:
        print("\nExiting without deleting cluster %s." % (cluster_name))
        sys.exit(0)

        
def stop_cluster(cluster_name, opts, slaves_only = False):
    """
    Stop a running cluster. The cluster can later be restarted with the 'start'
    command. All the data on the root drives of these instances will
    persist across reboots, but any data on attached drives will be lost.
    """
    
    print('[ Stopping cluster: %s ]' % cluster_name)

    # Get cluster machines
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
    if (master_node is None) and (len(slave_nodes) == 0):
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
        sys.exit(1)

    cmds = []

    if slaves_only:
        instance_list = slave_nodes
    else:
        instance_list = [master_node] + slave_nodes
        
    for instance in instance_list:
        cmds.append( COMMAND_PREFIX + ' instances stop ' + instance['host_name'] + ' --zone ' + instance['zone'] )

    # I can't decide if it is important to check with the user when stopping the cluster.  Seems
    # more convenient not to.  But I'm leaving this code in for now in case time shows that
    # it is important to include this confirmation step. -broxton
    #
    #print('Cluster %s with %d nodes will be stopped.  Data on scratch drives will be lost, but root disks will be preserved.' % (cluster_name, len(cmds)))
    #proceed = raw_input('Are you sure you want to proceed? (y/N) : ')
    #if proceed == 'y' or proceed == 'Y':

    print('[ Stopping nodes ]')
    run(cmds, parallelize = True, terminate_on_failure = False)

    # Clean up scratch disks
    cleanup_scratch_disks(cluster_name, opts, detach_first = True, slaves_only = slaves_only)

    #else:
    #   print("\nExiting without stopping cluster %s." % (cluster_name))
    #   sys.exit(0)


def start_cluster(cluster_name, opts):
    """
    Start a cluster that is in the stopped state.
    """

    print('[ Starting cluster: %s ]' % (cluster_name))

    # Get cluster machines
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
    if (master_node is None) and (len(slave_nodes) == 0):
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
        sys.exit(1)

    cmds = []
    for instance in [master_node] + slave_nodes:
        cmds.append( COMMAND_PREFIX + ' instances start ' + instance['host_name'] + ' --zone ' + instance['zone'] )

    print('[ Starting nodes ]')
    run(cmds, parallelize = True)

    # Wait some time for machines to bootup. We consider the cluster ready when
    # all hosts have been assigned an IP address.
    print('[ Waiting for cluster to enter into SSH-ready state ]')
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

    print("\n\n---------------------------------------------------------------------------")
    print("                       Cluster %s is running" % (cluster_name))
    print("")
    print(" Spark UI: http://" + master_node['external_ip'] + ":8080")
    print(" Ganglia : http://" + master_node['external_ip'] + ":5080/ganglia")
    print("\n\n---------------------------------------------------------------------------")


# -------------------------------------------------------------------------------------
#                      INSTALLATION AND CONFIGURATION HELPERS
# -------------------------------------------------------------------------------------
    
def deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes):
    print('[ Generating SSH keys on master and deploying to slave nodes ]')

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
    run(COMMAND_PREFIX + " copy-files " + cluster_name + "-master:.ssh.tgz /tmp/spark_gce_ssh.tgz --zone " + master_node['zone'])
    run(ssh_wrap(master_node, opts.identity_file, "rm -f .ssh.tgz"))

    # Upload to the slaves, and unpack the ssh keys on the slave nodes.
    run([COMMAND_PREFIX + " copy-files /tmp/spark_gce_ssh.tgz " + slave['host_name'] + ": --zone " + slave['zone'] for slave in slave_nodes], parallelize = True)
    run( [ssh_wrap(slave,
                   opts.identity_file,
                   "rm -rf ~/.ssh && tar xzf spark_gce_ssh.tgz && rm spark_gce_ssh.tgz")
          for slave in slave_nodes], parallelize = True)

    # Clean up: delete the local copy of the ssh keys
    run("rm -f /tmp/spark_gce_ssh.tgz")


def attach_local_ssd(cluster_name, opts, master_node, slave_nodes):
    print('[ Attaching 350GB NVME SSD drive to each node under /mnt ]')

    cmds = [ 'if [ ! -d /mnt ]; then sudo mkdir /mnt; fi',
             'sudo /usr/share/google/safe_format_and_mount -m "mkfs.ext4 -F" /dev/disk/by-id/google-local-ssd-0 /mnt',
             'sudo chmod a+w /mnt']

    cmds = [ ssh_wrap(slave, opts.identity_file, cmds, group = True) for slave in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

def cleanup_scratch_disks(cluster_name, opts, detach_first = True, slaves_only = False):
    # Get cluster ips
    print('[ Detaching and deleting cluster scratch disks ]')
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)

    # Detach drives
    if detach_first:
        cmds = []
        if not slaves_only:
            cmds = [ COMMAND_PREFIX + ' instances detach-disk ' + cluster_name + '-master --disk ' + cluster_name + '-m-scratch --zone ' + master_node['zone'] ]
        else: 
            cmds = [ ]
            
        for i, slave in enumerate(slave_nodes):
            cmds.append( COMMAND_PREFIX + ' instances detach-disk ' + cluster_name + '-slave' + str(slave['slave_id']) + ' --disk ' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch --zone ' + slave['zone'] )
        run(cmds, parallelize = True, terminate_on_failure = False)

    # Delete drives
    if not slaves_only:
        cmds = [ COMMAND_PREFIX + ' disks delete "' + cluster_name + '-m-scratch" --quiet --zone ' + master_node['zone'] ]
    else:
        cmds = []
        
    for i, slave in enumerate(slave_nodes):
        cmds.append( COMMAND_PREFIX + ' disks delete "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --quiet --zone ' + slave['zone'] )
    run(cmds, parallelize = True, terminate_on_failure = False)


def attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes):
    print('[ Adding new ' + opts.scratch_disk_size + ' drive of type "' + opts.scratch_disk_type + '" to each cluster node ]')

    cmds = []
    cmds.append( [COMMAND_PREFIX + ' disks create "' + cluster_name + '-m-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '" --zone ' + master_node['zone'],
                  COMMAND_PREFIX + ' instances attach-disk ' + cluster_name + '-master --device-name "' + cluster_name + '-m-scratch" --disk ' + cluster_name + '-m-scratch --zone ' + master_node['zone'],
                  ssh_wrap(master_node, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-m-scratch " + " -F < /dev/null && " + 
                           "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-scratch /mnt && " + 
                           'sudo chown "$USER":"$USER" /mnt') ] )

    for slave in slave_nodes:
        cmds.append( [ COMMAND_PREFIX + ' disks create "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --size ' + opts.scratch_disk_size + ' --type "' + opts.scratch_disk_type + '" --zone ' + slave['zone'],
                       COMMAND_PREFIX + ' instances attach-disk ' + cluster_name + '-slave' +  str(slave['slave_id']) + ' --disk ' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch --device-name "' + cluster_name + '-s' + str(slave['slave_id']) + '-scratch" --zone ' + slave['zone'],
                       ssh_wrap(slave, opts.identity_file, "sudo mkfs.ext4 /dev/disk/by-id/google-"+ cluster_name + "-s" + str(slave['slave_id']) + "-scratch " + " -F < /dev/null && " + 
                                "sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(slave['slave_id']) + "-scratch /mnt && " + 
                                'sudo chown "$USER":"$USER" /mnt' ) ] )

    run(cmds, parallelize = True)
    print('[ All volumes mounted, will be available at /mnt ]')
            
    
def initialize_cluster(cluster_name, opts, master_node, slave_nodes):
    print('[ Installing software dependencies (this will take several minutes) ]')

    # Create a user-writable /opt directory on all nodes.
    cmds = [ ssh_wrap(slave, opts.identity_file, "sudo mkdir -p /opt && sudo chown $USER.$USER /opt", group = True) for slave in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)
    
    # Install the copy-dir script
    run(COMMAND_PREFIX + " copy-files " + SPARK_EC2_PATH + "/support_files/copy-dir " + cluster_name + "-master: --zone " + master_node['zone'])
    cmds = ['mkdir -p /opt/spark/bin && mkdir -p /opt/spark/conf',
            'mv $HOME/copy-dir /opt/spark/bin',
            'chmod 755 /opt/spark/bin/copy-dir']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Create a file containing the list of all current slave nodes 
    import tempfile
    slave_file = tempfile.NamedTemporaryFile(delete = False)
    for slave in slave_nodes:
        slave_file.write(slave['host_name'] + '\n')
    slave_file.close()
    run(COMMAND_PREFIX + " copy-files " + slave_file.name + " " + cluster_name + "-master:slaves --zone " + master_node['zone'])
    run(ssh_wrap(master_node, opts.identity_file, "mv slaves /opt/spark/conf", group = True))
    run(ssh_wrap(master_node, opts.identity_file, "chmod 644 /opt/spark/conf/slaves", group = True))
    os.unlink(slave_file.name)

    # Download Anaconda, and copy to slave nodes
    cmds = [ 'wget http://storage.googleapis.com/spark-gce/packages/Anaconda-2.1.0-Linux-x86_64.sh',
             '/opt/spark/bin/copy-dir Anaconda-2.1.0-Linux-x86_64.sh']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))
        
    cmds = [ 'sudo apt-get update -q -y',

             # Install basic packages
             'sudo apt-get install -q -y screen less git mosh pssh emacs bzip2 dstat iotop strace sysstat htop g++ openjdk-7-jdk',

             # Rspark dependencies
             'sudo apt-get install -q -y R-base realpath',  # Realpath is used by R to find java installations

             # PySpark dependencies
             'rm -rf /opt/anaconda && bash Anaconda-2.1.0-Linux-x86_64.sh -b -p /opt/anaconda && rm Anaconda-2.1.0-Linux-x86_64.sh',

             # Set system path
             'sudo sh -c \"echo export PATH=/opt/anaconda/bin:\$PATH:/opt/spark/bin:/opt/ephemeral-hdfs/bin >> /etc/bash.bashrc\"'
         ]
    cmds = [ ssh_wrap(node, opts.identity_file, cmds, group = True) for node in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

def install_spark(cluster_name, opts, master_node, slave_nodes):
    print('[ Installing Spark ]')

    # Install Spark and Scala
    cmds = [ 'wget http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-1.3.0/spark-1.3.0-bin-cdh4.tgz',
             'tar xvf spark-1.3.0-bin-cdh4.tgz && rm spark-1.3.0-bin-cdh4.tgz',
             'rm -rf /opt/spark && mv $HOME/spark-1.3.0-bin-cdh4 /opt/spark',
             'wget http://downloads.typesafe.com/scala/2.11.6/scala-2.11.6.tgz',
             'tar xvzf scala-2.11.6.tgz && rm -rf scala-2.11.6.tgz',
             'rm -rf /opt/scala && mv $HOME/scala-2.11.6 /opt/scala']
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Set up the spark-env.conf and spark-defaults.conf files
    deploy_template(opts, master_node, "spark/conf/spark-env.sh")
    deploy_template(opts, master_node, "spark/conf/core-site.xml")
    deploy_template(opts, master_node, "spark/conf/spark-defaults.conf")
    deploy_template(opts, master_node, "spark/setup-auth.sh")
    cmds = ['echo export SPARK_HOME=/opt/spark >> $HOME/.bashrc',
            'echo export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64 >> $HOME/.bashrc',
            '/opt/spark/setup-auth.sh',

            # spark-env.conf
            'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" /opt/spark/conf/spark-env.sh',
            'sed -i "s/{{worker_cores}}/' + str(instance_info[opts.slave_instance_type]["num_cpus"]) + '/g" /opt/spark/conf/spark-env.sh',
            'sed -i "s/{{spark_master_memory}}/' + instance_info[opts.master_instance_type]["spark_master_memory"] + '/g" /opt/spark/conf/spark-env.sh',
            'sed -i "s/{{spark_slave_memory}}/' + instance_info[opts.slave_instance_type]["spark_slave_memory"] + '/g" /opt/spark/conf/spark-env.sh',

            # core-site.xml
            'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" /opt/spark/conf/core-site.xml',

            # spark-defaults.conf
            'sed -i "s/{{spark_master_memory}}/' + instance_info[opts.master_instance_type]["spark_master_memory"] + '/g" /opt/spark/conf/spark-defaults.conf',
            'sed -i "s/{{spark_slave_memory}}/' + instance_info[opts.slave_instance_type]["spark_slave_memory"] + '/g" /opt/spark/conf/spark-defaults.conf',

    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # (Re-)populate the file containing the list of all current slave nodes 
    import tempfile
    slave_file = tempfile.NamedTemporaryFile(delete = False)
    for slave in slave_nodes:
        slave_file.write(slave['host_name'] + '\n')
    slave_file.close()
    cmds = [ COMMAND_PREFIX + " copy-files " + slave_file.name + " " + cluster_name + "-master:/opt/spark/conf/slaves --zone " + master_node['zone']]
    run(cmds)
    os.unlink(slave_file.name)
    
    # Install the copy-dir script
    run(COMMAND_PREFIX + " copy-files " + SPARK_EC2_PATH + "/support_files/copy-dir " + cluster_name + "-master:/opt/spark/bin --zone " + master_node['zone'])
    
    # Copy spark to the slaves and create a symlink to $HOME/spark
    run(ssh_wrap(master_node, opts.identity_file, '/opt/spark/bin/copy-dir /opt/spark'))

    
def configure_and_start_spark(cluster_name, opts, master_node, slave_nodes):
    print('[ Configuring Spark ]')

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
    print('[ Starting Spark ]')
    run(ssh_wrap(master_node, opts.identity_file, '/opt/spark/sbin/start-all.sh') )

    
def configure_ganglia(cluster_name, opts, master_node, slave_nodes):
    print('[ Configuring Ganglia ]')
    
    run(ssh_wrap(master_node, opts.identity_file, "mkdir -p /opt/ganglia", group = True))

    deploy_template(opts, master_node, "ganglia/ports.conf")
    deploy_template(opts, master_node, "ganglia/ganglia.conf")
    deploy_template(opts, master_node, "ganglia/gmetad.conf")
    deploy_template(opts, master_node, "ganglia/gmond.conf")
    deploy_template(opts, master_node, "ganglia/000-default.conf")
    
    # Install gmetad and the ganglia web front-end on the master node
    cmds = [ 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -q -y ganglia-webfrontend gmetad ganglia-monitor',
             'sudo cp /opt/ganglia/ports.conf /etc/apache2/',
             'sudo cp /opt/ganglia/000-default.conf /etc/apache2/sites-enabled/',
             'sudo cp /opt/ganglia/ganglia.conf /etc/apache2/sites-enabled/'
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
    cmds = ['sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" /opt/ganglia/gmetad.conf',
            'sudo cp /opt/ganglia/gmetad.conf /etc/ganglia/',
            'sed -i -e  "s/{{master-node}}/' + cluster_name + '-master/g" /opt/ganglia/gmond.conf', 
            'sudo cp /opt/ganglia/gmond.conf /etc/ganglia/',
            'sudo service gmetad restart && sudo service ganglia-monitor restart && sudo service apache2 restart'
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Sleep for a sec
    import time
    time.sleep(2)

    # Install and configure gmond everywhere
    run(ssh_wrap(master_node, opts.identity_file, "/opt/spark/bin/copy-dir /opt/ganglia", group = True))
    cmds = [ 'sudo apt-get install -q -y ganglia-monitor gmetad',
             'sudo rm -rf /var/lib/ganglia/rrds/* && sudo rm -rf /mnt/ganglia/rrds/*',
             'sudo mkdir -p /mnt/ganglia/rrds',
             'sudo chown -R nobody:root /mnt/ganglia/rrds',
             'sudo rm -rf /var/lib/ganglia/rrds',
             'sudo ln -s /mnt/ganglia/rrds /var/lib/ganglia/rrds',

             'sudo cp /opt/ganglia/gmond.conf /etc/ganglia/',
             'sudo cp /opt/ganglia/gmetad.conf /etc/ganglia/',
             'sudo service ganglia-monitor restart']
    cmds = [ssh_wrap(node, opts.identity_file, cmds, group = True) for node in slave_nodes]
    run(cmds, parallelize = True)


def install_hadoop(cluster_name, opts, master_node, slave_nodes):

    print('[ Installing hadoop (this will take several minutes) ]')
    
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
            'rm -rf ephemeral-hdfs && mv hadoop-2.0.0-cdh4.2.0 /opt/ephemeral-hdfs',
            'rm -rf /opt/ephemeral-hdfs/etc/hadoop/',   # Use a single conf directory
            'mkdir -p /opt/ephemeral-hdfs/conf && ln -s /opt/ephemeral-hdfs/conf /opt/ephemeral-hdfs/etc/hadoop',
            'cp $HOME/hadoop-native/* /opt/ephemeral-hdfs/lib/native/',
            'rm -rf $HOME/hadoop-native',
            'cp /opt/spark/conf/slaves /opt/ephemeral-hdfs/conf/',

        # Install the Google Storage adaptor
        'cd /opt/ephemeral-hdfs/share/hadoop/hdfs/lib && rm -f gcs-connector-latest-hadoop2.jar && wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar',
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
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" /opt/ephemeral-hdfs/conf/core-site.xml',
        'sed -i "s/{{java_home}}/\/usr\/lib\/jvm\/java-1.7.0-openjdk-amd64/g" /opt/ephemeral-hdfs/conf/hadoop-env.sh',
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" /opt/ephemeral-hdfs/conf/hadoop-metrics2.properties',
        'sed -i "s/{{hdfs_data_dirs}}/\/mnt\/hadoop\/dfs\/data/g" /opt/ephemeral-hdfs/conf/hdfs-site.xml',
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" /opt/ephemeral-hdfs/conf/mapred-site.xml',
        'sed -i "s/{{active_master}}/' + cluster_name + '-master/g" /opt/ephemeral-hdfs/conf/masters',
    ]
    run(ssh_wrap(master_node, opts.identity_file, cmds, group = True))

    # Set up authentication for the Hadoop Google Storage adaptor
    deploy_template(opts, master_node, "ephemeral-hdfs/setup-auth.sh")
    deploy_template(opts, master_node, "ephemeral-hdfs/setup-slave.sh")
    
    # Copy the hadoop directory from the master node to the slave nodes
    run(ssh_wrap(master_node, opts.identity_file,'/opt/spark/bin/copy-dir /opt/ephemeral-hdfs'))

def configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes):

    print('[ Configuring hadoop ]')

    # Clean up old deployment
    cmds = [ ssh_wrap(node, opts.identity_file,'rm -rf /mnt/ephemeral-hdfs') for node in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

    # Set up the ephemeral HDFS directories
    cmds = [ ssh_wrap(node, opts.identity_file,'/opt/ephemeral-hdfs/setup-auth.sh') for node in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

    cmds = [ ssh_wrap(node, opts.identity_file,'/opt/ephemeral-hdfs/setup-slave.sh') for node in [master_node] + slave_nodes ]
    run(cmds, parallelize = True)

    # Format the ephemeral HDFS
    run(ssh_wrap(master_node, opts.identity_file,'/opt/ephemeral-hdfs/bin/hadoop namenode -format'))

    # Start Hadoop HDFS
    run(ssh_wrap(master_node, opts.identity_file,'/opt/ephemeral-hdfs/sbin/start-dfs.sh'))

    
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
        "-t", "--slave-instance-type", default="g2.8xlarge",
        help="Type of instance to launch (default: g2.8xlarge).")
    parser.add_option(
        "-m", "--master-instance-type", default="g2.8xlarge",
        help="Master instance type (default: g2.8xlarge)")
    parser.add_option(
        "-a", "--ami", default="ami-8ba3d3ee",
        help="Amazon Machine Image ID to use")
    
    # This option is not yet fully implemented -broxton
    #
    #    parser.add_argument("--preemptible",
    #                        action="store_true", dest="preemptible", default=False,
    #                        help="Mark slave instances as preemtible, which saves as much as 70% on their cost, but limits their uptime to 24h and means that they could be terminated at any time.  See Google Cloud Computing docs for more information.")
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
        "-r", "--region", default="us-east-1",
        help="AWS region to target when launching instances ( you can omit this argument if you set a default with \'aws configure\'")
    parser.add_option(
        "-z", "--zone", default="",
        help="AWS zone to target when launching instances (default: picks one at random)")

    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=0,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="standard",
        help="EBS volume type (e.g. 'gp2', 'standard').")
    parser.add_option(
        "--ebs-vol-num", type="int", default=1,
        help="Number of EBS volumes to attach to each node as /vol[x]. " +
        "The volumes will be deleted when the instances terminate. " +
        "Only possible on EBS-backed AMIs. " +
        "EBS volumes are only attached if --ebs-vol-size > 0. " +
        "Only support up to 8 EBS volumes.")

    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch slaves as spot instances with the given " +
        "maximum price (in dollars)")
    parser.add_option(
        "--placement-group", type="string", default=None,
        help="Which placement group to try and launch " +
        "instances into. Assumes placement group is already " +
        "created.")
    
    parser.add_option(
        "--delete-groups", action="store_true", default=False,
        help="When destroying a cluster, delete the security groups that were created")
    parser.add_option(
        "-u", "--user", default="root",
        help="The SSH user you want to connect as (default: %default)")

    parser.add_option(
        "--subnet-id", default=None,
        help="VPC subnet to launch instances in")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to launch instances in")
    parser.add_option(
        "--private-ips", action="store_true", default=False,
        help="Use private IPs for instances rather than public if VPC/subnet " +
        "requires that.")
    parser.add_option(
        "--user-data", type="string", default="",
        help="Path to a user-data file (most AMIs interpret this as an initialization script)")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--additional-security-group", type="string", default="",
        help="Additional security group to place the machines in")
    parser.add_option(
        "--additional-tags", type="string", default="",
        help="Additional tags to set on the machines; tags are comma-separated, while name and " +
        "value are colon separated; ex: \"Task:MySparkProject,Env:production\"")
    parser.add_option(
        "--instance-initiated-shutdown-behavior", default="stop",
        choices=["stop", "terminate"],
        help="Whether instances should terminate when shut down or just stop")
    parser.add_option(
        "--instance-profile-name", default=None,
        help="IAM profile name to launch instances under")
    
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
        print('Error: the selected master_instance_type was not recognized, or was an instance type without enough RAM to be a Spark master node.  Select from the list of instances below:')
        print(instance_info.keys())
        sys.exit(1)

    if not opts.slave_instance_type in instance_info.keys():
        print('Error: the selected slave_instance_type was not recognized, or was an instance type without enough RAM to be a Spark slave node.  Select from the list of instances below:')
        print(instance_info.keys())
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
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
        sys.exit(1)

    cmd = 'mosh --ssh="ssh -A -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no\" ' + str(master_node['external_ip'])
    subprocess.check_call(shlex.split(cmd))

    
def ssh_cluster(cluster_name, opts):
    import subprocess

    conn = open_ec2_connection(opts)
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)

    # Check to see if the cluster exists
    if not any(master_nodes + slave_nodes):
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
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
            print("\nERROR: Could not parse arguments to \'--ssh-tunnel\'.")
            print("       Be sure you use the syntax \'local_port:remote_port\'")
            sys.exit(1)
        print ("\nSSH port forwarding requested.  Remote port " + ssh_ports[1] +
               " will be accessible at http://localhost:" + ssh_ports[0] + '\n')
        try:
            cmd = 'ssh ' + master_nodes[0].dns_name + ' --ssh-flag="-L 8890:127.0.0.1:8888"'
            subprocess.check_call(shlex.split(cmd))
        except subprocess.CalledProcessError:
            print("\nERROR: Could not establish ssh connection with port forwarding.")
            print("       Check your Internet connection and make sure that the")
            print("       ports you have requested are not already in use.")
            sys.exit(1)

    else:
        print("Open SSH connection to", master_nodes[0].dns_name)
        #cmd = 'ssh ' + opts.user + "@" + master_nodes[0].dns_name
        #subprocess.check_call(shlex.split(cmd))
        ret = subprocess.call([
            'ssh',
            '-o', 'StrictHostKeyChecking=no',
            '-i', opts.identity_file,
            'ec2-user@{h}'.format(h=master_nodes[0].dns_name)])
        

def sshfs_cluster(cluster_name, opts, optional_arg):

    if optional_arg is None:
        print("\nCommand failed.  You must specify a local mount point.")
        sys.exit(1)

    # Attempt to create the local directory if it doesn't already exist.
    os.makedirs(optional_arg)
    
    import subprocess
    (master_node, slave_nodes) = get_cluster_info(cluster_name, opts)
    if (master_node is None) and (len(slave_nodes) == 0):
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
        sys.exit(1)

        
    cmd = 'sshfs -o auto_cache,idmap=user,noappledouble,noapplexattr,ssh_command="ssh -i ' + opts.identity_file + ' -o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no" ' + str(master_node['external_ip'] + ': ' + optional_arg)
    subprocess.check_call(shlex.split(cmd))
    print('Mounted your home directory on ' + cluster_name + '-master under local directory \"' + optional_arg + '\" using SSHFS.')
        


