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
import functools
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
VAPOR_PATH =  os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(VAPOR_PATH, os.path.join("support_files_ec2", "templates"))):
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
        if VERBOSE >= 1 and not ("ssh -i" in cmd) and not ("scp -i" in cmd):
            print("  CMD [local] " + cmd)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout = child.communicate()[0]
        return_code = child.returncode

        # Check for errors. Only store the output for failed commands
        if return_code != 0:
            result_queue.put( (return_code, cmd, stdout) )
        else:
            result_queue.put( (return_code, cmd, None) )
            
def run_local(cmds, parallelize = False, terminate_on_failure = True):
    """Run commands in serial or in parallel on the local machine.

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
        print(failed_stdout[0].decode("utf8").rstrip('\n'))
        print("******************************************************************************************")
        sys.exit(1)
    else:
        return num_failed
            
# def ssh_wrap(host, cmds, opts, group = False):
#     '''Given a command to run on a remote host, this function wraps the command in
#     the appropriate ssh invocation that can be run on the local host to achieve
#     the desired action on the remote host. This can then be passed to the run()
#     function to execute the command on the remote host.

#     This function can take a single command or a list of commands, and will
#     return a list of ssh-wrapped commands. However, if group = True, the list
#     of commands will be combined via the && shell operator and sent in a single
#     ssh command.
#     '''

#     if not isinstance(cmds, list):
#         cmds = [ cmds ]

#     for cmd in cmds:
#         if VERBOSE >= 1: print('  SSH [{h}]: {c}'.format(h = host.dns_name, c = cmd))

#     # Group commands using && to reduce the number of SSH commands that are
#     # executed.  This can speed things up on high latency connections.
#     if group == True:
#         cmds = [ ' && '.join(cmds) ]

#     username = opts.user
#     result = []
#     for cmd in cmds:
#         if host.ip_address is None:
#             print("Error: attempting to ssh into machine instance \"%s\" without a public IP address.  Exiting." % (host.dns_name))
#             sys.exit(1)
#         ssh_cmd = ('ssh -i {i} -o ConnectTimeout=900 -o BatchMode=yes -o ServerAliveInterval=60 ' +
#                    '-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no ' +
#                    '{u}@{host} \'{cmd}\'' ).format(i = opts.identity_file, u = opts.user, host = host.dns_name, cmd = cmd)
#         result.append(ssh_cmd)
#     return result

def scp_wrap(src_file, dst_file, opts):
    if VERBOSE >= 1: print('  SCP {src} {dst}'.format(src = src_file, dst = dst_file))

    cmd = ("scp -i {i} -o ConnectTimeout=900 -o BatchMode=yes -o ServerAliveInterval=60 " +
           "-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no " +
           "{src} {dst}").format(i = opts.identity_file, src = src_file, dst = dst_file)
    return cmd

def deploy_template(opts, node, template_name, root = "/opt"):

    if VERBOSE >= 1:
        print("  TEMPLATE: ", template_name)

    run_local(scp_wrap(VAPOR_PATH + "/support_files_ec2/templates/" + template_name,
                       "{u}@{h}:{root}/{template_name}".format(u = opts.user, h = node.dns_name,
                                                               root = root, template_name = template_name),
                       opts), parallelize = True)

# -------------------------------------------------------------------------------------

def run_ssh(host, cmds, opts, stop_on_failure = True):

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

    client = ssh_open(host, opts)
    for cmd in cmds:
        exit_status = ssh_command(client, cmd, stop_on_failure = stop_on_failure)

        # If stop_on_failure = False, and an error occured, we pass it up the
        # chain. Otherwise, an exception will already have been generated by
        # ssh_command().
        if exit_status:
            return exit_status

    # Everything ran fine.  Return exit_status = 0
    return 0
        

def ssh_command(ssh_client, cmd, stop_on_failure = True):

    host = ssh_client.get_transport().getpeername()[0]

    if VERBOSE >= 1:
        print("  SSH [{h}]: {c}".format(h=host, c=cmd))
    
    stdin, stdout, stderr = ssh_client.exec_command(cmd, get_pty=True)
    exit_status = stdout.channel.recv_exit_status()

    if exit_status and stop_on_failure:
        # TODO: Return a custom exception that includes the return code.
        # See: https://docs.python.org/3/library/subprocess.html#subprocess.check_output
        print("\n******************************************************************************************")
        print("\nSSH remote command failed:", cmd)
        print("\nCommand stdout:\n")
        print(stdout.read().decode("utf8").rstrip('\n'))
        print("\nCommand stderr:\n")
        print(stderr.read().decode("utf8").rstrip('\n'))
        print("******************************************************************************************")
        sys.exit(1)

    return exit_status
        
        

def async_execute(async_fns):

    import asyncio
    loop = asyncio.get_event_loop()
    tasks = []
    for fn in async_fns:
        task = loop.run_in_executor(executor=None, callback=fn)
        tasks.append(task)
    loop.run_until_complete(asyncio.wait(tasks))
    

def wait_for_node(node, conn, opts, retry_delay, max_retries): 
    import paramiko
    import socket
    

    nretry = 0
    while True:
        print("  Waiting for {h}".format(h=node))
        if nretry > max_retries:
            print("  [{h}] host never rensponded.".format(h=node.dns_name))
            raise SystemExit
        try:
            retval = run_ssh(node, "echo", opts)
            if retval > 0:
                time.sleep(retry_delay)
                nretry += 1
                continue
            else:
                print("  [{h}] SSH online.".format(h=node.dns_name))
                break
        except socket.timeout as e:
            time.sleep(retry_delay)
            nretry += 1
        except socket.error as e:
            time.sleep(retry_delay)
            nretry += 1
        except Exception as e:
            time.sleep(retry_delay)
            nretry += 1

def wait_for_cluster(cluster_name, opts, retry_delay = 10, max_retries = 30):
    import time

    print('[ Waiting for cluster to start up ]')

    conn = open_ec2_connection(opts)
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)

    # Wait for each node by attempting to connect with ssh, but run these checks
    # in parallel so that they are completed more quickly.
    cmds = []
    for node in master_nodes + slave_nodes:
        cmds.append(functools.partial(wait_for_node, node = node, conn = conn, opts = opts,
                                      retry_delay = retry_delay, max_retries = max_retries))
    async_execute( cmds )

    return (master_nodes[0], slave_nodes)


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
            print(output.decode('utf-8'))
        
    except OSError:
        print("%s executable not found. \n# Make sure aws command line tools are installed and authenticated\nPlease follow instructions at https://aws.amazon.com/cli/" % myexec)
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
        master_group.authorize('tcp', 7077, 7078, authorized_address)  # DELETME!!
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


def get_or_create_security_groups(cluster_name, vpc_id) -> 'List[boto.ec2.securitygroup.SecurityGroup]':
    """
    If they do not already exist, create all the security groups needed for a
    Flintrock cluster.
    """
    SecurityGroupRule = namedtuple(
        'SecurityGroupRule', [
            'ip_protocol',
            'from_port',
            'to_port',
            'src_group',
            'cidr_ip'])
    # TODO: Make these into methods, since we need this logic (though simple)
    #       in multiple places. (?)
    flintrock_group_name = 'flintrock'
    cluster_group_name = 'flintrock-' + cluster_name

    search_results = connection.get_all_security_groups(
        filters={
            'group-name': [flintrock_group_name, cluster_group_name]
        })
    flintrock_group = next((sg for sg in search_results if sg.name == flintrock_group_name), None)
    cluster_group = next((sg for sg in search_results if sg.name == cluster_group_name), None)

    if not flintrock_group:
        flintrock_group = connection.create_security_group(
            name=flintrock_group_name,
            description="flintrock base group",
            vpc_id=vpc_id)

    # Rules for the client interacting with the cluster.
    flintrock_client_ip = (
        urllib.request.urlopen('http://checkip.amazonaws.com/')
        .read().decode('utf-8').strip())
    flintrock_client_cidr = '{ip}/32'.format(ip=flintrock_client_ip)

    client_rules = [
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=22,
            to_port=22,
            cidr_ip=flintrock_client_cidr,
            src_group=None),
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=8080,
            to_port=8081,
            cidr_ip=flintrock_client_cidr,
            src_group=None),
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=4040,
            to_port=4040,
            cidr_ip=flintrock_client_cidr,
            src_group=None)
    ]
    
    # TODO: Don't try adding rules that already exist.
    # TODO: Add rules in one shot.
    for rule in client_rules:
        try:
            flintrock_group.authorize(**vars(rule))
        except boto.exception.EC2ResponseError as e:
            if e.error_code != 'InvalidPermission.Duplicate':
                print("Error adding rule: {r}".format(r=rule))
                raise

    # Rules for internal cluster communication.
    if not cluster_group:
        cluster_group = connection.create_security_group(
            name=cluster_group_name,
            description="Flintrock cluster group",
            vpc_id=vpc_id)

    cluster_rules = [
        SecurityGroupRule(
            ip_protocol='icmp',
            from_port=-1,
            to_port=-1,
            src_group=cluster_group,
            cidr_ip=None),
        SecurityGroupRule(
            ip_protocol='tcp',
            from_port=0,
            to_port=65535,
            src_group=cluster_group,
            cidr_ip=None),
        SecurityGroupRule(
            ip_protocol='udp',
            from_port=0,
            to_port=65535,
            src_group=cluster_group,
            cidr_ip=None)
    ]

    # TODO: Don't try adding rules that already exist.
    # TODO: Add rules in one shot.
    for rule in cluster_rules:
        try:
            cluster_group.authorize(**vars(rule))
        except boto.exception.EC2ResponseError as e:
            if e.error_code != 'InvalidPermission.Duplicate':
                print("Error adding rule: {r}".format(r=rule))
                raise

    return [flintrock_group, cluster_group]


# -------------------------------------------------------------------------------------
#                      CLUSTER LAUNCH, DESTROY, START, STOP
# -------------------------------------------------------------------------------------

def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """
    print("[ Searching for existing cluster {c} in region {r}...]".format( c=cluster_name, r=opts.region ))

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
        print("  Found {m} master{plural_m}, {s} slave{plural_s}.".format(
              m=len(master_instances),
              plural_m=('' if len(master_instances) == 1 else 's'),
              s=len(slave_instances),
              plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
              c=cluster_name, r=opts.region), file=sys.stderr)
        sys.exit(1)

    return (master_instances, slave_instances)


def launch_nodes(cluster_name, master_group, slave_group, opts):
    conn = open_ec2_connection(opts)

    # Launch nodes
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
                security_group_ids=[sg.id for sg in security_groups],
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
                    security_group_ids=[sg.id for sg in security_groups],
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
            security_group_ids=[sg.id for sg in security_groups],
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
        
        master_nodes = master_res.instances
        print("Launched master in %s, regid = %s" % (zone, master_res.id))


    # This wait time corresponds to SPARK-4983
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
    


def launch_cluster(cluster_name, opts):
    """
    Create a new cluster. 
    """

    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    
    print('[ Launching cluster: %s ]' % cluster_name)
    conn = open_ec2_connection(opts)

    # Set up the network
    #    (master_group, slave_group) = setup_network(conn, cluster_name, opts)

    # Check if instances are already running in our groups
    print('[ Launching nodes ]')
    launch_nodes(cluster_name, master_group, slave_group, opts)
 
    # Wait some time for machines to bootup. We consider the cluster ready when
    # all hosts have been assigned an IP address.
    (master_node, slave_nodes) = wait_for_cluster(cluster_name, opts)

    # Generate SSH keys and deploy to workers and slaves
    deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes)
 
    # Attach a new empty drive and format it
    attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes)

    # Initialize the cluster, installing important dependencies
    initialize_cluster(cluster_name, opts, master_node, slave_nodes)

    # Install, configure, and start ganglia
#    configure_ganglia(cluster_name, opts, master_node, slave_nodes)
       
    # Install and configure Hadoop
#    install_hadoop(cluster_name, opts, master_node, slave_nodes)
#    configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes)
    
    # Install, configure and start Spark
    install_spark(cluster_name, opts, master_node, slave_nodes)
    configure_and_start_spark(cluster_name, opts, master_node, slave_nodes)
    sys.exit(0)

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

    # Clean up scratch disks
    cleanup_scratch_disks(cluster_name, opts, master_nodes[0], slave_nodes)
    
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
    conn = open_ec2_connection(opts)
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
    if (len(master_nodes) == 0) or (len(slave_nodes) == 0):
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
        sys.exit(1)
    
    # I can't decide if it is important to check with the user when stopping the cluster.  Seems
    # more convenient not to.  But I'm leaving this code in for now in case time shows that
    # it is important to include this confirmation step. -broxton
    #
    #print('Cluster %s with %d nodes will be stopped.  Data on scratch drives will be lost, but root disks will be preserved.' % (cluster_name, len(cmds)))
    #proceed = raw_input('Are you sure you want to proceed? (y/N) : ')
    #if proceed == 'y' or proceed == 'Y':

    print('[ Stopping nodes ]')
    for inst in slave_nodes:
        inst.stop()

    if not slaves_only:
        for inst in master_nodes:
            inst.stop()

    # Give nodes a chance to shut down and umount disks (though this is not strictly necessary).
    time.sleep(5)
            
    # Clean up scratch disks
    cleanup_scratch_disks(cluster_name, opts, master_nodes[0], slave_nodes, detach_first = True, slaves_only = slaves_only)

    #else:
    #   print("\nExiting without stopping cluster %s." % (cluster_name))
    #   sys.exit(0)


def start_cluster(cluster_name, opts):
    """
    Start a cluster that is in the stopped state.
    """
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)


    print('[ Starting cluster: %s ]' % (cluster_name))

    # Get cluster machines
    conn = open_ec2_connection(opts)
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
    if (len(master_nodes) == 0) or (len(slave_nodes) == 0):
        print('Command failed.  Could not find a cluster named "' + cluster_name + '".')
        sys.exit(1)

    print('[ Starting nodes ]')
    for inst in master_nodes + slave_nodes:
        inst.start()

    # Wait some time for machines to bootup. We consider the cluster ready when
    # all hosts have been assigned an IP address.
    (master_node, slave_nodes) = wait_for_cluster(cluster_name, opts)

    # Generate SSH keys and deploy to workers and slaves
    deploy_ssh_keys(cluster_name, opts, master_node, slave_nodes)
            
    # Attach a new empty drive and format it
    attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes)

    # Install, configure, and start ganglia
#    configure_ganglia(cluster_name, opts, master_node, slave_nodes)

    # Configure and start Hadoop
#    configure_and_start_hadoop(cluster_name, opts, master_node, slave_nodes)

    # Configure and start Spark
    configure_and_start_spark(cluster_name, opts, master_node, slave_nodes)

    print("\n\n---------------------------------------------------------------------------")
    print("                       Cluster %s is running" % (cluster_name))
    print("")
    print(" Spark UI: http://" + master_node.dns_name + ":8080")
    print(" Ganglia : http://" + master_node.dns_name + ":5080/ganglia")
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
             "ssh-keyscan -H " + master_node.dns_name + " >> ~/.ssh/known_hosts" ]
    for slave in slave_nodes:
        cmds.append( "ssh-keyscan -H " + slave.dns_name + " >> ~/.ssh/known_hosts"  )
    cmds.append("tar czf .ssh.tgz .ssh");
    run_ssh(master_node, cmds, opts)
    
    # Copy the ssh keys locally, and Clean up the archive on the master node.
    run_local(scp_wrap("{u}@{m}:.ssh.tgz".format(u = opts.user, m = master_node.dns_name),
                       "/tmp/vapor_ssh_keys.tgz",
                       opts))
    run_ssh(master_node, "rm -f .ssh.tgz", opts)

    # Upload to the slaves, and unpack the ssh keys on the slave nodes.
    run_local( [scp_wrap("/tmp/vapor_ssh_keys.tgz",
                   "{u}@{s}:vapor_ssh_keys.tgz".format(u = opts.user, s = slave.dns_name),
                   opts) for slave in slave_nodes],
         parallelize = True )
    async_execute( [functools.partial(run_ssh,
                                      slave,
                                      "rm -rf ~/.ssh && tar xzf vapor_ssh_keys.tgz && rm vapor_ssh_keys.tgz",
                                      opts)
                    for slave in slave_nodes] )

    # Clean up: delete the local copy of the ssh keys
    run_local("rm -f /tmp/vapor_ssh_keys.tgz")



def attach_persistent_scratch_disks(cluster_name, opts, master_node, slave_nodes):
    
    print('[ Adding new ' + opts.scratch_disk_size + 'GB drive of type "' + opts.scratch_disk_type + '" to each cluster node ]')

    def attach_storage(node, opts):
        conn = open_ec2_connection(opts)
        vol = conn.create_volume(opts.scratch_disk_size, node.placement, volume_type = opts.scratch_disk_type)
        conn.create_tags([vol.id], {"Name":cluster_name + "-scratch"})

        # Wait for volume attachment
        curr_vol = conn.get_all_volumes([vol.id])[0]
        while curr_vol.status != 'available':
            time.sleep(5)
            curr_vol = conn.get_all_volumes([vol.id])[0]

        conn.attach_volume (vol.id, node.id, "/dev/sdb")

        # Wait for volume attachment
        curr_vol = conn.get_all_volumes([vol.id])[0]
        while curr_vol.status != 'in-use':
            time.sleep(5)
            curr_vol = conn.get_all_volumes([vol.id])[0]

        # Wait a sec for it to show up!
        time.sleep(5)

        if VERBOSE >= 1:
            print("  ({h}) allocating and attaching volume {v}".format(h = node.ip_address, v=vol))

        # Format and mount the disk
        cmds = ['sudo mkfs.ext4 /dev/sdb',
                'sudo mount -o "defaults,noatime,nodiratime" /dev/sdb /mnt',
                'sudo chmod o+rw /mnt']
        run_ssh(node, cmds, opts)
        
    async_execute([functools.partial(attach_storage, node, opts) for node in [master_node] + slave_nodes])
    
    print('[ All volumes mounted, will be available at /mnt ]')

    
def cleanup_scratch_disks(cluster_name, opts, master_node, slave_nodes, detach_first = True, slaves_only = False):
    print('[ Cleaning up and de-allocating scratch drives.]')

    def detach_storage(node, opts):
    
        conn = open_ec2_connection(opts)
        filters = {'tag:Name': cluster_name + '-scratch'}

        volumes = [v for v in conn.get_all_volumes(filters = filters) if v.attach_data.instance_id == node.id]

        for vol in volumes:

            if VERBOSE >= 1:
                print("  ({h}) de-allocating volume {v}".format(h = node.ip_address, v=vol))
            
            conn.detach_volume(vol.id, node.id, "/dev/sdb", force = True)
            
            # Wait for volume dettachment
            curr_vol = conn.get_all_volumes([vol.id])[0]
            while curr_vol.status != 'available':
                time.sleep(5)
                curr_vol = conn.get_all_volumes([vol.id])[0]
            
            conn.delete_volume(vol.id)

    async_execute([functools.partial(detach_storage, node, opts) for node in [master_node] + slave_nodes])

            
    
def initialize_cluster(cluster_name, opts, master_node, slave_nodes):
    print('[ Installing software dependencies (this will take several minutes) ]')

    # Create a user-writable /opt directory on all nodes.
    cmds = [ functools.partial(run_ssh,
                               node,
                               "sudo mkdir -p /opt && sudo -S chown ec2-user /opt",
                               opts) for node in [master_node] + slave_nodes ]
    async_execute(cmds)
    
    # Install the copy-dir script
    run_local(scp_wrap(VAPOR_PATH + "/support_files_ec2/copy-dir",
                       "{u}@{m}:".format(u = opts.user, m = master_node.dns_name),
                       opts))
    cmds = ['mkdir -p /opt/spark/bin && mkdir -p /opt/spark/conf',
            'mv $HOME/copy-dir /opt/spark/bin',
            'chmod 755 /opt/spark/bin/copy-dir']
    run_ssh(master_node, cmds, opts)

    # Create a file containing the list of all current slave nodes
    import tempfile
    slave_file = tempfile.NamedTemporaryFile(delete = False)
    for slave in slave_nodes:
        slave_file.write(bytes(slave.dns_name + '\n', 'UTF-8'))
    slave_file.close()
    run_local(scp_wrap(slave_file.name,
                       "{u}@{m}:slaves".format(u = opts.user, m = master_node.dns_name),
                       opts))
    run_ssh(master_node, "mv slaves /opt/spark/conf", opts)
    run_ssh(master_node, "chmod 644 /opt/spark/conf/slaves", opts)
    os.unlink(slave_file.name)

    # Download Anaconda, and copy to slave nodes
    cmds = [ 'wget http://storage.googleapis.com/spark-gce/packages/Anaconda3-2.3.0-Linux-x86_64.sh',
             '/opt/spark/bin/copy-dir Anaconda3-2.3.0-Linux-x86_64.sh']
    run_ssh(master_node, cmds, opts)

    cmds = [ 'sudo yum update -y',

             # Install basic packages
             'sudo yum install -y java-1.7.0-openjdk-devel screen less git mosh pssh emacs bzip2 dstat iotop strace sysstat htop',

             # PySpark dependencies
             'rm -rf /opt/anaconda && bash Anaconda3-2.3.0-Linux-x86_64.sh -b -p /opt/anaconda && rm Anaconda3-2.3.0-Linux-x86_64.sh',

             # Set system path
             'sh -c "echo \"export PATH=/opt/anaconda/bin:\$PATH:/opt/spark/bin:/opt/ephemeral-hdfs/bin\" >> /home/ec2-user/.bashrc"'

             # Allow sudo without a tty (needed for spark startup scripts)
             'sudo sed -i "s/requiretty/\!requiretty/g" /etc/sudoers'
    ]
    cmds = [ functools.partial(run_ssh,
                               node,
                               cmds, opts) for node in [master_node] + slave_nodes ]
    async_execute(cmds)

def install_spark(cluster_name, opts, master_node, slave_nodes):
    print('[ Installing Spark ]')

    # Install Spark and Scala
    cmds = [ 'wget http://mirror.cc.columbia.edu/pub/software/apache/spark/spark-1.5.0/spark-1.5.0-bin-cdh4.tgz',
             'tar xvf spark-1.5.0-bin-cdh4.tgz && rm spark-1.5.0-bin-cdh4.tgz',
             'rm -rf /opt/spark && mv $HOME/spark-1.5.0-bin-cdh4 /opt/spark',
             'wget http://downloads.typesafe.com/scala/2.11.6/scala-2.11.6.tgz',
             'tar xvzf scala-2.11.6.tgz && rm -rf scala-2.11.6.tgz',
             'rm -rf /opt/scala && mv $HOME/scala-2.11.6 /opt/scala']
    run_ssh(master_node, cmds, opts)
    
    # Install the copy-dir script
    run_local(scp_wrap(VAPOR_PATH + "/support_files_ec2/copy-dir",
                       "{u}@{m}:/opt/spark/bin".format(u = opts.user, m = master_node.dns_name),
                       opts))
    sys.exit(1)

    
def configure_and_start_spark(cluster_name, opts, master_node, slave_nodes):
    print('[ Configuring Spark ]')

    # Set up the spark-env.conf and spark-defaults.conf files
    deploy_template(opts, master_node, "spark/conf/spark-env.sh")
    deploy_template(opts, master_node, "spark/conf/core-site.xml")
    deploy_template(opts, master_node, "spark/conf/spark-defaults.conf")
    deploy_template(opts, master_node, "spark/setup-auth.sh")
    cmds = ['echo export SPARK_HOME=/opt/spark >> $HOME/.bashrc',
            'echo export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 >> $HOME/.bashrc',
            '/opt/spark/setup-auth.sh',

            # spark-env.conf
            'sed -i "s/{{active_master}}/' + master_node.private_dns_name + '/g" /opt/spark/conf/spark-env.sh',
            'sed -i "s/{{worker_cores}}/' + str(instance_info[opts.slave_instance_type]["num_cpus"]) + '/g" /opt/spark/conf/spark-env.sh',
            'sed -i "s/{{spark_master_memory}}/' + instance_info[opts.master_instance_type]["spark_master_memory"] + '/g" /opt/spark/conf/spark-env.sh',
            'sed -i "s/{{spark_slave_memory}}/' + instance_info[opts.slave_instance_type]["spark_slave_memory"] + '/g" /opt/spark/conf/spark-env.sh',

            # core-site.xml
            'sed -i "s/{{active_master}}/' + master_node.private_dns_name + '/g" /opt/spark/conf/core-site.xml',

            # spark-defaults.conf
            'sed -i "s/{{spark_master_memory}}/' + instance_info[opts.master_instance_type]["spark_master_memory"] + '/g" /opt/spark/conf/spark-defaults.conf',
            'sed -i "s/{{spark_slave_memory}}/' + instance_info[opts.slave_instance_type]["spark_slave_memory"] + '/g" /opt/spark/conf/spark-defaults.conf',

    ]
    run_ssh(master_node, cmds, opts)

    cmds = ['echo export SPARK_HOME=/opt/spark >> $HOME/.bashrc',
            'echo export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 >> $HOME/.bashrc']
    async_execute([functools.partial(run_ssh,node, cmds, opts) for node in slave_nodes])

    
    # (Re-)populate the file containing the list of all current slave nodes 
    import tempfile
    slave_file = tempfile.NamedTemporaryFile(delete = False)
    for slave in slave_nodes:
        slave_file.write(bytes(slave.private_dns_name + '\n', 'UTF-8'))
        slave_file.close()
        run_local(scp_wrap(slave_file.name,
                           "{u}@{m}:/opt/spark/conf/slaves".format(u = opts.user, m = master_node.dns_name),
                           opts))
        os.unlink(slave_file.name)
    
    # Create the Spark scratch directory on the local SSD
    #
    # Disable Transparent Huge Pages (THP)
    # THP can result in system thrashing (high sys usage) due to frequent defrags of memory.
    # Most systems recommends turning THP off.
    #
    # Allow memory to be over committed. Helps in pyspark where we fork
    #
    cmds = ['sudo chmod o+rw /mnt',
            'if [ ! -d /mnt/spark ]; then mkdir /mnt/spark; fi',
            'if [[ -e /sys/kernel/mm/transparent_hugepage/enabled ]]; then sudo sh -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"; fi',
            'sudo sh -c "echo 1 > /proc/sys/vm/overcommit_memory"']
    async_execute([functools.partial(run_ssh, node, cmds, opts) for node in [master_node] + slave_nodes])

    # Copy spark to the slaves
    run_ssh(master_node, '/opt/spark/bin/copy-dir /opt/spark', opts)
    
    # Start Spark
    print('[ Starting Spark ]')
    run_ssh(master_node, '/opt/spark/sbin/start-all.sh', opts)

    
def configure_ganglia(cluster_name, opts, master_node, slave_nodes):
    print('[ Configuring Ganglia ]')
    
    run_ssh(master_node, "mkdir -p /opt/ganglia", opts)

    deploy_template(opts, master_node, "ganglia/ports.conf")
    deploy_template(opts, master_node, "ganglia/ganglia.conf")
    deploy_template(opts, master_node, "ganglia/gmetad.conf")
    deploy_template(opts, master_node, "ganglia/gmond.conf")
    deploy_template(opts, master_node, "ganglia/000-default.conf")

    sys.exit(0)
    
    # Install gmetad and the ganglia web front-end on the master node
    cmds = [ 'sudo yum install -y ganglia-web ganglia-gmetad ganglia-gmond'
#             'sudo cp /opt/ganglia/ports.conf /etc/apache2/',
#             'sudo cp /opt/ganglia/000-default.conf /etc/apache2/sites-enabled/',
             'sudo cp /opt/ganglia/ganglia.conf /etc/httpd/conf.d/'
             'sudo cp /opt/ganglia/httpd.conf /etc/httpd/conf/'
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
    cmds = ['sed -i -e  "s/{{master-node}}/' + master_node.dns_name + '/g" /opt/ganglia/gmetad.conf',
            'sudo cp /opt/ganglia/gmetad.conf /etc/ganglia/',
            'sed -i -e  "s/{{master-node}}/' + master_node.dns_name + '/g" /opt/ganglia/gmond.conf', 
            'sudo cp /opt/ganglia/gmond.conf /etc/ganglia/',
            'sudo /etc/init.d/gmond restart',
            'chown -R nobody /var/lib/ganglia/rrds',
            'ln -s /usr/share/ganglia/conf/default.json /var/lib/ganglia/conf/',
            '/etc/init.d/gmetad restart'
            '/etc/init.d/httpd restart'
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
        "-i", "--identity-file", 
        help="AWS identity file use for logging into instances (*.pem)")
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
        "--scratch-disk-type", default="standard",
        help="Scratch disk type.  (e.g. 'gp2' or 'standard')")
    parser.add_option(
        "--boot-disk-size", default="50",
        help="The size of the boot disk.  Run \'gcloud compute disk-types list\' to see your options.")
    parser.add_option(
        "--scratch-disk-size", default="256",
        help="The size of the boot disk (in GB).  Run \'gcloud compute disk-types list\' to see your options.")

    parser.add_option(
        "-r", "--region", default="us-east-1",
        help="AWS region to target when launching instances ( you can omit this argument if you set a default with \'aws configure\'")
    parser.add_option(
        "-z", "--zone", default="",
        help="AWS zone to target when launching instances (default: picks one at random)")

    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=200,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="gp2",
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
        "-u", "--user", default="ec2-user",
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

    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)
    
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
            ret = subprocess.call([
                'ssh',
                '-o', 'StrictHostKeyChecking=no',
                '-i', opts.identity_file,
                '-L', '{local}:127.0.0.1:{remote}'.format(local=ssh_ports[0], remote=ssh_ports[1]),
                '{u}@{h}'.format(u=opts.user, h=master_nodes[0].dns_name)])
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
            '{u}@{h}'.format(u=opts.user, h=master_nodes[0].dns_name)])
        

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
        


