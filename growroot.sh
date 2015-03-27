#!/bin/bash -u
#
# This script was originally written by Mish Bruckman 
# (https://github.com/mbrukman/stackexchange-answers/tree/master/stackoverflow/24021214) 
#
# Copyright 2014 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################
#
# We're restoring a 10GB OS image onto a larger disk. That space will not be
# visible until we repartition the disk, which then also requires us to reboot
# the instance.
#
# We assume that there's a single disk at /dev/sda with a single root partition
# and we're extending it to fill the entire disk for simplicity.
#
# This script differs from fdisk.sh in that it users cloud-init and the
# "growroot" tool it includes to do this automatically, rather than using
# fdisk(8) and resize2fs(8) directly.
#
################################################################################

# The ratio between the entire disk and the first partition in blocks or space
# that needs to be exceeded for us to repartition the disk or resize the
# filesystem.
#
# We are giving it some slack in case not all blocks or sectors are in use, and
# we would like to avoid both an infinite loop with reboot, or running resize2fs
# on every reboot.
declare -r THRESHOLD="1.1"

# Args:
#   $1: numerator
#   $2: denominator
#   $3: threshold (optional; defaults to $THRESHOLD)
#
# Returns:
#   1 if (numerator / denominator > threshold)
#   0 otherwise
function ratio_over_threshold() {
  local numer="${1}"
  local denom="${2}"
  local threshold="${3:-${THRESHOLD}}"

  if `which python > /dev/null 2>&1`; then
    python -c "print(1 if (1. * ${numer} / ${denom} > ${threshold}) else 0)"
  elif `which bc > /dev/null 2>&1`; then
    echo "${numer} / ${denom} > ${threshold}" | bc -l
  else
    echo "Neither python nor bc were found; calculation infeasible." >&2
    exit 1
  fi
}

# Repartitions the disk or resizes the file system, depending on the current
# state of the partition table.
function main() {
  # This gets us the size, in blocks, of the whole disk and the first partition.
  local dev_sda="$(fdisk -s /dev/sda)"
  local dev_sda1="$(fdisk -s /dev/sda1)"

  # If the ratio between the entire disk and the first partition is over
  # ${THRESHOLD}, then we need to install a tool to automatically grow the root
  # partition.
  if [ $(ratio_over_threshold "${dev_sda}" "${dev_sda1}") -eq 1 ]; then
    echo "Installing growroot ..."
    if which apt-get > /dev/null 2>&1 ; then
      # Debian and derivatives.
      echo "Using apt-get (Debian, etc.) ..."
      apt-get -qq -y update
      apt-get -qq -y install cloud-init cloud-initramfs-growroot
    elif which yum > /dev/null 2>&1 ; then
      # RHEL and CentOS.
      echo "Using rpm/yum (RHEL, CentOS, etc.); WARNING: work-in-progress"
      rpm -Uvh --quiet "https://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm"
      yum makecache
      yum -q -y install cloud-init cloud-initramfs-tools dracut-modules-growroot
    elif which zypper > /dev/null 2>&1 ; then
      # SUSE and derivatives.
      echo "Using zypper (SUSE, etc.); WARNING: work-in-progress"
      zypper refresh
      zypper --non-interactive --quiet install cloud-init cloud-utils
    else
      echo "Not a supported OS: none of {apt-get, yum, zypper} were found." >&2
      exit 2
    fi
    echo "Done"

    # The first partition will be automatically expanded to fill the available
    # space on disk after the next reboot with no further action from us. In
    # particular, we don't need to manually call "resize2fs".
    echo "Rebooting ..."
    reboot
  fi
}

# Emulate Python's way of only executing the code if this file were invoked as
# the main one:
#
# if __name__ == '__main__':
#   ...
if [[ "$(basename $0)" != "growroot_test.sh" ]]; then
  main
fi
