#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

"""IO cluster services module."""

import glob
import logging
import os
import shutil

from config import CORIO_CFG, CLUSTER_CFG
from src.commons.constants import CMD_MOUNT
from src.commons.utils import support_bundle_utils as sb
from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)
NODES = CLUSTER_CFG["nodes"]


def mount_nfs_server(host_dir: str, mnt_dir: str) -> bool:
    """
    Mount nfs server on mount directory.

    :param: host_dir: Link of NFS server with path.
    :param: mnt_dir: Path of directory to be mounted.
    """
    try:
        if not os.path.exists(mnt_dir):
            os.makedirs(mnt_dir)
            LOGGER.debug("Created directory: %s", mnt_dir)
        if host_dir:
            if not os.path.ismount(mnt_dir):
                resp = os.system(CMD_MOUNT.format(host_dir, mnt_dir))
                if resp:
                    raise IOError(f"Failed to mount server: {host_dir} on {mnt_dir}")
                LOGGER.debug("NFS Server: %s, mount on %s successfully.", host_dir, mnt_dir)
            else:
                LOGGER.debug("NFS Server already mounted.")
            return os.path.ismount(mnt_dir)
        LOGGER.debug("NFS Server not provided, Storing logs locally at %s", mnt_dir)
        return os.path.isdir(mnt_dir)
    except OSError as error:
        LOGGER.error(error)
        return False


def collect_support_bundle():
    """Collect support bundles from various components using support bundle cmd."""
    try:
        bundle_dir = os.path.join(os.getcwd(), "support_bundle")
        if os.path.exists(bundle_dir):
            LOGGER.debug("Removing existing directory %s", bundle_dir)
            shutil.rmtree(bundle_dir)
        os.mkdir(bundle_dir)
        if CLUSTER_CFG["product_family"] == "LC":
            status, bundle_fpath = sb.collect_support_bundle_k8s(local_dir_path=bundle_dir)
            if not status:
                raise IOError(f"Failed to generated SB. Response:{bundle_fpath}")
        else:
            raise Exception(
                f"Support bundle collection unsupported for {CLUSTER_CFG['product_family']}")
    except OSError as error:
        LOGGER.error("An error occurred in collect_support_bundle: %s", error)
        return False, error

    return os.path.exists(bundle_fpath), bundle_fpath


def rotate_logs(dpath: str, max_count: int = 0):
    """
    Remove old logs based on creation time and keep as per max log count, default is 5.

    :param: dpath: Directory path of log files.
    :param: max_count: Maximum count of log files to keep.
    """
    max_count = max_count if max_count else CORIO_CFG.get("max_sb", 5)
    if not os.path.exists(dpath):
        raise IOError(f"Directory '{dpath}' path does not exists.")
    files = sorted(glob.glob(dpath + '/**'), key=os.path.getctime, reverse=True)
    LOGGER.debug(files)
    if len(files) > max_count:
        for fpath in files[max_count:]:
            if os.path.exists(fpath):
                if os.path.isfile(fpath):
                    os.remove(fpath)
                    LOGGER.debug("Removed: Old log file: %s", fpath)
                if os.path.isdir(fpath):
                    shutil.rmtree(fpath)
                    LOGGER.debug("Removed: Old log directory: %s", fpath)

    if len(os.listdir(dpath)) > max_count:
        raise IOError(f"Failed to rotate SB logs: {os.listdir(dpath)}")


def collect_upload_sb_to_nfs_server(mount_path: str, run_id: str, max_sb: int = 0):
    """
    Collect SB and copy to NFS server log and keep SB logs as per max_sb count.

    :param mount_path: Path of mounted directory.
    :param run_id: Unique id for each run.
    :param max_sb: maximum sb count to keep on nfs server.
    """
    try:
        sb_files = []
        sb_dir = os.path.join(mount_path, "CorIO-Execution", str(run_id), "Support_Bundles")
        if not os.path.exists(sb_dir):
            os.makedirs(sb_dir)
        status, fpath = collect_support_bundle()
        if status:
            shutil.copy2(fpath, sb_dir)
            LOGGER.info("Support bundle path: %s", os.path.join(sb_dir, os.path.basename(fpath)))
            rotate_logs(sb_dir, max_sb)
            sb_files = os.listdir(sb_dir)
            LOGGER.debug("SB list: %s", sb_files)
    except IOError as error:
        LOGGER.error(error)
        return False, error

    return status, sb_files
