#!/usr/bin/env python
# -*- coding=utf-8 -*-

import sys
import uuid
import os

reload(sys)
sys.setdefaultencoding("utf8")
import logging
import subprocess

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)d %(levelname)s - fun:%(funcName)s - %(message)s',
    datefmt='%Y/%m/%d %H:%M:%S'
)

log = logging.getLogger(__name__)

HDFS_CMD = "hdfs dfs "

NATIVE_LIB_TEXT = 'Unable to load native-hadoop'


def _prefix_path(dir):
    if not dir:
        dir = '/'
    elif not dir.startswith('/') and not dir.startswith("hdfs:///"):
        dir = '/' + dir
    return dir


def run_shell(cmd, print_cmd=False):
    if print_cmd:
        log.info("=== " + cmd)
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdoutdata, stderrdata = popen.communicate()
    if stderrdata and NATIVE_LIB_TEXT not in stderrdata:
        logging.error(stderrdata)
    return popen.returncode, stdoutdata


def list_dir(dir=None):
    dir = _prefix_path(dir)
    status, output = run_shell(HDFS_CMD + "-ls " + dir)
    if status == 0 and output:
        return [line.rsplit(None, 1)[-1] for line in output.split('\n') if line and NATIVE_LIB_TEXT not in line and "Found" not in line]
    return []


def is_dir_exist(dir):
    dir = _prefix_path(dir)
    status, output = run_shell(HDFS_CMD + "-ls " + dir)
    for line in output.split('\n'):
        if "No such file or directory" in line:
            return False
    return True


def write_to_file(path, content):
    if path and content and isinstance(content, basestring):
        path = _prefix_path(path)
        dir_path = get_parent_dir(path)
        dir_path = _prefix_path(dir_path)
        status, output = run_shell(HDFS_CMD + "-mkdir -p " + dir_path)
        if status == 0:
            temp_file_name = str(uuid.uuid1())
            if not os.path.exists('tmp'):
                os.makedirs('tmp')
            with open('tmp/' + temp_file_name, 'w') as tmp_file:
                tmp_file.write(content)
            status, output = run_shell(HDFS_CMD + "-put tmp/" + temp_file_name + " " + path)
            if status == 0:
                log.info("=== Result file [%s] written to HDFS" % path)
            else:
                log.warn("=== Failed to write result file: %s" % path)
                logging.warn(output)
            os.remove('tmp/' + temp_file_name)
            os.rmdir('tmp')
            return
        _delete_empty_path(dir_path)
        log.info("=== Failed to write result file: %s" % path)


def _delete_empty_path(path):
    if path:
        path = _prefix_path(path)
        status, output = run_shell(HDFS_CMD + "-count " + path)
        if status == 0:
            file_count = int([line.rsplit(None, 2)[-2] for line in output.split('\n') if NATIVE_LIB_TEXT not in line][0])
            if file_count == 0:
                status, output = run_shell(HDFS_CMD + "-rm -r " + path)
                if status == 0:
                    _delete_empty_path(get_parent_dir(path))


def get_parent_dir(path):
    slash_index = path.rfind('/')
    dir_path = path[:slash_index]
    file_name = path[slash_index + 1:]
    return dir_path


if __name__ == "__main__":
    list_dir()
    # write_to_file('aaa/sss/ddd.txt', "asdasd")
