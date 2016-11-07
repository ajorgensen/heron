# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific
'''resources.py'''
import glob
import shutil
import logging

from heron.proto import topology_pb2
import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.cli.src.python.args as cli_args
import heron.tools.cli.src.python.topology as topology
import heron.tools.common.src.python.utils.config as config

from heron.common.src.python.utils.log import Log

def create_parser(subparsers):
  '''
  :param subparsers:
  :return:
  '''
  parser = subparsers.add_parser(
      'resources',
      help='Provides information about the resources required to schedule the topology',
      usage="%(prog)s [options] " + \
            "topology-file-name topology-class-name [topology-args]",
      add_help=False
  )

  cli_args.add_titles(parser)
  cli_args.add_cluster_role_env(parser)
  cli_args.add_topology_file(parser)
  cli_args.add_topology_class(parser)
  cli_args.add_system_property(parser)
  cli_args.add_config(parser)
  cli_args.add_verbose(parser)

  parser.set_defaults(subcommand='resources')
  return parser

# pylint: disable=unused-argument
def run(command, parser, cl_args, unknown_args):
  '''
  :param command:
  :param parser:
  :param cl_args:
  :param unknown_args:
  :return:
  '''
  topology_file = cl_args['topology-file-name']
  topology_class = cl_args['topology-class-name']
  jvm_properties = cl_args['topology_main_jvm_property']

  initial_state = topology_pb2.TopologyState.Name(topology_pb2.RUNNING)
  defn_dir = topology.generate_definition(topology_file, topology_class, jvm_properties,
                                          unknown_args, initial_state)

  try:
    defn_files = glob.glob(defn_dir + '/*.defn')

    if len(defn_files) == 0:
      raise Exception("No topologies found")

    for defn_file in defn_files:
      # load the topology definition from the file
      topology_defn = topology_pb2.Topology()

      handle = open(defn_file, "rb")
      topology_defn.ParseFromString(handle.read())
      handle.close()

      release_yaml_file = config.get_heron_release_file()
      new_args = [
          "--config_path", cl_args['config_path'],
          "--heron_home", config.get_heron_dir(),
          "--override_config_file", cl_args['override_config_file'],
          "--topology_defn", defn_file,
          "--release_file", release_yaml_file,
      ]

      if Log.getEffectiveLevel() == logging.DEBUG:
        new_args.append("--verbose")

      lib_jars = config.get_heron_libs(
          jars.scheduler_jars() + jars.statemgr_jars() + jars.packing_jars()
      )

      execute.heron_class('com.twitter.heron.scheduler.ResourcesMain',
                          lib_jars,
                          extra_jars=[],
                          args=new_args)
  finally:
    shutil.rmtree(defn_dir)

  return True
