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
# See the License for the specific language governing permissions and
# limitations under the License.

# !/usr/bin/env python2.7
''' topology.py '''
import tempfile

import heron.tools.cli.src.python.execute as execute
import heron.tools.cli.src.python.jars as jars
import heron.tools.cli.src.python.opts as opts
import heron.tools.common.src.python.utils.config as config

def generate_definition(topology_file, topology_class, jvm_properties, args, initial_state):
  tmp_dir = tempfile.mkdtemp()

  # set the tmp dir and deactivated state in global options
  opts.set_config('cmdline.topologydefn.tmpdirectory', tmp_dir)
  opts.set_config('cmdline.topology.initial.state', initial_state)

  try:
    execute.heron_class(topology_class,
                        config.get_heron_libs(jars.topology_jars()),
                        extra_jars=[topology_file],
                        args=tuple(args),
                        java_defines=jvm_properties)
  except Exception as err:
    raise Exception("Unable to execute topology main class {0}".format(err))

  return tmp_dir
