# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:

# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#
# Copyright 2019, Battelle Memorial Institute.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This material was prepared as an account of work sponsored by an agency of
# the United States Government. Neither the United States Government nor the
# United States Department of Energy, nor Battelle, nor any of their
# employees, nor any jurisdiction or organization that has cooperated in the
# development of these materials, makes any warranty, express or
# implied, or assumes any legal liability or responsibility for the accuracy,
# completeness, or usefulness or any information, apparatus, product,
# software, or process disclosed, or represents that its use would not infringe
# privately owned rights. Reference herein to any specific commercial product,
# process, or service by trade name, trademark, manufacturer, or otherwise
# does not necessarily constitute or imply its endorsement, recommendation, or
# favoring by the United States Government or any agency thereof, or
# Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the
# United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by
# BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
# under Contract DE-AC05-76RL01830
# }}}
# }}}

import logging
import sys
import psutil
from os import path, walk
from collections import namedtuple

from volttron.platform.vip.agent import Agent, RPC
from volttron.platform.agent import utils
from volttron.platform.scheduling import periodic


utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = '4.0'


# TODO: Remove deprecated code.
# def sysmon_agent(config_path, **kwargs):
#     """Load the SysMon Agent default configuration and returns and instance
#     of the agent created using that configuration.
#
#     :param config_path: Path to a configuration file.
#
#     :type config_path: str
#     :returns: SysMonAgent instance
#     :rtype: SysMonAgent
#     """
#     default_config = utils.load_config(config_path)
#     return SysMonAgent(default_config, **kwargs)


class SysMonAgent(Agent):
    """Monitor utilization of system resources (CPU, memory, swap, disk, network)

    Statistics for each system resource can be queried via
    RPC and they are published periodically to configured topics.

    :param config: Configuration dict
    :type config: dict
    """

    IMPLEMENTED_METHODS = ('cpu_percent', 'cpu_times', 'cpu_times_percent', 'cpu_stats', 'cpu_freq', 'load_average',
                           'memory', 'swap', 'disk_partitions', 'disk_usage', 'disk_io','path_usage', 'path_usage_rate',
                           'network_io', 'network_connections', 'network_interface_addresses',
                           'network_interface_statistics', 'sensors_temperatures', 'sensors_fans', 'sensors_battery',
                           'boot_time', 'users'  # TODO: Should all of these be pollable? What about cpu_count?
                           )

    def __init__(self, config_path, **kwargs):
        super(SysMonAgent, self).__init__(**kwargs)
        self.publish_type = None
        self.base_topic = None
        self._scheduled = []
        self.last_path_size = None

        default_config = utils.load_config(config_path)
        self.vip.config.set_default('config', default_config)
        self.vip.config.subscribe(self.on_configure, actions=['NEW', 'UPDATE'], pattern='config')
        self.vip.config.subscribe(self.on_reconfigure, actions=['UPDATE', 'DELETE'], pattern='_runtime_config')

    def on_configure(self, config_name, action, contents):
        _log.info('Received configuration store event of type: {}. Loading configuration from config://{}'.format(
            action, config_name))

        # Stop any currently scheduled monitors.
        for sched in self._scheduled:
            sched.cancel()
        self._scheduled = []

        self.publish_type = contents.pop('publish_type', None)
        if self.publish_type:
            self.base_topic = self.publish_type + '/' + contents.pop('base_topic', 'Log/Platform')
        else:  # TODO: Deprecated configuration block. Make if block unconditional in future release:
            # BEGIN DEPRECATED CONFIGURATION BLOCK.
            self.publish_type = 'datalogger'
            self.base_topic = contents.pop('base_topic', 'datalogger/log/platform')
            _log.warning('No "publish_type" configuration found, using deprecated configuration method:'
                         'publish_type: "datalogger", base_topic: publish_type + "log/platform".'
                         'See SysMonAgent/README.md for information on new configuration format.')
            # END DEPRECATED CONFIGURATION BLOCK.

        monitors = contents.pop('monitor', {})

        # TODO: Deprecated configuration block. Remove block between 'begin' and 'end' in future release:
        # BEGIN DEPRECATED CONFIGURATION BLOCK.
        for dep in [('cpu_interval', 'cpu_check_interval'),
                           ('memory', 'memory_check_interval'),
                           ('disk_usage', 'disk_check_interval')]:
            deprecated_interval = contents.pop(dep[1], None)
            if monitors.get(dep[0]) and deprecated_interval:
                _log.warning('Ignoring deprecated configuration {}, using provided monitor["{}"]["check_interval"'
                             'See SysMonAgent/README.md for information on new configuration format.'
                             ).format(dep[1], dep[0])
            elif deprecated_interval:
                monitors[dep[0]] = {'point_name': dep[0], 'check_interval': dep[1], 'poll': True}
                _log.warning('Starting cpu_percent monitor using deprecated configuration "cpu_check_interval".'
                             ' Update configuration to use monitor["cpu_percent"]["check_interval"].'
                             'See SysMonAgent/README.md for information on new configuration format.')
        # END DEPRECATED CONFIGURATION BLOCK.

        # Start Monitors:
        action_items = []
        for method in self.IMPLEMENTED_METHODS:
            item = monitors.pop(method, None)
            if item and item['poll'] is True:
                topic = self.base_topic + item['point_name']
                interval = item['check_interval']
                params = item['params']
                action_items.append((getattr(self, method), interval, topic, params))
        if self.publish_type == 'datalogger':
            for item in action_items:
                self._periodic_pub(*item)
        else:
            # TODO: Implement record_type messages. Call each method separately (different intervals).
            pass

        for key in contents:
            _log.warning('Ignoring unrecognized configuration parameter %s', key)
        for key in monitors:
            _log.warning('Ignoring unimplemented monitor method: {}'.format(key))

    def on_reconfigure(self, config_name, action, contents):
        _log.info('Received configuration store event of type: {}. Reconfiguring from config://{}'.format(
            action, config_name))
        # TODO: Write runtime reconfiguration'
        pass

    def _configure(self, config):
        self.base_topic = config.pop('base_topic', self.base_topic)
        self.cpu_check_interval = config.pop('cpu_check_interval', self.cpu_check_interval)
        self.memory_check_interval = config.pop('memory_check_interval', self.memory_check_interval)
        self.disk_check_interval = config.pop('disk_check_interval', self.disk_check_interval)
        self.disk_path = config.pop('disk_path', self.disk_path)
        for key in config:
            _log.warning('Ignoring unrecognized configuration parameter %s', key)

    def _periodic_pub(self, func, period, topic, params):
        """Periodically call func and publish its return value"""
        def pub_wrapper():
            data = func(**params)
            self.vip.pubsub.publish(peer='pubsub', topic=topic, message=data)
        sched = self.core.schedule(periodic(period), pub_wrapper)
        self._scheduled.append(sched)

    @RPC.export
    def cpu_percent(self, per_cpu=False):
        """Return CPU usage percentage"""
        cpu_stats = psutil.cpu_percent(percpu=per_cpu)
        if per_cpu:
            return dict(enumerate(cpu_stats))
        else:
            return cpu_stats

    @RPC.export
    def cpu_times(self, per_cpu=False, sub_points=None):
        """Return percentage of time the CPU has spent in a given mode."""
        times = psutil.cpu_times_percent(percpu=per_cpu)
        times = self._format_return(times, sub_points=sub_points)
        return times

    @RPC.export
    def cpu_times_percent(self, per_cpu=False, sub_points=None, prior_interval=None):
        """Return percentage of time the CPU has spent in a given mode."""
        percentages = psutil.cpu_times_percent(interval=prior_interval, percpu=per_cpu)
        percentages = self._format_return(percentages, sub_points=sub_points)
        return percentages


    @RPC.export
    def cpu_count(self, logical=True):
        """Return the number of CPU cores if logical=True or the number of physical CPUs if logical=False"""
        return psutil.cpu_count(logical)

    @RPC.export
    def cpu_stats(self, sub_points=None):
        """Return various CPU statistics."""
        stats = psutil.cpu_stats()
        stats = self._format_return(stats, sub_points)
        return stats

    @RPC.export
    def cpu_freq(self, per_cpu=False, sub_points=None):
        freq = psutil.cpu_freq(percpu=per_cpu)
        freq = self._format_return(freq, sub_points=sub_points)
        return freq

    @RPC.export
    def memory_percent(self):
        """Return memory usage percentage"""
        _log.warning('Method "memory_percent" is deprecated. Use "memory" instead.')
        return self.memory().percent

    @RPC.export
    def memory(self, sub_points=None):
        """Return memory usage statistics"""
        virtual_memory = psutil.virtual_memory()
        return self._format_return(virtual_memory, sub_points)

    @RPC.export
    def swap(self, sub_points=None):
        """Return swap usage statistics"""
        swap_memory = psutil.swap_memory()
        if sub_points:
            return self._format_return(swap_memory, sub_points)

    @RPC.export
    def disk_partitions(self, all_partitions=False, included_partitions=None, sub_points=None):
        partitions = psutil.disk_partitions(all_partitions)
        partitions = self._format_return(partitions, sub_points=sub_points, includes=included_partitions)
        return partitions

    @RPC.export
    def disk_percent(self, disk_path='/'):
        """Return usage of disk mounted at configured path"""
        _log.warning('Method "disk_percent" is deprecated. Use "disk_usage" instead.')
        return self.disk_usage(disk_path).percent

    @RPC.export
    def disk_usage(self, disk_path='/', sub_points=None):
        """Return disk usage statistics."""
        usage = psutil.disk_usage(disk_path)
        return self._format_return(usage, sub_points)  # TODO: Should return dict like other methods. (Use _handle_multiples)

    @RPC.export
    def load_average(self, sub_points):
        """Return load averages."""
        las = namedtuple('las', ('OneMinute', 'FiveMinute', 'FifteenMinute'))
        averages = las(*psutil.getloadavg())
        return self._format_return(averages, sub_points)

    @RPC.export
    def path_usage(self, path_name):
        """Return storage used within a filesystem path."""
        try:
            path_size = sum(path.getsize(path.join(dir_path, filename)) for dir_path, dir_names, filenames in
                            walk(path_name) for filename in filenames)
            return {path_name: path_size}
        except Exception as e:
            # TODO: Can we return an exception from an RPC call? What about where this is called as a periodic?
            _log.error('Exception in path_usage: {}'.format(e))

    @RPC.export
    def path_usage_rate(self, path_name, interval):
        """Return rate of change in storage used within a filesystem path in bytes per second."""
        current_path_size = self.path_usage(path_name)
        rate_of_change = None
        if self.last_path_size is not None:
            rate_of_change = (current_path_size - self.last_path_size) / interval
        else:
            _log.error('Unable to calculate path_usage_rate. No prior value.')
        return {path_name: rate_of_change}

    @RPC.export
    def disk_io(self, per_nic=False, no_wrap=True, included_nics=None, sub_points=None):
        """Return disk input/output statistics."""
        io_stats = psutil.net_io_counters(pernic=per_nic, nowrap=no_wrap)
        # TODO: Get disk throughput from comparing sequential psutil.disk_io_counters.
        io_stats = self._format_return(io_stats, sub_points, includes=included_nics)
        return io_stats

    @RPC.export
    def network_io(self, per_disk=False, no_wrap=True, included_nics=None, sub_points=None):
        """Return network input/output statistics."""
        io_stats = psutil.disk_io_counters(perdisk=per_disk, nowrap=no_wrap)
        # TODO: Get network bandwidth from comparing sequential psutil.network_io_counters.
        io_stats = self._format_return(io_stats, sub_points, includes=included_nics)
        return io_stats

    @RPC.export
    def network_connections(self, kind='inet', sub_points=None):
        """Return system-wide socket connections"""
        connections = psutil.net_connections(kind)
        connections = self._format_return(connections, sub_points=sub_points)
        for k, v in connections.items():
            v['family'] = v['family'].name
            v['type'] = v['type'].name
            v['laddr'] = v['laddr'].ip + ':' + str(v['laddr'].port) if type(v['laddr']) is psutil._common.addr else ''
            v['raddr'] = v['raddr'].ip + ':' + str(v['raddr'].port) if type(v['raddr']) is psutil._common.addr else ''
        return connections

    @RPC.export
    def network_interface_addresses(self, included_interfaces=None, sub_points=None):
        """Return addresses associated with network interfaces."""
        addresses = psutil.net_if_addrs()
        addresses = self._format_return(addresses, sub_points, includes=included_interfaces)
        for k, v in addresses.items():
            for item in v:
                item['family'] = item['family'].name
        return addresses

    @RPC.export
    def network_interface_statistics(self, included_interfaces=None, sub_points=None):
        """Return information about each network interface."""
        stats = psutil.net_if_stats()
        stats = self._format_return(stats, sub_points, includes=included_interfaces)
        for k, v in stats.items():
            v['duplex'] = v['duplex'].name
        return stats

    @RPC.export
    def sensors_temperatures(self, fahrenheit=False, sub_points=None, included_sensors=None):
        """Return hardware temperatures."""
        temps = psutil.sensors_temperatures(fahrenheit)
        temps = self._format_return(temps, sub_points, includes=included_sensors)
        return temps

    @RPC.export
    def sensors_fans(self, sub_points=None, included_sensors=None):
        """Return fan speed in RPM."""
        fans = psutil.sensors_fans()
        fans = self._format_return(fans, sub_points,includes=included_sensors)
        return fans

    @RPC.export
    def sensors_battery(self, sub_points=None):
        """Return battery status information."""
        battery = psutil.sensors_battery()
        battery = self._format_return(battery, sub_points)
        return battery

    @RPC.export
    def boot_time(self):
        """Return time of last system boot as seconds since the epoch."""
        return psutil.boot_time()

    @RPC.export
    def users(self, sub_points=None):
        """Return user session data for users currently connected to the system."""
        users = psutil.users()
        users = self._format_return(users, sub_points)
        return users

    @RPC.export
    def reconfigure(self, **kwargs):
        """Reconfigure the agent"""
        self._configure(kwargs)
        self._restart()  # TODO: Rewrite reconfigure method.

    # TODO: HANDLE THE VERSION DEPENDENCY FOR LOAD AVG
    # TODO: Update README.md to explain new style configuration format.
    # TODO: Make consistent use of single and dict returns. (single saved as point_name, dict with sub_points.)

    def _format_return(self, stats, sub_points, includes=None):
        if type(stats).__bases__[0] == tuple:
            stats = {0: stats}
        elif type(stats) == list:
            stats = dict(enumerate(stats))
        else:
            stats = {'total': stats}
        if includes:
            includes = includes if type(includes) is list else [includes]
            stats = {key:value for (key, value) in stats.items() if key in includes}
        for k, v in stats.items():
            if v is list:
                for item in v:
                    self._filter_sub_points(item, sub_points)
            else:
                self._filter_sub_points(v, sub_points)
        keys = list(stats.keys())
        if len(keys) == 1 and keys[0] == 0:
            stats = stats[0]
        return stats

    @staticmethod
    def _filter_sub_points(item, sub_points):
        if sub_points:
            return {key: value for (key, value) in item._asdict().items() if key in sub_points}
        else:
            return {key: value for (key, value) in item._asdict().items()}


def main(argv=sys.argv):
    """Main method called by the platform."""
    utils.vip_main(SysMonAgent, identity='platform.sysmon', version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
