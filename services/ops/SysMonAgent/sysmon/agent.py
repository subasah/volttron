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
from os import path, walk, access, R_OK
from collections import namedtuple
from gevent import sleep

from volttron import platform  # Used to get VOLTTRON version #.
from volttron.platform.vip.agent import Agent, RPC
from volttron.platform.agent import utils
if int(platform.__version__.split('.')[0]) >= 6:
    from volttron.platform.scheduling import periodic


utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = '4.0'


# TODO: HANDLE THE VERSION DEPENDENCY FOR LOAD AVG
# TODO: Update README.md to explain new style configuration format.
class SysMonAgent(Agent):
    """Monitor utilization of system resources (CPU, memory, swap, disk, network)

    Statistics for each system resource can be queried via
    RPC and they are published periodically to configured topics.

    :param config: Configuration dict
    :type config: dict
    """

    IMPLEMENTED_METHODS = (
        'cpu_percent', 'cpu_times', 'cpu_times_percent', 'cpu_statistics', 'cpu_frequency', 'cpu_count', 'load_average',
        'memory', 'memory_percent', 'swap', 'disk_partitions', 'disk_usage', 'disk_percent', 'disk_io', 'path_usage',
        'path_usage_rate', 'network_io', 'network_connections', 'network_interface_addresses',
        'network_interface_statistics', 'sensors_temperatures', 'sensors_fans', 'sensors_battery', 'boot_time', 'users'
    )

    RECORD_ONLY_PUBLISH_METHODS = ('disk_partitions', 'network_connections', 'network_interface_address',
                                   'sensors_temperatures', 'users')

    UNITS = {'boot_time': 's',
             'cpu_count': 'count',
             'cpu_frequency': 'MHz',
             'cpu_percent': 'percent',
             'cpu_stats': 'count',
             'cpu_times': 's',
             'cpu_times_percent': 'percent',
             'disk_io': {'read_count': 'reads', 'write_count': 'writes', 'read_bytes': 'bytes', 'write_bytes': 'bytes',
                         'read_time': 'ms', 'write_time': 'ms', 'read_merged_count': 'reads',
                         'write_merged_count': 'writes', 'busy_time': 'ms', 'read_throughput': 'bytes/s',
                         'write_throughput': 'bytes/s'},
             'disk_partitions': None,
             'disk_percent': 'percent',
             'disk_usage': {'total': 'bytes', 'used': 'bytes', 'free': 'bytes', 'percent': 'percent'},
             'load_average': 'load_average',
             'memory': {'total': 'bytes', 'available': 'bytes', 'percent': 'percent', 'used': 'bytes', 'free': 'bytes',
                        'active': 'bytes', 'inactive': 'bytes', 'buffers': 'bytes', 'cached': 'bytes',
                        'shared': 'bytes', 'slab': 'bytes'},
             'memory_percent': 'percent',
             'network_connections': None,
             'network_interface_address': None,
             'network_interface_statitics': {'isup': 'bool', 'duplex': 'enum', 'speed': 'Mbps', 'mtu': 'bytes'},
             'network_io': {'bytes_sent': 'bytes', 'bytes_recv': 'bytes', 'packets_sent': 'packets',
                            'packets_recv': 'packets', 'errin': 'errors', 'errout': 'errors', 'dropin': 'packets',
                            'dropout': 'packets', "receive_throughput": 'bytes/s', "send_throughput": 'bytes/s'},
             'path_usage': 'bytes',
             'path_usage_rate': 'bytes/s',
             'sensors_battery': {'percent': 'percent', 'secsleft': 's', 'power_plugged': 'bool'},
             'sensors_fans': 'rpm',
             'sensors_temperatures': 'degrees',
             'swap': {'total': 'bytes', 'used': 'bytes', 'free': 'bytes', 'percent': 'bytes', 'sin': 'bytes',
                      'sout': 'bytes'},
             'users': None
             }

    publish_data = namedtuple('publish_data', ['value', 'units', 'data_type', 'now'])

    def __init__(self, config_path, **kwargs):
        super(SysMonAgent, self).__init__(**kwargs)
        self.default_publish_type = None
        self.base_topic = None
        self._scheduled = []

        # Tracking variables.
        # Initial value for last_path_sizes is set in on_configure for configured paths.
        self.last_path_sizes = {}  # Used by path_usage_rate: {path: {usage, dt}}
        self.last_disk_read_bytes = {}  # Used by disk_io: {disk: {bytes, dt}}
        self.last_disk_write_bytes = {}  # Used by disk_io: {disk: {bytes, dt}}
        # Set initial values for self.last_disk_read_bytes and self.last_disk_write_bytes.
        self.disk_io(per_disk=False)
        self.disk_io(per_disk=True)
        self.last_network_received_bytes = {}  # Used by network_io: {disk: {bytes, dt}}
        self.last_network_sent_bytes = {}  # Used by network_io: {disk: {bytes, dt}}
        # Set initial values for self.last_network_received_bytes and self.last_network_sent_bytes
        self.network_io(per_nic=False)
        self.network_io(per_nic=True)

        default_config = utils.load_config(config_path)
        self.vip.config.set_default('config', default_config)
        self.vip.config.subscribe(self.on_reconfigure, actions=['UPDATE', 'DELETE'], pattern='_runtime_config')
        self.vip.config.subscribe(self.on_configure, actions=['NEW', 'UPDATE'], pattern='config')

    def on_configure(self, config_name, action, contents):
        _log.info('Received configuration store event of type: {}. Loading configuration from config://{}'.format(
            action, config_name))

        # Stop any currently scheduled monitors.
        for sched in self._scheduled:
            sched.cancel()
        self._scheduled = []

        self.default_publish_type = contents.pop('default_publish_type', None)
        if self.default_publish_type:
            self.base_topic = contents.pop('base_topic', 'Log/Platform')
        else:  # TODO: Deprecated configuration block. Make if block unconditional in future release:
            # BEGIN DEPRECATED CONFIGURATION BLOCK.
            self.default_publish_type, self.base_topic = contents.pop('base_topic',
                                                                      'datalogger/log/platform').split('/', 1)
            _log.warning('No "default_publish_type" configuration found, using deprecated configuration method:'
                         'default_publish_type: "datalogger", base_topic: default_publish_type + "log/platform".'
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
                             .format(dep[1], dep[0]))
            elif deprecated_interval:
                monitors[dep[0]] = {'point_name': dep[0], 'check_interval': dep[1], 'poll': True}
                _log.warning('Starting cpu_percent monitor using deprecated configuration "cpu_check_interval".'
                             ' Update configuration to use monitor["cpu_percent"]["check_interval"].'
                             'See SysMonAgent/README.md for information on new configuration format.')
        # END DEPRECATED CONFIGURATION BLOCK.

        # Start Monitors:
        sleep(1)  # Wait for a second to pass to avoid divide by zero errors from tracking variables.
        for method in self.IMPLEMENTED_METHODS:
            item = monitors.pop(method, None)
            if method == 'path_usage_rate' and item.get('path_name', None):
                # Set initial value(s) of self.last_path_sizes for any configured path names.
                self.path_usage_rate(item.get('path_name'))
            if item and item.pop('poll', None) is True:
                item_publish_type = item.get('publish_type', None)
                item_publish_type = 'record' if method in self.RECORD_ONLY_PUBLISH_METHODS else item_publish_type
                item_publish_type = item_publish_type if item_publish_type else self.default_publish_type
                self._periodic_pub(getattr(self, method), item_publish_type, item['check_interval'],
                                   item['point_name'], item['params'])

        for key in contents:
            _log.warning('Ignoring unrecognized configuration parameter %s', key)
        for key in monitors:
            _log.warning('Ignoring unimplemented monitor method: {}'.format(key))

    def _periodic_pub(self, func, publish_type, check_interval, point_name, params):
        """Periodically call func and publish its return value"""

        def _unpack(topic, item, now, entries=None):
            data_type = type(item)
            entries = entries if entries else {}
            if data_type in [int, float, str, bool, None]:
                units = self.UNITS[func.__name__]
                topic = path.normpath(topic)
                if type(units) == dict:
                    units = units[topic.split('/')[-1]]
                entries[topic] = self.publish_data(item, units, type(item).__name__, now)
            elif data_type is dict:
                for k, v in item.items():
                    entries = _unpack(topic + '/' + str(k), v, now, entries)
            else:
                _log.warning('Unexpected return type from method: {}'.format(func.__name__))
            return entries

        def _datalogger_publish(parameters):
            data = func(**parameters)
            now = utils.get_aware_utc_now()
            tz = now.tzname()
            now = utils.format_timestamp(now)
            entries = _unpack(point_name, data, now)
            point_base = path.dirname(path.commonprefix(list(entries.keys())))
            entries = {path.relpath(topic, point_base): value for topic, value in entries.items()}
            message = {}
            header = {'Date': now}
            for k, v in entries.items():
                message[k] = {'Readings': [v.now, v.value], 'Units': v.units, 'tz': tz, 'data_type': v.data_type}
            self.vip.pubsub.publish(peer='pubsub', topic=publish_type + '/' + self.base_topic + '/' + point_base,
                                    headers=header, message=message)

        def _all_type_publish(parameters):
            data = func(**parameters)
            now = utils.format_timestamp(utils.get_aware_utc_now())
            entries = _unpack(point_name, data, now)
            point_base = path.dirname(path.commonprefix(list(entries.keys())))
            entries = {path.relpath(topic, point_base): value for topic, value in entries.items()}
            val, meta = {}, {}
            for k, v in entries.items():
                val[k] = v
                meta[k] = {'Units': v.units, 'data_type': v.data_type}
            message = [val, meta]
            header = {'Date': now}
            topic = publish_type + '/' + self.base_topic + '/' + point_base + '/all'
            self.vip.pubsub.publish(peer='pubsub', topic=topic, headers=header, message=message)

        def _record_publish(parameters):
            data = func(**parameters)
            now = utils.format_timestamp(utils.get_aware_utc_now())
            header = {'Date': now}
            self.vip.pubsub.publish(peer='pubsub', topic=publish_type + '/' + self.base_topic + '/' + point_name,
                                    headers=header, message=data)

        if publish_type == 'record':
            pub_wrapper = _record_publish
        elif publish_type == 'datalogger':
            pub_wrapper = _datalogger_publish
        else:
            pub_wrapper = _all_type_publish
        if int(platform.__version__.split('.')[0]) >= 6:  # TODO: Deprecated self.core.periodic required to support VOLTTRON < 6.
            sched = self.core.schedule(periodic(check_interval), pub_wrapper, params)
        else:
            sched = self.core.periodic(check_interval, pub_wrapper, kwargs={'parameters': params})
        self._scheduled.append(sched)

    def on_reconfigure(self, config_name, action, contents):
        _log.info('Received configuration store event of type: {}. Reconfiguring from config://{}'.format(
            action, config_name))
        # TODO: Write runtime reconfiguration'
        pass

    @RPC.export('cpu_percent')
    def cpu_percent(self, per_cpu=False, capture_interval=None):
        """Return CPU usage percentage"""
        cpu_stats = psutil.cpu_percent(percpu=per_cpu, interval=capture_interval)
        if per_cpu:
            return dict(enumerate(cpu_stats))
        else:
            return cpu_stats

    @RPC.export('cpu_times')
    def cpu_times(self, per_cpu=False, sub_points=None):
        """Return percentage of time the CPU has spent in a given mode."""
        times = psutil.cpu_times_percent(percpu=per_cpu)
        times = self._process_statistics(times, sub_points=sub_points)
        return times

    @RPC.export('cpu_times_percent')
    def cpu_times_percent(self, per_cpu=False, sub_points=None, capture_interval=None):
        """Return percentage of time the CPU has spent in a given mode."""
        percentages = psutil.cpu_times_percent(interval=capture_interval, percpu=per_cpu)
        percentages = self._process_statistics(percentages, sub_points=sub_points)
        return percentages

    @RPC.export('cpu_count')
    def cpu_count(self, logical=True):
        """Return the number of CPU cores if logical=True or the number of physical CPUs if logical=False"""
        return psutil.cpu_count(logical)

    @RPC.export('cpu_stats')
    def cpu_stats(self, sub_points=None):
        """Return various CPU statistics."""
        stats = psutil.cpu_stats()
        stats = self._process_statistics(stats, sub_points=sub_points)
        return stats

    @RPC.export('cpu_frequency')
    def cpu_frequency(self, per_cpu=False, sub_points=None):
        freq = psutil.cpu_freq(percpu=per_cpu)
        freq = self._process_statistics(freq, sub_points=sub_points)
        return freq

    @RPC.export('memory_percent')
    def memory_percent(self):
        """Return memory usage percentage"""
        _log.warning('Method "memory_percent" is deprecated. Use "memory" instead.')
        return self.memory()['percent']

    @RPC.export('memory')
    def memory(self, sub_points=None):
        """Return memory usage statistics"""
        virtual_memory = psutil.virtual_memory()
        return self._process_statistics(virtual_memory, sub_points)

    @RPC.export('swap')
    def swap(self, sub_points=None):
        """Return swap usage statistics"""
        swap_memory = psutil.swap_memory()
        return self._process_statistics(swap_memory, sub_points)

    # TODO: Should includes allow filtering by mount_point, device, etc, rather than enumeration?
    @RPC.export('disk_partitions')
    def disk_partitions(self, all_partitions=False, included_partitions=None, sub_points=None):
        partitions = psutil.disk_partitions(all_partitions)
        partitions = self._process_statistics(partitions, sub_points=sub_points, includes=included_partitions)
        return partitions

    @RPC.export('disk_percent')
    def disk_percent(self, disk_path='/'):
        """Return usage of disk mounted at configured path"""
        _log.warning('Method "disk_percent" is deprecated. Use "disk_usage" instead.')
        return self.disk_usage(disk_path)[disk_path]['percent']

    @RPC.export('disk_usage')
    def disk_usage(self, disk_path='/', sub_points=None):
        """Return disk usage statistics."""
        usage = {}
        if type(disk_path) is not list:
            disk_path = [disk_path]
        for d_path in disk_path:
            usage[d_path] = psutil.disk_usage(d_path)
        return self._process_statistics(usage, sub_points)

    @RPC.export('load_average')
    def load_average(self, sub_points=None):
        """Return load averages."""
        las = namedtuple('las', ('OneMinute', 'FiveMinute', 'FifteenMinute'))
        averages = las(*psutil.getloadavg())
        # noinspection PyTypeChecker
        return self._process_statistics(averages, sub_points)

    @RPC.export('path_usage')
    def path_usage(self, path_name):
        """Return storage used within a filesystem path."""
        path_size = {}
        if type(path_name) is not list:
            path_name = [path_name]
        for path_n in path_name:
            try:
                if path.isfile(path_n):
                    path_size[path_n] = path.getsize(path_n)
                elif path.isdir(path_n):
                    if not access(path_n, R_OK):
                        raise PermissionError('Inaccessible path: path_n. Check read permissions and that path exists.')
                    path_size[path_n] = sum(path.getsize(path.join(dir_path, filename))
                                            for dir_path, dir_names, filenames
                                            in walk(path_n) for filename in filenames)
                else:
                    raise Exception('Path is neither a file nor a directory: {}'.format(path_n))
            except Exception as e:
                _log.error('Exception in path_usage: {}'.format(e))
                path_size[path_n] = -1  # Error code -1 indicates path exception.
        return path_size

    @RPC.export('path_usage_rate')
    def path_usage_rate(self, path_name, interval=None):
        """Return rate of change in storage used within a filesystem path in bytes per second."""
        current_path_sizes = {}
        rates = {}
        if type(path_name) is not list:
            path_name = [path_name]
        for path_n in path_name:
            current_usage = self.path_usage(path_n)[path_n]
            now = utils.get_aware_utc_now()
            if current_usage > 0:  # Don't use or store error codes as a usage value.
                current_path_sizes[path_n] = {'value': current_usage, 'dt': now}
                if path_n in self.last_path_sizes:
                    rates[path_n] = (current_usage - self.last_path_sizes[path_n]['value']) \
                                    / (now - self.last_path_sizes[path_n]['dt']).seconds
                else:
                    rates[path_n] = -2  # Error code -2 indicates no prior tracking data. Call again for valid response.
            else:
                rates[path_n] = current_usage  # Return error code from self.path_usage.
        self.last_path_sizes.update(current_path_sizes)
        return rates

    @RPC.export('disk_io')
    def disk_io(self, per_disk=False, no_wrap=True, included_disks=None, sub_points=None):
        """Return disk input/output statistics."""
        if included_disks and not per_disk:
            per_disk = True
        io_stats = psutil.disk_io_counters(perdisk=per_disk, nowrap=no_wrap)
        retval = self._process_statistics(io_stats, sub_points, includes=included_disks)
        self._get_throughput(io_stats, retval, per_disk, sub_points, 'read_throughput', 'write_throughput',
                             'read_bytes', 'write_bytes', self.last_disk_read_bytes, self.last_disk_write_bytes)
        retval = self._format_return(retval)
        return retval

    @RPC.export('network_io')
    def network_io(self, per_nic=False, no_wrap=True, included_nics=None, sub_points=None):
        """Return network input/output statistics."""
        if included_nics and not per_nic:
            per_nic = True
        io_stats = psutil.net_io_counters(pernic=per_nic, nowrap=no_wrap)
        retval = self._process_statistics(io_stats, sub_points, includes=included_nics)
        self._get_throughput(io_stats, retval, per_nic, sub_points, 'receive_throughput', 'send_throughput',
                             'bytes_recv', 'bytes_sent', self.last_network_received_bytes, self.last_network_sent_bytes)
        retval = self._format_return(retval)
        return retval

    @RPC.export('network_connections')
    def network_connections(self, kind='inet', sub_points=None):
        """Return system-wide socket connections"""
        connections = psutil.net_connections(kind)
        connections = self._process_statistics(connections, sub_points=sub_points, format_return=False)
        if sys.version_info.major >= 3:  # TODO: Deprecated -- not an enum in Python < 3.4.
            for k, v in connections.items():
                if 'family' in v:
                    v['family'] = v['family'].name
                if 'type' in v:
                    v['type'] = v['type'].name
                if 'laddr' in v:
                    v['laddr'] = v['laddr'].ip + ':' + str(v['laddr'].port) \
                        if type(v['laddr']) is psutil._common.addr else ''
                if 'raddr' in v:
                    v['raddr'] = v['raddr'].ip + ':' + str(v['raddr'].port) \
                        if type(v['raddr']) is psutil._common.addr else ''
        connections = self._format_return(connections)
        return connections

    @RPC.export('network_interface_addresses')
    def network_interface_addresses(self, included_interfaces=None, sub_points=None):
        """Return addresses associated with network interfaces."""
        addresses = psutil.net_if_addrs()
        addresses = self._process_statistics(addresses, sub_points, includes=included_interfaces, format_return=False)
        if sys.version_info.major >= 3:  # TODO: Deprecated -- not an enum in Python < 3.4.
            for k, v in addresses.items():
                for item in v:
                    if 'family' in item:
                        item['family'] = item['family'].name
        addresses = self._format_return(addresses)
        return addresses

    @RPC.export('network_interface_statistics')
    def network_interface_statistics(self, included_interfaces=None, sub_points=None):
        """Return information about each network interface."""
        stats = psutil.net_if_stats()
        stats = self._process_statistics(stats, sub_points, includes=included_interfaces, format_return=False)
        if sys.version_info.major >= 3:  # TODO: Deprecated -- not an enum in Python < 3.4.
            for k, v in stats.items():
                if 'duplex' in v:
                    v['duplex'] = v['duplex'].name
            stats = self._format_return(stats)
        return stats

    # TODO: Currently marked record only publish, as psutil bug #1708 duplicates core temps.
    # TODO: Change list into dict of label names, pending resolution of psutil bug #1708 if labels become unique.
    @RPC.export('sensors_temperatures')
    def sensors_temperatures(self, fahrenheit=False, sub_points=None, included_sensors=None):
        """Return hardware temperatures."""
        temps = psutil.sensors_temperatures(fahrenheit)
        temps = self._process_statistics(temps, sub_points, includes=included_sensors)
        return temps

    @RPC.export('sensors_fans')
    def sensors_fans(self, sub_points=None, included_sensors=None):
        """Return fan speed in RPM."""
        fans = psutil.sensors_fans()
        fans = self._process_statistics(fans, sub_points, includes=included_sensors)
        return fans

    @RPC.export('sensors_battery')
    def sensors_battery(self, sub_points=None):
        """Return battery status information."""
        battery = psutil.sensors_battery()
        battery = self._process_statistics(battery, sub_points)
        return battery

    @RPC.export('boot_time')
    def boot_time(self):
        """Return time of last system boot as seconds since the epoch."""
        return psutil.boot_time()

    @RPC.export('users')
    def users(self, sub_points=None):
        """Return user session data for users currently connected to the system."""
        users = psutil.users()
        users = self._process_statistics(users, sub_points)
        return users

    @RPC.export('reconfigure')
    def reconfigure(self, **kwargs):
        """Reconfigure the agent"""
        # self._configure(kwargs)
        # self._restart()  # TODO: Rewrite reconfigure method.
        pass

    def _process_statistics(self, stats, sub_points, includes=None, format_return=True):
        if type(stats).__bases__[0] == tuple:  # Case: stats is a named tuple.
            retval = {0: stats}
        elif type(stats) == list:  # Case: stats is a list of values or named tuples.
            retval = dict(enumerate(stats))
        elif type(stats) == dict:
            retval = stats.copy()
        elif stats is None:
            retval = {}
        else:  # Case: stats is a single value
            retval = {'total': stats}
        if includes:
            retval = self._filter_includes(includes, retval)
        for k, v in retval.items():
            if type(v) is list:  # Handle nested lists of named tuples
                for index, item in enumerate(v):
                    retval[k][index] = self._filter_sub_points(item, sub_points)
            else:
                retval[k] = self._filter_sub_points(v, sub_points)
        if format_return:
            retval = self._format_return(retval)
        return retval

    @staticmethod
    def _filter_includes(includes, stats):
            includes = includes if type(includes) is list else [includes]
            return {key: value for (key, value) in stats.items() if key in includes}

    @staticmethod
    def _format_return(stats):
        keys = list(stats.keys())
        # TODO: This leads to odd behavior for disk_partitions, where if only 0 is requested it has a different output
        #  format than if only 1 is requested.
        if len(keys) == 1 and keys[0] == 0:  # TODO: Do we care if it is an enumerated list or do this for single dicts?
            stats = stats[0]  # TODO: Does this need another [0] at the end?
        return stats

    @staticmethod
    def _filter_sub_points(item, sub_points):
        if type(sub_points) is list:
            return {key: value for (key, value) in item._asdict().items() if key in sub_points}
        elif type(sub_points) is str:
            return {key: value for (key, value) in item._asdict().items() if key == sub_points}
        elif type(sub_points) is dict:
            return {key: value for (key, value) in item._asdict().items() if sub_points.get(key, False) is True}
        else:
            return {key: value for (key, value) in item._asdict().items()}

    # TODO: The tracking variables for this should us a look back buffer, not just last value as currently used.
    @staticmethod
    def _get_throughput(io_stats, retval, per_device, sub_points,
                        in_ret_label, out_ret_label, in_label, out_label, last_in, last_out):
        now = utils.get_aware_utc_now()
        current_in_bytes = {}
        current_out_bytes = {}
        if per_device is False:
            current_in_bytes = {'not_per_device': {'value': getattr(io_stats, in_label), 'dt': now}}
            current_out_bytes = {'not_per_device': {'value': getattr(io_stats, out_label), 'dt': now}}
        else:
            for device in io_stats:
                current_in_bytes[device] = {'value': getattr(io_stats[device], in_label), 'dt': now}
                current_out_bytes[device] = {'value': getattr(io_stats[device], out_label), 'dt': now}

        for device, in_bytes in current_in_bytes.items():
            if device in last_in:
                throughput = (in_bytes['value'] - last_in[device]['value']) / (now - last_in[device]['dt']).seconds
            else:
                throughput = -2
            if not sub_points or in_ret_label in sub_points:
                if not per_device:
                    retval[in_ret_label] = throughput
                elif device in retval:
                    retval[device][in_ret_label] = throughput
        last_in.update(current_in_bytes)
        for device, out_bytes in current_out_bytes.items():
            if device in last_out:
                throughput = (out_bytes['value'] - last_out[device]['value']) / (now - last_out[device]['dt']).seconds
            else:
                throughput = -2
            if not sub_points or out_ret_label in sub_points:
                if not per_device:
                    retval[out_ret_label] = throughput
                elif device in retval:
                    retval[device][out_ret_label] = throughput
        last_out.update(current_out_bytes)

def main():
    """Main method called by the platform."""
    utils.vip_main(SysMonAgent, identity='platform.sysmon', version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
