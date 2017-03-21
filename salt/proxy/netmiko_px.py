# -*- coding: utf-8 -*-
'''
netmiko proxy
'''
from __future__ import absolute_import

# Import python stdlib
import logging

# Import third party libs
try:
    from netmiko import ConnectHandler
    from netmiko.ssh_exception import NetMikoTimeoutException
    from netmiko.ssh_exception import NetMikoAuthenticationException
    HAS_NETMIKO = True
except ImportError:
    HAS_NETMIKO = False

# Import salt modules
from salt.ext import six

# -----------------------------------------------------------------------------
# proxy properties
# -----------------------------------------------------------------------------

__proxyenabled__ = ['netmiko']
# proxy name

# -----------------------------------------------------------------------------
# globals
# -----------------------------------------------------------------------------

__virtualname__ = 'netmiko'
log = logging.getLogger(__name__)
netmiko_device = {}

# -----------------------------------------------------------------------------
# propery functions
# -----------------------------------------------------------------------------


def __virtual__():
    '''
    Proxy module available only if Netmiko is installed.
    '''
    if not HAS_NETMIKO:
        return False, 'The netmiko proxy module requires netmio library to be installed.'
    return __virtualname__

# -----------------------------------------------------------------------------
# proxy functions
# -----------------------------------------------------------------------------


def init(opts):
    '''
    Open the connection to the network device
    managed through netmiko.
    '''
    proxy_dict = opts.get('proxy', {})
    netmiko_connection_args = {}
    netmiko_connection_args.update(proxy_dict)
    netmiko_connection_args.pop('proxytype', None)
    try:
        connection = ConnectHandler(**netmiko_connection_args)
        netmiko_device['connection'] = connection
        netmiko_device['initialized'] = True
        netmiko_device['up'] = True
    except NetMikoTimeoutException as t_err:
        log.error('Unable to setup the netmiko connection', exc_info=True)
    except NetMikoAuthenticationException as au_err:
        log.error('Unable to setup the netmiko connection', exc_info=True)
    return True


def alive(opts):
    '''
    Return the connection status with the network device.
    '''
    if ping() and initialized():
        return netmiko_device['connection'].remote_conn.transport.is_alive()
    return False


def ping():
    '''
    Connection open successfully?
    '''
    return netmiko_device.get('up', False)


def initialized():
    '''
    Connection finished initializing?
    '''
    return netmiko_device.get('initialized', False)


def shutdown(opts):
    '''
    Closes connection with the device.
    '''
    return call('disconnect')


# -----------------------------------------------------------------------------
# callable functions
# -----------------------------------------------------------------------------


def call(method, *args, **kwargs):
    '''
    Calls an arbitrary netmiko method.
    '''
    kwargs_copy = {}
    kwargs_copy.update(kwargs)
    for karg, warg in six.iteritems(kwargs_copy):
        if warg is None or karg.startswith('__pub_'):
            kwargs.pop(karg)
    return getattr(netmiko_device.get('connection'), method)(*args, **kwargs)
