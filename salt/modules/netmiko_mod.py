# -*- coding: utf-8 -*-
'''
netmiko execution module
'''
from __future__ import absolute_import

# Import python stdlib
import logging

# Import third party libs
try:
    from netmiko import ConnectHandler
    HAS_NETMIKO = True
except ImportError:
    HAS_NETMIKO = False

# -----------------------------------------------------------------------------
# execution module properties
# -----------------------------------------------------------------------------

__proxyenabled__ = ['netmiko']
# proxy name

# -----------------------------------------------------------------------------
# globals
# -----------------------------------------------------------------------------

__virtualname__ = 'netmiko'
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# propery functions
# -----------------------------------------------------------------------------


def __virtual__():
    '''
    Execution module available only if Netmiko is installed.
    '''
    if not HAS_NETMIKO:
        return False, 'The netmiko execution module requires netmio library to be installed.'
    return __virtualname__


def send_command(*args, **kwargs):
    return __proxy__['netmiko.call']('send_command', *args, **kwargs)
