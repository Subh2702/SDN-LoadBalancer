#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
import os

def config_queues():
    """
    s7–s10: min_rate = 5 Mbps, max_rate = 10 Mbps
    Others: min_rate = 1 Mbps, max_rate = 5 Mbps
    """
    for sw in ['edge7', 'edge8']:
        for port in range(1, 3):
            os.system(
                f"ovs-vsctl set Port {sw}-eth{port} qos=@newqos "
                f"-- --id=@newqos create QoS type=linux-htb other-config:max-rate=10000000 "
                f"queues:1=@q1 -- --id=@q1 create Queue "
                f"other-config:min-rate=5000000 other-config:max-rate=10000000"
            )

    low_rate_switches = ['core1', 'core2',
                         'agg1', 'agg2', 'agg3', 'agg4',
                         'edge1', 'edge2', 'edge3', 'edge4', 'edge5', 'edge6']
    for sw in low_rate_switches:
        for port in range(1, 3):
            os.system(
                f"ovs-vsctl set Port {sw}-eth{port} qos=@newqos "
                f"-- --id=@newqos create QoS type=linux-htb other-config:max-rate=5000000 "
                f"queues:1=@q1 -- --id=@q1 create Queue "
                f"other-config:min-rate=1000000 other-config:max-rate=5000000"
            )

def treeTopo():
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)

    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)

    core_switches = [net.addSwitch(f'core{i+1}', protocols='OpenFlow13') for i in range(2)]
    agg_switches = [net.addSwitch(f'agg{i+1}', protocols='OpenFlow13') for i in range(4)]
    edge_switches = [net.addSwitch(f'edge{i+1}', protocols='OpenFlow13') for i in range(8)]
    hosts = [net.addHost(f'h{i+1}') for i in range(16)]

    for core_sw in core_switches:
        for agg_sw in agg_switches:
            net.addLink(core_sw, agg_sw)

    net.addLink(agg_switches[0], edge_switches[0])
    net.addLink(agg_switches[0], edge_switches[1])
    net.addLink(agg_switches[1], edge_switches[2])
    net.addLink(agg_switches[1], edge_switches[3])
    net.addLink(agg_switches[2], edge_switches[4])
    net.addLink(agg_switches[2], edge_switches[5])
    net.addLink(agg_switches[3], edge_switches[6])
    net.addLink(agg_switches[3], edge_switches[7])

    for i in range(8):
        net.addLink(edge_switches[i], hosts[i*2])
        net.addLink(edge_switches[i], hosts[i*2+1])

    net.build()
    net.start()
    info("*** Configuring Queues...\n")
    config_queues()

    info("*** Running traffic experiments...\n")
    # Example: Run iperf TCP test between h1 and h16
    h1, h16 = net.get('h1'), net.get('h16')
    h16.cmd('iperf -s &')  # Start iperf server on h16
    result = h1.cmd('iperf -c ' + h16.IP() + ' -t 10')  # Run iperf client from h1
    info(result)

    # Example: Ping all hosts
    info("*** Running pingall...\n")
    net.pingAll()

    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    treeTopo()
