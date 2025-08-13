#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink
from time import sleep
import time

def validate_connectivity(net):
    """Test connectivity between key host pairs"""
    info('*** Testing connectivity...\n')
    
    # Test a few representative connections
    test_pairs = [('h1', 'h2'), ('h1', 'h8'), ('h1', 'h16'), ('h8', 'h16')]
    
    connectivity_results = []
    for src, dst in test_pairs:
        src_host = net.get(src)
        dst_host = net.get(dst)
        
        result = src_host.cmd(f'ping -c 1 -W 2 {dst_host.IP()}')
        success = '1 received' in result
        connectivity_results.append((src, dst, success))
        
        if success:
            info(f'{src} -> {dst}: OK\n')
        else:
            info(f'{src} -> {dst}: FAILED\n')
    
    return connectivity_results

def generate_traffic(net):
    """Enhanced traffic generation with result logging"""
    
    info('*** Generating Traffic...\n')
    
    # Create traffic log file
    with open('traffic_log.txt', 'w') as log_file:
        log_file.write(f"Traffic Generation Started: {time.ctime()}\n")
        log_file.write("="*50 + "\n")
    
    # Get host objects more efficiently
    hosts = {}
    for i in range(1, 17):
        hosts[f'h{i}'] = net.get(f'h{i}')
    
    # Create a simple index.html file for HTTP testing
    hosts['h5'].cmd('echo "Hello from SDN Fat-Tree Network - Host 5" > index.html')
    hosts['h9'].cmd('echo "Hello from SDN Fat-Tree Network - Host 9" > index.html')
    
    # 1. TCP Traffic (multiple pairs for better testing)
    info('Starting TCP traffic...\n')
    hosts['h1'].cmd('iperf -s -p 5001 > tcp_server_h1.log 2>&1 &')
    sleep(1)
    hosts['h2'].cmd('iperf -c 10.0.0.1 -p 5001 -t 60 -i 10 > tcp_client_h2.log 2>&1 &')
    
    # Cross-pod TCP traffic
    hosts['h9'].cmd('iperf -s -p 5002 > tcp_server_h9.log 2>&1 &')
    sleep(1)
    hosts['h16'].cmd('iperf -c 10.0.0.9 -p 5002 -t 60 -i 10 > tcp_client_h16.log 2>&1 &')
    
    # 2. UDP Traffic with different bandwidth settings
    info('Starting UDP traffic...\n')
    hosts['h3'].cmd('iperf -s -u -p 5003 > udp_server_h3.log 2>&1 &')
    sleep(1)
    hosts['h4'].cmd('iperf -c 10.0.0.3 -u -p 5003 -t 60 -b 10M -i 10 > udp_client_h4.log 2>&1 &')
    
    # 3. Continuous ping for latency measurement
    info('Starting ICMP traffic...\n')
    hosts['h7'].cmd('ping 10.0.0.8 -i 0.5 -c 120 > ping_h7_h8.log 2>&1 &')
    hosts['h11'].cmd('ping 10.0.0.12 -i 0.5 -c 120 > ping_h11_h12.log 2>&1 &')
    
    # 4. HTTP Traffic
    info('Starting HTTP traffic...\n')
    hosts['h5'].cmd('python -m SimpleHTTPServer 8080 > http_server_h5.log 2>&1 &')
    sleep(2)  # Give server time to start
    hosts['h6'].cmd('for i in {1..20}; do wget -O /dev/null http://10.0.0.5:8080/index.html 2>>wget_h6.log; sleep 3; done &')
    
    # Additional HTTP server for diversity
    hosts['h13'].cmd('python -m SimpleHTTPServer 8081 > http_server_h13.log 2>&1 &')
    sleep(2)
    hosts['h14'].cmd('for i in {1..15}; do wget -O /dev/null http://10.0.0.13:8081/index.html 2>>wget_h14.log; sleep 4; done &')
    
    # 5. Mixed protocol traffic for comprehensive testing
    info('Starting additional mixed traffic...\n')
    hosts['h10'].cmd('iperf -s -p 5004 > tcp_server_h10.log 2>&1 &')
    sleep(1)
    hosts['h15'].cmd('iperf -c 10.0.0.10 -p 5004 -t 45 -i 5 > tcp_client_h15.log 2>&1 &')

def treeTopo():
    "Create a Fat-Tree network topology"

    net = Mininet(
        controller=RemoteController,
        switch=OVSKernelSwitch,
        link=TCLink
    )

    info('*** Adding controller\n')
    net.addController(
        'c0',
        controller=RemoteController,
        ip='127.0.0.1',
        port=6633
    )

    info('*** Adding switches\n')
    core_switches = []
    agg_switches = []
    edge_switches = []

    # 2 Core Switches
    for i in range(2):
        switch = net.addSwitch(f'c{i+1}')
        core_switches.append(switch)

    # 4 Aggregation Switches
    for i in range(4):
        switch = net.addSwitch(f'a{i+1}')
        agg_switches.append(switch)

    # 8 Edge Switches
    for i in range(8):
        switch = net.addSwitch(f'e{i+1}')
        edge_switches.append(switch)

    info('*** Adding hosts\n')
    hosts = []
    # 16 Hosts
    for i in range(16):
        host = net.addHost(f'h{i+1}', ip=f'10.0.0.{i+1}/24')
        hosts.append(host)

    info('*** Creating links\n')
    
    # Core switches to Aggregation switches (full mesh)
    info('Linking Core to Aggregation switches...\n')
    for core_sw in core_switches:
        for agg_sw in agg_switches:
            net.addLink(core_sw, agg_sw, bw=100)  # 100 Mbps links

    # Aggregation switches to Edge switches (CORRECTED LOGIC)
    info('Linking Aggregation to Edge switches...\n')
    # Each aggregation switch connects to 4 edge switches
    # a1,a2 connect to e1,e2,e3,e4
    # a3,a4 connect to e5,e6,e7,e8
    for i in range(2):  # 2 pods
        pod_agg_switches = agg_switches[i*2:(i+1)*2]  # 2 agg switches per pod
        pod_edge_switches = edge_switches[i*4:(i+1)*4]  # 4 edge switches per pod
        
        for agg_sw in pod_agg_switches:
            for edge_sw in pod_edge_switches:
                net.addLink(agg_sw, edge_sw, bw=50)  # 50 Mbps links

    # Edge switches to Hosts
    info('Linking Edge switches to Hosts...\n')
    for i in range(8):
        edge_sw = edge_switches[i]
        for j in range(2):
            host = hosts[i*2+j]
            net.addLink(edge_sw, host, bw=10)  # 10 Mbps access links

    info('*** Network topology created\n')
    info('*** Topology Summary:\n')
    info(f'*** Core switches: {len(core_switches)}\n')
    info(f'*** Aggregation switches: {len(agg_switches)}\n')
    info(f'*** Edge switches: {len(edge_switches)}\n')
    info(f'*** Hosts: {len(hosts)}\n')

    info('*** Starting network\n')
    net.build()
    net.start()

    # Wait for network to stabilize
    info('*** Waiting for network to stabilize...\n')
    sleep(5)

    # Validate connectivity
    connectivity_results = validate_connectivity(net)
    failed_connections = [result for result in connectivity_results if not result[2]]
    
    if failed_connections:
        info('*** WARNING: Some connectivity tests failed:\n')
        for src, dst, _ in failed_connections:
            info(f'*** {src} -> {dst}: FAILED\n')
    else:
        info('*** All connectivity tests passed!\n')

    # Generate traffic
    generate_traffic(net)

    info('*** Traffic generation started in background.\n')
    info('*** Monitor these log files for results:\n')
    info('*** - flow_stats.csv (from Ryu controller)\n')
    info('*** - port_stats.csv (from Ryu controller)\n') 
    info('*** - tcp_*.log, udp_*.log, ping_*.log, wget_*.log\n')
    info('*** - traffic_log.txt\n')
    info('***\n')
    info('*** Useful commands in CLI:\n')
    info('*** - pingall: Test connectivity between all hosts\n')
    info('*** - iperf h1 h2: Quick iperf test\n')
    info('*** - h1 ping h16: Test cross-pod connectivity\n')
    info('*** - py net.ping(): Ping all pairs\n')
    info('***\n')
    info('*** Type "exit" to stop the simulation.\n')

    # Start CLI
    CLI(net)

    info('*** Stopping network\n')
    
    # Cleanup: Kill any remaining background processes
    info('*** Cleaning up background processes...\n')
    for host in hosts:
        host.cmd('pkill -f iperf')
        host.cmd('pkill -f python')
        host.cmd('pkill -f ping')
        host.cmd('pkill -f wget')
    
    net.stop()
    
    info('*** Network stopped. Check log files for results.\n')

if __name__ == '__main__':
    setLogLevel('info')
    treeTopo()
