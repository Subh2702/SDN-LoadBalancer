# sdn_monitor.py

from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub

import csv
import time
from operator import attrgetter


class SdnMonitor(simple_switch_13.SimpleSwitch13):
    """
    A custom Ryu application that extends SimpleSwitch13 to monitor
    and log network statistics periodically.
    """

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SdnMonitor, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)
        
        # Store previous statistics for throughput calculation
        self.port_stats_history = {}
        self.flow_stats_history = {}

        # Separate CSV files for different metrics
        try:
            self.flow_csv = open("flow_stats.csv", "w", newline='')
            self.port_csv = open("port_stats.csv", "w", newline='')
            
            self.flow_writer = csv.writer(self.flow_csv)
            self.port_writer = csv.writer(self.port_csv)
            
            # Flow stats header
            self.flow_writer.writerow([
                'timestamp', 'datapath_id', 'flow_id', 'in_port', 'eth_dst',
                'out_port', 'packets', 'bytes', 'duration_sec', 'throughput_bps',
                'packet_rate_pps'
            ])
            
            # Port stats header  
            self.port_writer.writerow([
                'timestamp', 'datapath_id', 'port_no', 'rx_packets', 'tx_packets',
                'rx_bytes', 'tx_bytes', 'rx_errors', 'tx_errors', 'rx_bps', 'tx_bps',
                'rx_pps', 'tx_pps', 'error_rate_percent'
            ])
            
            self.flow_csv.flush()
            self.port_csv.flush()
            
            self.logger.info("CSV files initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing CSV files: {e}")

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """
        Handles switch connection and disconnection events.
        """
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.debug('Register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('Unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    def _monitor(self):
        """
        Main monitoring loop that periodically requests stats from switches.
        """
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            # Wait for 10 seconds before the next round of requests
            hub.sleep(10)

    def _request_stats(self, datapath):
        """
        Sends flow and port stats requests to a given switch (datapath).
        """
        self.logger.debug('Sending stats request to datapath: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Request flow stats
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

        # Request port stats
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        """
        Handles flow stats replies from switches and logs them to CSV.
        """
        body = ev.msg.body
        datapath_id = ev.msg.datapath.id
        current_time = time.time()

        try:
            for stat in sorted([flow for flow in body if flow.priority == 1],
                               key=lambda flow: (flow.match.get('in_port', 0), 
                                               str(flow.match.get('eth_dst', '')))):
                
                # Handle cases where match fields might not exist
                in_port = stat.match.get('in_port', 'N/A')
                eth_dst = stat.match.get('eth_dst', 'N/A')
                
                # Extract output port more safely
                out_port = 'N/A'
                try:
                    if stat.instructions and len(stat.instructions) > 0:
                        instruction = stat.instructions[0]
                        if hasattr(instruction, 'actions') and len(instruction.actions) > 0:
                            action = instruction.actions[0]
                            if hasattr(action, 'port'):
                                out_port = action.port
                except (IndexError, AttributeError):
                    out_port = 'N/A'
                
                flow_id = f"{in_port}-{eth_dst}-{out_port}"
                flow_key = f"{datapath_id}-{flow_id}"
                
                # Calculate throughput and packet rate
                throughput_bps = 0
                packet_rate_pps = 0
                
                if stat.duration_sec > 0:
                    throughput_bps = (stat.byte_count * 8) / stat.duration_sec
                    packet_rate_pps = stat.packet_count / stat.duration_sec
                
                # Store current stats for future rate calculations
                self.flow_stats_history[flow_key] = (stat, current_time)
                
                self.flow_writer.writerow([
                    current_time, datapath_id, flow_id, in_port, eth_dst,
                    out_port, stat.packet_count, stat.byte_count, 
                    stat.duration_sec, throughput_bps, packet_rate_pps
                ])
            
            self.flow_csv.flush()
            
        except Exception as e:
            self.logger.error(f"Error processing flow stats: {e}")

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        """
        Handles port stats replies from switches and logs them to CSV.
        """
        body = ev.msg.body
        datapath_id = ev.msg.datapath.id
        current_time = time.time()

        try:
            for stat in sorted(body, key=attrgetter('port_no')):
                # Skip local port (usually port 0xfffffffe)
                if stat.port_no >= 0xffffff00:
                    continue
                    
                port_key = f"{datapath_id}-{stat.port_no}"
                
                # Calculate throughput and packet rates if we have previous data
                rx_bps = tx_bps = rx_pps = tx_pps = 0
                error_rate = 0
                
                if port_key in self.port_stats_history:
                    prev_stat, prev_time = self.port_stats_history[port_key]
                    time_diff = current_time - prev_time
                    
                    if time_diff > 0:
                        # Calculate bit rates
                        rx_bps = (stat.rx_bytes - prev_stat.rx_bytes) * 8 / time_diff
                        tx_bps = (stat.tx_bytes - prev_stat.tx_bytes) * 8 / time_diff
                        
                        # Calculate packet rates
                        rx_pps = (stat.rx_packets - prev_stat.rx_packets) / time_diff
                        tx_pps = (stat.tx_packets - prev_stat.tx_packets) / time_diff
                
                # Calculate error rate
                total_packets = stat.rx_packets + stat.tx_packets
                total_errors = stat.rx_errors + stat.tx_errors
                if total_packets > 0:
                    error_rate = (total_errors / total_packets) * 100
                
                # Store current stats for next calculation
                self.port_stats_history[port_key] = (stat, current_time)
                
                self.port_writer.writerow([
                    current_time, datapath_id, stat.port_no, stat.rx_packets,
                    stat.tx_packets, stat.rx_bytes, stat.tx_bytes, 
                    stat.rx_errors, stat.tx_errors, rx_bps, tx_bps,
                    rx_pps, tx_pps, error_rate
                ])
            
            self.port_csv.flush()
            
        except Exception as e:
            self.logger.error(f"Error processing port stats: {e}")

    def __del__(self):
        """
        Cleanup when controller stops
        """
        try:
            if hasattr(self, 'flow_csv') and self.flow_csv:
                self.flow_csv.close()
            if hasattr(self, 'port_csv') and self.port_csv:
                self.port_csv.close()
            self.logger.info("CSV files closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing CSV files: {e}")

    def stop(self):
        """
        Graceful shutdown method
        """
        try:
            if hasattr(self, 'monitor_thread'):
                self.monitor_thread.kill()
            self.__del__()
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
