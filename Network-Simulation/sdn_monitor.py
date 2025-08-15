from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet
from ryu.lib import hub
import csv
import time
import datetime

class StatsCollector(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(StatsCollector, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)

        # Metadata for experiment
        self.experiment_id = "exp001"  # Set this per run
        self.topology_id = "fat-tree-v1"
        self.scenario_label = "baseline"  # Update per scenario
        self.run_number = 1  # Update per repeat

        # CSV file for saving stats
        self.csv_file = open("stats.csv", "w", newline="")
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow([
            "experiment_id", "topology_id", "scenario_label", "run_number",
            "timestamp", "dpid", "in_port", "out_port",
            "pkt_count", "byte_count",
            "port_no", "rx_packets", "tx_packets", "rx_bytes", "tx_bytes", "rx_dropped", "tx_dropped",
            "queue_id", "queue_tx_bytes", "queue_tx_packets", "queue_tx_errors"
        ])

    # ----------------- Switch Registration -----------------
    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.info("Register datapath: %016x", datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.info("Unregister datapath: %016x", datapath.id)
                del self.datapaths[datapath.id]

    # ----------------- Periodic Monitor -----------------
    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(10)  # every 10 seconds

    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # Flow stats
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

        # Port stats
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

        # Queue stats
        req = parser.OFPQueueStatsRequest(datapath, 0, ofproto.OFPP_ANY, ofproto.OFPQ_ALL)
        datapath.send_msg(req)

    # ----------------- Flow Stats Reply -----------------
    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        for stat in body:
            if stat.priority != 0:  # ignore table-miss flows
                match = stat.match
                in_port = match.get('in_port', None)
                out_port = stat.instructions[0].actions[0].port if stat.instructions else None
                self.csv_writer.writerow([
                    self.experiment_id, self.topology_id, self.scenario_label, self.run_number,
                    ts, dpid, in_port, out_port,
                    stat.packet_count, stat.byte_count,
                    "", "", "", "", "", "", "", "", "", ""
                ])

    # ----------------- Port Stats Reply -----------------
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        dpid = ev.msg.datapath.id

        for stat in ev.msg.body:
            self.csv_writer.writerow([
                self.experiment_id, self.topology_id, self.scenario_label, self.run_number,
                ts, dpid, "", "",
                "", "",
                stat.port_no, stat.rx_packets, stat.tx_packets,
                stat.rx_bytes, stat.tx_bytes,
                stat.rx_dropped, stat.tx_dropped,
                "", "", "", ""
            ])

    # ----------------- Queue Stats Reply -----------------
    @set_ev_cls(ofp_event.EventOFPQueueStatsReply, MAIN_DISPATCHER)
    def queue_stats_reply_handler(self, ev):
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        dpid = ev.msg.datapath.id

        for stat in ev.msg.body:
            self.csv_writer.writerow([
                self.experiment_id, self.topology_id, self.scenario_label, self.run_number,
                ts, dpid, "", "",
                "", "",
                "", "", "", "", "", "", "",
                stat.queue_id, stat.tx_bytes, stat.tx_packets, stat.tx_errors
            ])

    # ----------------- Basic Learning Switch -----------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        dst = eth.dst
        src = eth.src

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        # learn MAC
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install flow
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
            mod = parser.OFPFlowMod(datapath=datapath, priority=1,
                                    match=match, instructions=inst)
            datapath.send_msg(mod)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
