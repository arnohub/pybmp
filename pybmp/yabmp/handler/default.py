# Copyright 2015-2017 Cisco Systems, Inc.
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import logging
import sys
import time
import pymysql

from oslo_config import cfg

from yabmp.handler import BaseHandler

CONF = cfg.CONF

LOG = logging.getLogger(__name__)

MSG_PROCESS_OPTS = [
    cfg.StrOpt(
        'write_dir',
        default=os.path.join(os.environ.get('HOME', './'), 'data/bmp/local/msg'),
        help='The BMP messages storage path'),
    cfg.IntOpt(
        'write_msg_max_size',
        default=500,
        help='The Max size of one BMP message file, the unit is MB')
]

db = pymysql.connect(host='*********', user='root', password='**********', port=3306, db='bmp')

class DefaultHandler(BaseHandler):
    """default handler
    """
    def __init__(self):
        super(DefaultHandler, self).__init__()
        CONF.register_cli_opts(MSG_PROCESS_OPTS, group='message')
        self.bgp_peer_dict = dict()

    def init(self):
        if not os.path.exists(CONF.message.write_dir):
            try:
                os.makedirs(CONF.message.write_dir)
                LOG.info('Create message output path: %s', CONF.message.write_dir)
            except Exception as e:
                LOG.error(e, exc_info=True)
                sys.exit()

    def on_connection_made(self, peer_host, peer_port):
        """process for connection made
        """
        file_path = os.path.join(CONF.message.write_dir, peer_host)
# peer host is here! switch device manange-ip
        if not os.path.exists(file_path):
            os.makedirs(file_path)
            LOG.info('Create directory: %s for peer %s', file_path, peer_host)

    def on_connection_lost(self, peer_host, peer_port):
        """process for connection lost
        """
        pass

    def on_message_received(self, peer_host, peer_port, msg, msg_type):
        """process for message received
        """
        if msg_type in [4, 5, 6]:
            return
        peer_ip = msg[0]['addr']
        if peer_ip not in self.bgp_peer_dict:
            self.bgp_peer_dict[peer_ip] = {}
            peer_msg_path = os.path.join(
                os.path.join(CONF.message.write_dir, peer_host), peer_ip)
            if not os.path.exists(peer_msg_path):
                # this peer first come out
                # create a peer path and open the first message file
                os.makedirs(peer_msg_path)
                LOG.info('Create directory for peer: %s' % peer_msg_path)
                msg_file_name = os.path.join(peer_msg_path, '%s.msg' % time.time())
                self.bgp_peer_dict[peer_ip]['msg_seq'] = 1
            else:
                # this peer is not first come out
                # find the latest message file and get the last message sequence number
                file_list = os.listdir(peer_msg_path)
                file_list.sort()
                msg_file_name = os.path.join(peer_msg_path, file_list[-1])
                self.bgp_peer_dict[peer_ip]['msg_seq'] = self.get_last_seq(msg_file_name)
            self.bgp_peer_dict[peer_ip]['file'] = open(msg_file_name, 'a')
        if msg_type == 0:  # route monitoring message
            if int(msg[0]['flags']['L']):
                # pos-policy RIB
                msg_list = [time.time(), self.bgp_peer_dict[peer_ip]['msg_seq'], 130, msg[1], (1, 1)]
            else:
                # pre-policy RIB
                msg_list = [time.time(), self.bgp_peer_dict[peer_ip]['msg_seq'], msg[1][0], msg[1][1], (1, 1)]
            self.bgp_peer_dict[peer_ip]['file'].write(str(msg_list) + '\n')
            self.bgp_peer_dict[peer_ip]['msg_seq'] += 1
            self.bgp_peer_dict[peer_ip]['file'].flush()
            if msg[1][1]['attr'].has_key(2):
                as_path = str(msg[1][1]['attr'][2][0][1])
            else:
                as_path = 'Not exist'
            timestamp = int(msg[0]['time'][0])
            nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            routemonimsg = {
                'peer_address': str(msg[0]['addr']),
                'peer_bgp_id': str(msg[0]['bgpID']),
                'peer_as': str(msg[0]['as']),
                'peer_flags_A': str(msg[0]['flags']['A']),
                'peer_flags_L': str(msg[0]['flags']['L']),
                'peer_flags_V': str(msg[0]['flags']['V']),
                'path_attr_ORIGIN': str(msg[1][1]['attr'].get(1, 'Not exist')),
                'path_attr_AS_PATH': as_path,
                'path_attr_NEXT_HOP': str(msg[1][1]['attr'].get(3, 'Not exist')),
                'path_attr_MED': str(msg[1][1]['attr'].get(4, 'Not exist')),
                'path_attr_LOCAL_PREF': str(msg[1][1]['attr'].get(5, 'Not exist')),
                'path_attr_ATOMIC_AGGREGATE': str(msg[1][1]['attr'].get(6, 'Not exist')),
                'path_attr_AGGREGATOR': str(msg[1][1]['attr'].get(7, 'Not exist')),
                'path_attr_COMMUNITY': str(msg[1][1]['attr'].get(8, 'Not exist')),
                'nlri_withdrawn': str(msg[1][1]['withdraw']),
                'nlri':str(msg[1][1]['nlri']),
                'device_ip': peer_host,
                'timestamp': nowtime
            }
            cursor = db.cursor()
            mysql_table = 'route_monitoring'
            cols = routemonimsg.keys()
            vals = routemonimsg.values()
            sql_statement = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(mysql_table, ','.join(cols),','.join('"' + i + '"' for i in vals))
            cursor.execute(sql_statement)
            db.commit()

        elif msg_type == 1:  # statistic message
            msg_list = [time.time(), self.bgp_peer_dict[peer_ip]['msg_seq'], 129, msg[1], (0, 0)]
            self.bgp_peer_dict[peer_ip]['file'].write(str(msg_list) + '\n')
            self.bgp_peer_dict[peer_ip]['msg_seq'] += 1
            self.bgp_peer_dict[peer_ip]['file'].flush()
            timestamp = int(msg[0]['time'][0])
            nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            statmsg = {
                'peer_address': str(msg[0]['addr']),
                'peer_bgp_id': str(msg[0]['bgpID']),
                'peer_as': str(msg[0]['as']),
                'peer_flags_A': str(msg[0]['flags']['A']),
                'peer_flags_L': str(msg[0]['flags']['L']),
                'peer_flags_V': str(msg[0]['flags']['V']),
                'num_pref_rejected_by_in_policy': str(msg[1].get(0, 'Not exist')),
                'num_dup_prefixes_advertise': str(msg[1].get(1, 'Not exist')),
                'num_dup_withdraws': str(msg[1].get(2, 'Not exist')),
                'num_updates_invalid_CLUSTER_LIST_loop': str(msg[1].get(3, 'Not exist')),
                'num_updates_invalid_AS_PATH_loop': str(msg[1].get(4, 'Not exist')),
                'num_updates_invalid_ORIGINATOR_loop': str(msg[1].get(5, 'Not exist')),
                'num_updates_invalid_AS_CONFED_loop': str(msg[1].get(6, 'Not exist')),
                'num_routes_Adj_RIBs_In': str(msg[1].get(7, 'Not exist')),
                'num_routes_Loc_RIB': str(msg[1].get(8, 'Not exist')),
                'num_routes_per_AFI_SAFI_Adj_RIBs_In': str(msg[1].get(9, 'Not exist')),
                'num_routes_per_AFI_SAFI_Loc_RIB': str(msg[1].get(10, 'Not exist')),
                'num_updates_treat_as_withdraw': str(msg[1].get(11, 'Not exist')),
                'num_prefixes_treat_as_withdraw': str(msg[1].get(12, 'Not exist')),
                'num_dup_updates_msg': str(msg[1].get(13, 'Not exist')),
                'device_ip': peer_host,
                'timestamp': nowtime
            }
            cursor = db.cursor()
            mysql_table = 'statistics_report'
            cols = statmsg.keys()
            vals = statmsg.values()
            sql_statement = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(mysql_table, ','.join(cols), ','.join("'" + i + "'" for i in vals))
            cursor.execute(sql_statement)
            db.commit()

        elif msg_type == 2:  # peer down message
            msg_list = [time.time(), self.bgp_peer_dict[peer_ip]['msg_seq'], 3, msg[1], (0, 0)]
            self.bgp_peer_dict[peer_ip]['file'].write(str(msg_list) + '\n')
            self.bgp_peer_dict[peer_ip]['msg_seq'] += 1
            self.bgp_peer_dict[peer_ip]['file'].flush()
            timestamp = int(msg[0]['time'][0])
            nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            peerdownmsg = {
                'peer_address': str(msg[0]['addr']),
                'peer_bgp_id': str(msg[0]['bgpID']),
                'peer_as': str(msg[0]['as']),
                'peer_flags_A': str(msg[0]['flags']['A']),
                'peer_flags_L': str(msg[0]['flags']['L']),
                'peer_flags_V': str(msg[0]['flags']['V']),
                'peer_down_reason': str(msg[1]),
                'device_ip': peer_host,
                'timestamp': nowtime
            }
            cursor = db.cursor()
            mysql_table = 'peer_down_notification'
            cols = peerdownmsg.keys()
            vals = peerdownmsg.values()
            sql_statement = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(mysql_table, ','.join(cols), ','.join("'" + i + "'" for i in vals))
            cursor.execute(sql_statement)
            db.commit()

        elif msg_type == 3:  # peer up message
            msg_list = [time.time(), self.bgp_peer_dict[peer_ip]['msg_seq'], 1, msg[1]['received_open_msg'], (0, 0)]
            self.bgp_peer_dict[peer_ip]['file'].write(str(msg_list) + '\n')
            self.bgp_peer_dict[peer_ip]['msg_seq'] += 1
            self.bgp_peer_dict[peer_ip]['file'].flush()
            timestamp = int(msg[0]['time'][0])
            nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            peerupmsg = {
                'peer_address': str(msg[0]['addr']),
                'peer_bgp_id': str(msg[0]['bgpID']),
                'peer_as': str(msg[0]['as']),
                'peer_flags_A': str(msg[0]['flags']['A']),
                'peer_flags_L': str(msg[0]['flags']['L']),
                'peer_flags_V': str(msg[0]['flags']['V']),
                'peer_bgp_port': str(msg[1]['remote_port']),
                'peer_hold_time': str(msg[1]['received_open_msg']['hold_time']),
                'peer_capabilities_route_refresh': str(msg[1]['received_open_msg']['capabilities']['route_refresh']),
                'peer_capabilities_four_bytes_as': str(msg[1]['received_open_msg']['capabilities']['four_bytes_as']),
                'local_address': str(msg[1]['local_address']),
                'local_bgp_id': str(msg[1]['sent_open_msg']['bgp_id']),
                'local_as': str(msg[1]['sent_open_msg']['asn']),
                'local_hold_time': str(msg[1]['sent_open_msg']['hold_time']),
                'local_bgp_port': str(msg[1]['local_port']),
                'local_capabilities_route_refresh': str(msg[1]['sent_open_msg']['capabilities']['route_refresh']),
                'local_capabilities_four_bytes_as': str(msg[1]['sent_open_msg']['capabilities']['four_bytes_as']),
                'device_ip': peer_host,
                'timestamp': nowtime
            }
            cursor = db.cursor()
            mysql_table = 'peer_up_notification'
            cols = peerupmsg.keys()
            vals = peerupmsg.values()
            sql_statement = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(mysql_table, ','.join(cols), ','.join("'" + i + "'" for i in vals))
            cursor.execute(sql_statement)
            db.commit()

    @staticmethod
    def get_last_seq(file_name):

        """
        Get the last sequence number in the log file.
        """

        lines_2find = 1
        f = open(file_name, 'rb')
        f.seek(0, 2)  # go to the end of the file
        bytes_in_file = f.tell()
        lines_found, total_bytes_scanned = 0, 0
        while (lines_2find + 1 > lines_found and
                bytes_in_file > total_bytes_scanned):
            byte_block = min(1024, bytes_in_file - total_bytes_scanned)
            f.seek(-(byte_block + total_bytes_scanned), 2)
            total_bytes_scanned += byte_block
            lines_found += f.read(1024).count(b'\n')
        f.seek(-total_bytes_scanned, 2)
        line_list = list(f.readlines())
        last_line = line_list[-lines_2find:][0]
        try:
            last_line = eval(last_line)
            return last_line[1] + 1
        except Exception as e:
            LOG.info(e)
            return 1