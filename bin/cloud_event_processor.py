#!/usr/bin/python
'''Process CloudStack events and perform secondary functions'''

import argparse
import base64
import logging
import os
import sys
import time

from datetime import date, timedelta

import ConfigParser
import daemon
import MySQLdb

# pylint: disable=import-error,wrong-import-position
# Import plugins
LIB_PATH = os.path.dirname(os.path.realpath(__file__)) + '/plugins'
sys.path.append(LIB_PATH)
import nictool_dns


# Depends on this custom table used to store event states
# CREATE TABLE `cloud_usage_events` (
#   `id` bigint(20) unsigned NOT NULL,
#   `state` int(0) unsigned NOT NULL,
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB;


def parse_arguments():
    '''Parse arguments and options'''
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", "-d", action='store_true', default=False,
                        help="Run as a daemon in the background")
    parser.add_argument("--config", "-c", default="/etc/cloudeventprocessor/event_manager.cfg",
                        help="Config file path")
    parser.add_argument("--debug", action='store_true', default=False,
                        help="Run with debug loglevel")
    args = parser.parse_args()
    return args


class UsageEventMonitor(object):
    '''Worker class'''
    def __init__(self, loglevel, config):
        '''Import config and setup logger'''
        self.config = ConfigParser.RawConfigParser()
        self.config.read(os.path.normpath(config))
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(loglevel)
        formatting = '[%(asctime)s] - %(levelname)s - %(funcName)s[%(lineno)d]: %(message)s'
        formatter = logging.Formatter(formatting)
        fhandler = logging.FileHandler(self.config.get('main', 'logfile'))
        fhandler.setLevel(loglevel)
        fhandler.setFormatter(formatter)
        chandler = logging.StreamHandler()
        chandler.setLevel(loglevel)
        chandler.setFormatter(formatter)
        self.logger.addHandler(fhandler)
        self.logger.addHandler(chandler)

    def process_events(self):
        '''Read DB events and process new entries'''
        self.logger.info("Initializing CloudStack Usage Event Monitor")

        while True:
            dbconn = MySQLdb.connect(
                host=self.config.get('cs', 'db_host'),
                user=self.config.get('cs', 'db_user'),
                passwd=base64.b64decode(self.config.get('cs', 'db_passwd')),
                db='cloud'
            )
            latest = self.latest_events(dbconn)
            event_ids = [str(event[0]) for event in latest if event[1] is None]
            vm_data = []
            if len(event_ids) > 0:
                vm_data = self.collect_vms(dbconn, event_ids)
            for virtm in vm_data:
                self.logger.info("Processing [%s] for %s", virtm[1], virtm[3])
                self.logger.debug(virtm)
                self.process_vm_event(virtm)
                self.complete_event(dbconn, virtm)
            dbconn.close()
            time.sleep(10)

    def latest_events(self, db_conn):
        '''Retrieve the day's events'''
        cur = db_conn.cursor()
        time_period = (date.today() - timedelta(days=1)).isoformat()
        sql = "SELECT usage_event.id, ue.state \
               FROM usage_event LEFT OUTER JOIN %s AS ue \
               ON usage_event.id = ue.id \
               WHERE usage_event.created > '%s' \
               ORDER BY ID" % (self.config.get('cs', 'events_table'), time_period)
        cur.execute(sql)
        events = []
        for row in cur.fetchall():
            events.append(row)
        self.logger.debug(events)
        return events

    def collect_vms(self, db_conn, event_ids):
        '''Query VM info with event resource ids'''
        cur = db_conn.cursor()
        ids = ", ".join(event_ids)
        sql = """SELECT usage_event.id, usage_event.type, usage_event.created, \
               usage_event.resource_name, vm.private_ip_address, vm.instance_name, \
               vm.private_mac_address, vm.data_center_id, guest_os.display_name as "os", \
               host.private_ip_address as "host_ip", host.private_mac_address as "host_mac", \
               networks.network_domain as "network_domain" \
               FROM usage_event, vm_instance as vm, guest_os, host, nics, networks \
               WHERE vm.id = usage_event.resource_id \
               AND usage_event.type IN ('VM.CREATE', 'VM.DESTROY') \
               AND DATE_SUB(CURDATE(),INTERVAL 7 DAY) < usage_event.created \
               AND vm.private_mac_address = nics.mac_address \
               AND nics.network_id = networks.id \
               AND guest_os.id = vm.guest_os_id \
               AND (vm.last_host_id = host.id OR vm.host_id = host.id) \
               AND usage_event.id IN (%s)""" % (ids)
        cur.execute(sql)
        vms = []
        for row in cur.fetchall():
            vms.append(row)
        self.logger.debug(vms)
        return vms
# pylint: disable=line-too-long
# Event Query Return
# [(28928L, 'VM.CREATE', datetime.datetime(2018, 9, 10, 15, 39, 50), 'mynodename001', '10.81.98.155', 'i-3-2852-VM', '06:e7:32:00:0f:96', 1L, 'Other Ubuntu (64-bit)', '10.81.96.6  3', '78:45:c4:fb:26:0a', 'example.domain')]

    def process_vm_event(self, vm_event):
        '''Call the event type's corresponding function'''
        event_map = {
            'VM.CREATE': self.process_create,
            'VM.DESTROY': self.process_destroy
        }
        for event_type, event_func in event_map.iteritems():
            if event_type == vm_event[1]:
                event_func(vm_event)

    def complete_event(self, db_conn, vm_event):
        '''Mark event as complete in custom usage table'''
        self.logger.info("Event processed, changing state for %s[%s] to finished.", vm_event[3], vm_event[1])
        cur = db_conn.cursor()
        sql = "INSERT INTO %s (id, state) VALUES (%d, 1) \
               ON DUPLICATE KEY UPDATE state = 1" % (self.config.get('cs', 'events_table'), int(vm_event[0]))
        cur.execute(sql)
        if cur.rowcount > 0:
            db_conn.commit()
        return

# Customize these for your business/environment with your plugins
    def process_create(self, vm_event):
        '''Call functions for VM create'''
        fqdn = vm_event[3] + '.' + vm_event[11]
        nicdns = nictool_dns.NictoolDNS(self.config, vm_event[4], fqdn)
        nicdns.create_dns()

    def process_destroy(self, vm_event):
        '''Call functions for VM destroy'''
        fqdn = vm_event[3] + '.' + vm_event[11]
        nicdns = nictool_dns.NictoolDNS(self.config, vm_event[4], fqdn)
        nicdns.remove_dns()


def start_daemon(loglevel, config):
    '''Daemonize the worker'''
    with daemon.DaemonContext():
        eventmonitor = UsageEventMonitor(loglevel, config)
        eventmonitor.process_events()


def main():
    '''Parse args & start worker'''
    args = parse_arguments()
    config = args.config
    if args.debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    if args.daemon:
        start_daemon(loglevel, config)
    else:
        eventmonitor = UsageEventMonitor(loglevel, config)
        eventmonitor.process_events()


if __name__ == "__main__":
    main()
