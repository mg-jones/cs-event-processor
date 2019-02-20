'''NicTool DNS handlers'''

import __main__
import base64
import logging

import NicTool


class NictoolDNS(object):
    '''Connexity DNS object and methods'''
    def __init__(self, config, ipaddr, fqdn):
        self.ipaddr = ipaddr
        self.fqdn = fqdn
        self.nictool = NicTool.NicTool(config.get('nictool', 'user'),
                                       base64.b64decode(config.get('nictool', 'password')),
                                       config.get('nictool', 'url'),
                                       config.get('nictool', 'soap'))
        self.logger = logging.getLogger(__main__.__name__)

    # pylint: disable=broad-except
    def create_dns(self):
        '''Create forward and reverse DNS'''
        self.logger.info("Creating DNS entries for %s [%s]", self.fqdn, self.ipaddr)
        try:
            self.nictool.add_forward_and_reverse_records(hostname=self.fqdn, ipaddr=self.ipaddr)
        except Exception, err:
            self.logger.error("Unable to add records: %s", err)

    def remove_dns(self):
        '''Remove forward and reverse DNS'''
        self.logger.info("Removing DNS entries for %s [%s]", self.fqdn, self.ipaddr)
        try:
            self.nictool.delete_forward_and_reverse_records(hostname=self.fqdn, ip=self.ipaddr)
        except Exception, err:
            self.logger.error("Unable to remove records: %s", err)
