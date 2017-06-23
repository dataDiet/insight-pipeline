#!/usr/bin/python

import json
import subprocess
import re

# class file for configuring properties file information

class PropertyConfig:

    def __init__(self):
        self.properties = {}

    def importProperties(self, property_file_path):
        with open(property_file_path, 'r') as properties_lines:
            for line in properties_lines:
                properties_tuple = line.rstrip().split('=')
                self.setProperty(properties_tuple[0], properties_tuple[1])

    def exportProperties(self):
        with open('properties.txt', "w") as file_pointer:
            for key, value in sorted(self.properties.items()):
                file_pointer.write(key + '=' + str(value) + '\n')

    def getShellOutput(self, shell_commands):
        proc = subprocess.Popen(shell_commands, stdout=subprocess.PIPE, shell=False)
        (out, err) = proc.communicate()
        return out.decode("utf-8")

    def updateClusterData(self, shell_output):
        shell_output_list = shell_output.split('\n')
        host_name_list = []
        public_dns_list = []
        for line in shell_output_list:
            host_name_regex_match = re.search('^\s+Hostname:\s+(.*)$', line)
            public_dns_regex_match = re.search('^\s+Public DNS:\s+(.*)$', line)
            if public_dns_regex_match:
                public_dns_list.append(public_dns_regex_match.group(1))
            if host_name_regex_match:
                host_name_list.append(host_name_regex_match.group(1))
        self.setProperty('cluster_node_count', len(host_name_list))
        self.setProperty('host_port_pairs',
                         ",".join([host + ':' + self.getProperty('kafka_ports') for host in host_name_list]))
        self.setProperty('public_dns_names',
                         ",".join(public_dns_list))

    def updateRedshiftData(self, shell_output):
        redshift_config = json.loads(shell_output)["Clusters"][0]
        self.setProperty("rs_host", redshift_config["Endpoint"]["Address"])
        self.setProperty("rs_port", redshift_config["Endpoint"]["Port"])
        self.setProperty("rs_user", redshift_config["MasterUsername"])
        self.setProperty("rs_public_ssh_key", redshift_config["ClusterPublicKey"].replace('\n', ''))
        self.setProperty("rs_node_public_ips",
                         ','.join([node["PublicIPAddress"] for node in redshift_config["ClusterNodes"]]))
        self.setProperty("rs_node_private_ips",
                         ','.join([node["PrivateIPAddress"] for node in redshift_config["ClusterNodes"]]))

    def outputManifest(self):
        manifest = {}
        manifest['entries'] = []
        for dns in self.getProperty('public_dns_names').split(','):
            entry = {}
            entry['mandatory'] = self.getProperty("manifest_not_found_mandatory") in ('True', 'true', 't', 'T')
            entry['username'] = self.getProperty("ec2_user")
            entry['endpoint'] = dns
            entry['command'] = self.getProperty("consumer_code")
            manifest['entries'].append(entry)
        with open("manifest.txt", "w") as file_pointer:
            json.dump(manifest, file_pointer)

    def getProperty(self, property_key):
        return self.properties[property_key]

    def setProperty(self, property_key, value):
        self.properties[property_key] = value
