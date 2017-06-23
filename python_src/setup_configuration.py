#!/usr/bin/python

import os
import config_properties
import boto3


def main():
    PROPERTIES = 'properties.txt'
    property_config = config_properties.PropertyConfig()
    property_config.importProperties(PROPERTIES)
    os.system(' '.join(('cp',
                        PROPERTIES,
                        PROPERTIES + '-old')))
    property_config.updateClusterData(
        property_config.getShellOutput(["peg", "fetch", property_config.getProperty("cluster_name")]))
    redshift_schema = property_config.getShellOutput(["aws", "redshift", "describe-clusters"])
    property_config.updateRedshiftData(redshift_schema)
    property_config.exportProperties()
    property_config.outputManifest()
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('manifest.txt', property_config.getProperty("aws_bucket_manifest"), 'manifest.txt')
    os.chdir("../")
    os.system("mvn clean compile assembly:single")
    os.chdir("python_src")
    for node_number in range(property_config.getProperty('cluster_node_count')):
        os.system(' '.join(('echo',
                            "'" + property_config.getProperty('rs_public_ssh_key') + "'",
                            '|',
                            'peg',
                            'sshcmd-node',
                            property_config.getProperty('cluster_name'),
                            str(node_number + 1),
                            "'cat >> /home/" + property_config.getProperty('ec2_user') + "/.ssh/authorized_keys'")))
        os.system(' '.join((
            'peg',
            'scp',
            'from-local',
            property_config.getProperty('cluster_name'),
            str(node_number + 1),
            PROPERTIES,
            './'
        )))
        os.system(' '.join((
            'peg',
            'scp',
            'from-local',
            property_config.getProperty('cluster_name'),
            str(node_number + 1),
            '../target/insight-pipeline-0.1-jar-with-dependencies.jar',
            './'
        )))


if __name__ == '__main__':
    main()
