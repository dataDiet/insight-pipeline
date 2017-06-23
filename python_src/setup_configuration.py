#!/usr/bin/python

import os
import config_properties
import boto3


def main():
    # read existing properties file
    PROPERTIES = 'properties.txt'
    property_config = config_properties.PropertyConfig()
    property_config.importProperties(PROPERTIES)
    # create backup of properties file in case of issues
    os.system(' '.join(('cp',
                        PROPERTIES,
                        PROPERTIES + '-old')))
    # obtain public DNS addresses and hostnames of cluster via pegasus output
    property_config.updateClusterData(
        property_config.getShellOutput(["peg", "fetch", property_config.getProperty("cluster_name")]))
    # obtain redshift cluster configuration data
    redshift_schema = property_config.getShellOutput(["aws", "redshift", "describe-clusters"])
    property_config.updateRedshiftData(redshift_schema)
    # upload properties as a new properties.txt file
    property_config.exportProperties()
    # construct manifest for SSH Copy and forward to S3
    property_config.outputManifest()
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('manifest.txt', property_config.getProperty("aws_bucket_manifest"), 'manifest.txt')
    # compile java code
    os.chdir("../")
    os.system("mvn clean compile assembly:single")
    os.chdir("python_src")
    for node_number in range(property_config.getProperty('cluster_node_count')):
        # append redshift publish ssh key to authorized_keys on ec2 nodes
        os.system(' '.join(('echo',
                            "'" + property_config.getProperty('rs_public_ssh_key') + "'",
                            '|',
                            'peg',
                            'sshcmd-node',
                            property_config.getProperty('cluster_name'),
                            str(node_number + 1),
                            "'cat >> /home/" + property_config.getProperty('ec2_user') + "/.ssh/authorized_keys'")))
        # because the java code run from the cluster also uses values from the properties file, send the file data there
        os.system(' '.join((
            'peg',
            'scp',
            'from-local',
            property_config.getProperty('cluster_name'),
            str(node_number + 1),
            PROPERTIES,
            './'
        )))
        # send jar file with java code
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
