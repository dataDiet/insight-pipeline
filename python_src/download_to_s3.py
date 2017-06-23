#!/usr/bin/python

import psycopg2
import os
import config_properties
import boto3


def main():
    # Get postgreSQL database connections set
    PROPERTIES = 'properties.txt'
    property_config = config_properties.PropertyConfig()
    property_config.importProperties(PROPERTIES)
    conn_string = "host = '%s' dbname='%s' user='%s' password='%s'" % (
        property_config.getProperty('pg_host'),
        property_config.getProperty('pg_db'),
        property_config.getProperty('pg_user'),
        property_config.getProperty('pg_pass')
    )

    # acquire relevant tables (will take a while if starting from scracth and then send to s3 upon receiving
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute(
        "select b.relname as tablename from (select attrelid FROM pg_attribute WHERE attname = 'id' group by attrelid) a, pg_class b WHERE a.attrelid=b.oid and b.relname not like '%_pkey' GROUP BY b.relname")
    records = [table[0] for table in cursor.fetchall()]
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(property_config.getProperty('aws_bucket'))
    files_complete = set()
    for object in my_bucket.objects.all():
        files_complete.add(object.key)
    for table in records:
        if table not in files_complete and table != 'tracks':
            try:
                with open(table, 'w') as h:
                    cursor.copy_to(h, "%s.%s" % (settings_dict['pg_schema'], table))
                    os.system("aws s3 mv %s s3://%s/%s" % (table, settings_dict['aws_bucket'], table))
                    print('Table %s successfully copied' % table)

            except:
                print('Unexpected error:', sys.exc_info()[0])
                print('Table %s unsuccessfully copied' % table)
                conn.close()
                conn = psycopg2.connect(conn_string)
                cursor = conn.cursor()
                os.remove(table)


if __name__ == '__main__':
    main()
