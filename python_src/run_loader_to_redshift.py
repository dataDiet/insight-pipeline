#!/usr/bin/python
import psycopg2
import config_properties


def main():
    PROPERTIES = 'properties.txt'
    property_config = config_properties.PropertyConfig()
    property_config.importProperties(PROPERTIES)

    conn_string = "host = '%s' dbname='%s' user='%s' password='%s' port=%s" % (
        property_config.getProperty('rs_host'),
        property_config.getProperty('rs_db'),
        property_config.getProperty('rs_user'),
        property_config.getProperty('rs_pass'),
        property_config.getProperty('rs_port'))

    while (True):
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        query = "copy events from 's3://" + \
                property_config.getProperty("manifest_bucket") + \
                "/manifest.txt' credentials 'aws_access_key_id=" + \
                property_config.getProperty("aws_id") + \
                ";aws_secret_access_key=" + \
                property_config.getProperty("aws_secret") + \
                "' region '" + \
                property_config.getProperty("aws_region") + \
                "' FILLRECORD DELIMITER '|' ssh MAXERROR 100000 TIMEFORMAT as 'auto';"
        cursor.execute(query)
        cursor.execute("SELECT count(*) FROM events")
        print(cursor.fetchone())
        conn.commit()
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
