import psycopg2
import os
from time import sleep
import sys
import boto3

def main():
	# import settings
	settings_dict = {}
	with open('settings.txt','r') as settings_lines:
		for line in settings_lines:
			setting = line.rstrip().split('=')
			settings_dict[setting[0]]=setting[1]
			print(setting)

	conn_string = "host = '%s' dbname='%s' user='%s' password='%s'" % (settings_dict['pg_host'],
		settings_dict['pg_db'],
		settings_dict['pg_user'],
		settings_dict['pg_pass']
		)

	# acquire relevant tables
	conn = psycopg2.connect(conn_string)
	cursor=conn.cursor()
	cursor.execute("select b.relname as tablename from (select attrelid FROM pg_attribute WHERE attname = 'id' group by attrelid) a, pg_class b WHERE a.attrelid=b.oid and b.relname not like '%_pkey' GROUP BY b.relname")
	records=[table[0] for table in cursor.fetchall()]
	s3 = boto3.resource('s3')
	my_bucket = s3.Bucket(settings_dict['aws_bucket'])
	files_complete=set()
	for object in my_bucket.objects.all():
		files_complete.add(object.key)
	for table in records:
		if table not in files_complete and table !='tracks':
			try:	
				with open(table,'w') as h:
					cursor.copy_to(h,"%s.%s" % (settings_dict['pg_schema'],table))
					os.system("aws s3 mv %s s3://%s/%s" % (table,settings_dict['aws_bucket'],table))
					print('Table '+table+' successfully copied')
			
			except:
				print('Unexpected error:',sys.exc_info()[0])
				print('Table '+table+' unsuccessfully copied')
				conn.close()
				conn = psycopg2.connect(conn_string)
				cursor=conn.cursor()
				os.remove(table)
if __name__=="__main__":
	main()
