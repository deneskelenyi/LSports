import sys

import mysql.connector

import config as app_config


def create_connection():
	return app_config.create_mysql_connection()


try:
	mydb = create_connection()
	mydb2 = create_connection()
except mysql.connector.Error as error:
	print("parameterized query failed {}".format(error))
	print("Mysql connect failed")
	sys.exit(2)
