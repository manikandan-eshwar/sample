#!/usr/bin/python2.7
#
# Manikandan Vellore Muneeswaran (mvellore@asu.edu)
#

import psycopg2
import re
import os
import threading

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='postgres', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def rangepartitionforSort(ratingstablename, numberofpartitions, openconnection, columnName):
		if os.path.isfile(ratingstablename+'range_partitions_exist'):
				print "Already a range partition scheme exists.. Delete the partitions and corresponding metadata files to create partitions with new range..."
		else:
				cur=openconnection.cursor()
				cur.execute('select %s from %s where %s=(select max(%s) from %s);'%(columnName,ratingstablename,columnName,columnName,ratingstablename))
				rangeValue=float(cur.fetchone()[0])/numberofpartitions
				cur=openconnection.cursor()
				startrange=-1
				endrange=rangeValue
				print "Creating Range Partitions......."
				for x in range(0,numberofpartitions):
						rangetableName=ratingstablename+'rangepart'+`x`
						print "Range Partition Table {0}: {1}".format(x,rangetableName)
						cur.execute('Create Table %s as select * from %s where 1=2;' %(rangetableName,ratingstablename))
						openconnection.commit()
						cur.execute('INSERT INTO %s (SELECT *  FROM %s  WHERE %s>%s AND %s<=%s);' %(rangetableName,ratingstablename,columnName,startrange,columnName,endrange))
						openconnection.commit()
						startrange=endrange
						endrange=endrange+rangeValue
				cur.close()
				f=open(ratingstablename+"range_partitions_exist","w")
				txt="partitions:"+`numberofpartitions`
				f.write(txt)
				f.close()
				print 'Range Partition Successful: {0}  partitions based on {2} column  with range value {1} was created.....'.format(numberofpartitions, rangeValue, columnName)

def rangepartitionforJoin(ratingstablename, numberofpartitions, openconnection, columnName, maxVal):
		if os.path.isfile(ratingstablename+'range_partitions_exist'):
				print "Already a range partition scheme exists.. Delete the partitions and corresponding metadata files to create partitions with new range..."
		else:
				cur=openconnection.cursor()
				#cur.execute('select %s from %s where %s=(select max(%s) from %s);'%(columnName,ratingstablename,columnName,columnName,ratingstablename))
				rangeValue=float(maxVal)/numberofpartitions
				#cur=openconnection.cursor()
				startrange=-1
				endrange=rangeValue
				print "Creating Range Partitions......."
				for x in range(0,numberofpartitions):
						rangetableName=ratingstablename+'rangepart'+`x`
						print "Range Partition Table {0}: {1}".format(x,rangetableName)
						cur.execute('Create Table %s as select * from %s where 1=2;' %(rangetableName,ratingstablename))
						openconnection.commit()
						cur.execute('INSERT INTO %s (SELECT *  FROM %s  WHERE %s>%s AND %s<=%s);' %(rangetableName,ratingstablename,columnName,startrange,columnName,endrange))
						openconnection.commit()
						startrange=endrange
						endrange=endrange+rangeValue
				cur.close()
				f=open(ratingstablename+"range_partitions_exist","w")
				txt="partitions:"+`numberofpartitions`
				f.write(txt)
				f.close()
				print 'Range Partition Successful: {0}  partitions based on {2} column  with range value {1} was created.....'.format(numberofpartitions, rangeValue, columnName)


def parallelsort(Table, SortingColumnName, OutputTable, openconnection):
		delete_partitions(Table)
		rangepartitionforSort(Table, 5, openconnection, SortingColumnName)
		cur=openconnection.cursor()
		cur.execute('DROP table if exists %s;'%(OutputTable))
		openconnection.commit()
		t1=threading.Thread(target=sortpartition,args=(Table+'rangepart0',SortingColumnName,openconnection))
		t2=threading.Thread(target=sortpartition,args=(Table+'rangepart1',SortingColumnName,openconnection))
		t3=threading.Thread(target=sortpartition,args=(Table+'rangepart2',SortingColumnName,openconnection))
		t4=threading.Thread(target=sortpartition,args=(Table+'rangepart3',SortingColumnName,openconnection))
		t5=threading.Thread(target=sortpartition,args=(Table+'rangepart4',SortingColumnName,openconnection))
		t1.start()
		t2.start()
		t3.start()
		t4.start()
		t5.start()
		t1.join()
		t2.join()
		t3.join()
		t4.join()
		t5.join()
		cur=openconnection.cursor()
		cur.execute('CREATE table %s as Select * from %s where 1=2;'%(OutputTable,Table))
		openconnection.commit()
		cur.execute('ALTER TABLE %s  add COLUMN "TupleOrder" bigserial;'%(OutputTable))
		for x in range(0,5):
			rangetablename=Table+"rangepart"+`int(x)`
			cur.execute('INSERT into %s (select * from %s);'%(OutputTable,rangetablename))
			openconnection.commit()
		print 'Parallel sort was successful using five threads...Output is stored in {0}'.format(OutputTable)


def sortpartition(partitiontable, SortingColumnName,openconnection):
		cur=openconnection.cursor()
		temp='temp'+partitiontable
		cur.execute('CREATE table %s as Select * from %s where 1=2;'%(temp,partitiontable))
		openconnection.commit()
		cur.execute('INSERT into %s (select * from %s order by %s);'%(temp,partitiontable,SortingColumnName))
		openconnection.commit()
		cur.execute('DROP table %s;'%(partitiontable))
		openconnection.commit()
		cur.execute('ALTER table %s RENAME to %s;'%(temp,partitiontable))
		openconnection.commit()

def paralleljoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
		cur=openconnection.cursor()
		cur.execute('SELECT max(%s) from %s;'%(Table1JoinColumn, InputTable1))
		max1=cur.fetchone()[0]
		cur.execute('SELECT max(%s) from %s;'%(Table2JoinColumn, InputTable2))
		max2=cur.fetchone()[0]
		if(max2>max1):
			max1=max2
		cur.execute('DROP TABLE IF EXISTS %s;'%(OutputTable))
		delete_partitions(InputTable1)
		delete_partitions(InputTable2)
		rangepartitionforJoin(InputTable1, 5,openconnection, Table1JoinColumn, max1)
		rangepartitionforJoin(InputTable2, 5,openconnection, Table2JoinColumn, max1)
		columnList=""
		cur.execute("Select column_name, udt_name from information_schema.columns where table_name='%s';"%(InputTable1))
		for row in cur.fetchall():
			columnList=columnList+"t1_"+str(row[0])+' '+str(row[1])+","
		cur.execute("Select column_name, udt_name from information_schema.columns where table_name='%s';"%(InputTable2))
		i=0
		for row in cur.fetchall():
			if i==0:
				columnList=columnList+"t2_"+str(row[0])+' '+str(row[1])
				i=i+1
			else:
				columnList=columnList+","+"t2_"+str(row[0])+' '+str(row[1])
				i=i+1
		cur.execute('CREATE TABLE %s (%s);'%(OutputTable,columnList))
		openconnection.commit()
		threads=[]
		for x in range(0,5):
			t=threading.Thread(target=localjoin, args=(InputTable1+'rangepart'+`x`, InputTable2+'rangepart'+`x`, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))
			t.start()
			threads.append(t)
		[t.join() for t in threads]
		print 'Parallel Join is successful using five threads...The output is stored in {0}'.format(OutputTable)

def localjoin(Table1Partition, Table2Partition, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
		cur=openconnection.cursor()
		cur.execute('INSERT into %s (SELECT * from %s INNER JOIN %s ON %s.%s=%s.%s);'%(OutputTable, Table1Partition, Table2Partition, Table1Partition, Table1JoinColumn, Table2Partition, Table2JoinColumn))
		openconnection.commit()

def delete_partitions(tableName):
		"""
		Deletes the range and round robin partitions and the corresponding metadata created for partitioning those
		"""
		with getopenconnection() as con:
			cur=con.cursor()
			if os.path.isfile('round_partitions_exist'):
					f=open('round_partitions_exist','r')
					line=f.readline()
					n=line.split(':')[1]
					for x in range(0,int(n)):
						roundtableName='roundpart'+`x`
						cur.execute('Drop table %s;' %(roundtableName,))
						con.commit()
					cur.execute('Drop table robinmeta;')
					con.commit()
					os.remove('round_partitions_exist')
					if os.path.isfile('round_row_id'):
						os.remove('round_row_id')
			if os.path.isfile(tableName+'range_partitions_exist'):
					f=open(tableName+'range_partitions_exist','r')
					line=f.readline()
					n=line.split(':')[1]
					for x in range(0,int(n)):
							rangetableName=tableName+'rangepart'+`x`
							cur.execute('Drop table %s;' %(rangetableName,))
							con.commit()
					os.remove(tableName+'range_partitions_exist')
			cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
		#delete_partitions()
		pass

def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
		try:
				# Use this function to do any set up before creating the DB, if any
				before_db_creation_middleware()

				create_db(DATABASE_NAME)

				# Use this function to do any set up after creating the DB, if any
				after_db_creation_middleware(DATABASE_NAME)

				with getopenconnection() as con:
						# Use this function to do any set up before I starting calling your functions to test, if you want to
						#before_test_script_starts_middleware(con, DATABASE_NAME)
						# Here is where I will start calling your functions to test them. For example,
						#paralleljoin('ratings_1','ratings_2','rating','rating','joinout',con)
						#parallelsort('Ratings','rating','SortedTable',con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
						after_test_script_ends_middleware(con, DATABASE_NAME)
		except Exception as detail:
						print "OOPS! This is the error ==> ", detail
