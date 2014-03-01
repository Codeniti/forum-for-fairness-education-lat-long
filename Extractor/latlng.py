import urllib2
import json

import MySQLdb


'''
	So in this process data can be given to each computer in u r network and assign a range of numbers 

	which these computers can be finished in x days equally

	for example if there are 5 systems and 1 million address

	each system gets to process 2 lakh queries

	so as google allows 2500 quries per day

	each take 80 days to query whole data

	As data is said to be 3lakh queries

	we suggest 20 systems

	so u can process all data in 6 days

	we can improve this by limiting quries 

'''


# connect
db = MySQLdb.connect(unix_socket = '/Applications/MAMP/tmp/mysql/mysql.sock', host="localhost", port=8890, user="root", passwd="root", db="schools")
db.autocommit(True)

cursor = db.cursor()

#placename = 'hyderabad'

APIKEY = 'AIzaSyAlzIgKDmSBFH3FZh4W9KWqM4NGkksuOdQ'

#mapper

def getLatLng(cur, data, schoolname, address, postalcode, apikey):
	APIKEY = apikey

	address = schoolname + ',' +' Kishan Bagh Rd, Rambagh Colony, Hyderabad, Andhra Pradesh, India'

	postalcode = '500064'

	url = 'https://maps.googleapis.com/maps/api/geocode/json?sensor=true&key=' + APIKEY

	address = address.replace(' ', '+')

	url += '&address=' + address

	url += '&components=postalcode:' + postalcode

	response = urllib2.urlopen(url)

	info = response.read()

	info = json.loads(info)

	if info['status'] == 'OK':
			info = info["results"][0]

			location = info['geometry']['location']

			location_pos = []

			location_pos.append(location['lat'])
			location_pos.append(location['lng'])

			if info['geometry']["viewport"]['northeast']:
				northeast = info['geometry']["viewport"]['northeast']
				location_pos.append(northeast['lat'])
				location_pos.append(northeast['lng'])
			else:
				location_pos.append('NULL')
				location_pos.append('NULL')


			if info['geometry']["viewport"]['southwest']:
				southwest = info['geometry']["viewport"]['southwest']
				location_pos.append(southwest['lat'])
				location_pos.append(southwest['lng'])
			else:
				location_pos.append('NULL')
				location_pos.append('NULL')

			location_pos.append('1')

			location_pos.append(str(data[0]))

			print location_pos

			cur.execute("UPDATE school SET lat=%s, lng=%s, nelat=%s, nelng=%s, swlat=%s, swlng=%s, superceded=%s WHERE schoolid = %s", 
    				tuple(location_pos)) 

	else:
		info = info["results"][0]

		location_pos = []

		location_pos.append('-1')

		location_pos.append(str(data[0]))

		cur.execute("UPDATE school SET superceded=%s WHERE schoolid = %s", 
				tuple(location_pos))

#mapper
def processdata(cur, listp):
	key = listp['key']

	for data in listp['data']:
		schoolname = data[1]
		address = data[2]
		postalcode = data[3]

		getLatLng(cur, data, schoolname, address, postalcode, key) 


 
def dividebactchs(cur, keys, limit):
	no_people = len(keys)

	ranges_list = []

	start = 0

	print 'starting acces'

	# execute SQL select statement
	cur.execute("SELECT * FROM school where superceded = 0 LIMIT " + str(no_people * limit))

	rows = cur.fetchall()

	for i in range(no_people):
		listp = {}
		listp['key'] = keys[i]

		start = start + i * (limit)
		end = start + (limit)

		listp['data'] = rows[start:end]

		ranges_list.append(listp)

	print ranges_list

	## assigning batch process
	## this can be disturbuted to multiple computers to make it faster
	## use hadoop
	## links helpful
	## production http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/
	## development http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/
	## code http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
	for process in ranges_list:
		processdata(cur, process)

keys = [APIKEY]
dividebactchs(cursor, keys, 100)

