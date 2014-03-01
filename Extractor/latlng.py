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

'''


# connect
db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="schools")

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
			location = info['geometry']['location']
			northeast = info['geometry']['northeast']
			southwest = info['geometry']['southwest']

			location_pos = []

			location_pos.append(location['lat'])
			location_pos.append(location['lng'])

			location_pos.append(northeast['lat'])
			location_pos.append(northeast['lng'])

			location_pos.append(southwest['lat'])
			location_pos.append(southwest['lng'])

			location_pos.append('Y')

			location_pos.append(data['schoolid'])

			cur.execute("UPDATE school SET lat=%s, lng=%s, nelat=%s, nelng=%s, swlat=%s, swlng=%s, superceded=%s WHERE Id = %s", 
    				tuple(location_pos)) 

	else:
		location_pos = []

		location_pos.append('UN')

		location_pos.append(data['schoolid'])

		cur.execute("UPDATE school SET superceded=%s WHERE Id = %s", 
				tuple(location_pos))

#mapper
def processdata(cur, listp):
	key = listp.key

	for data in listp.data:
		schoolname = data['schoolname']
		address = data['address']
		postalcode = data['postalcode']

		getLatLng(cur, data, schoolname, address, postalcode, key) 


 
def dividebactchs(cur, keys, limit):
	no_people = len(keys)

	ranges_list = []

	start = 0

	# execute SQL select statement
	cur.execute("SELECT * FROM school where superceded = 'N' LIMIT " . str(no_people * limit))

	rows = cur.fetchall()

	for i in range(no_people):
		listp = {}
		listp.key = keys[i]

		start = start + i * (limit)
		end = listp.start + (limit)

		listp.data = rows[start:end]

		ranges_list.append(listp)

	## assigning batch process
	## this can be disturbuted to multiple computers to make it faster
	## use hadoop
	## links helpful
	## production http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/
	## development http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/
	## code http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
	for process in ranges_list:
		processdata(cur, process)


