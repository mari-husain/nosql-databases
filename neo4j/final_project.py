# - Put the use case you chose here. Then justify your database choice:
# I chose the Bike-share app use case. I chose Neo4j because in a bike-share app, the data is highly interrelated;
# each bike belongs to someone, is used by someone, and it comes out of a dock at a certain place and goes into a dock at another place;
# every photo is taken by a person of a bike. Such interrelation lends itself well to a graph database, especially since
# I imagine the data a bike-share company would be interested in would be relationship-related data - 
# which kind of bike is getting the most trips? Do people start trips or end trips more at a given station?
# Is there a particular group of people using a particular group of bikes?
# Such queries would be painful to do in a SQL database because it would require many joins, but the data can
# be intuitively accessed using the native-ness of relationships in a graph database.
#
# - Explain what will happen if coffee is spilled on one of the servers in your cluster, causing it to go down.
# per https://neo4j.com/docs/operations-manual/current/clustering/high-availability/architecture/
# If I am using a high-availability cluster with a master instance and one or more slave instances, all instances
# in my cluster will have a full copy of the data stored locally. 
#
# If a slave has failed, the other instances in the cluster will mark that slave as failed, and transactions will no
# longer be pushed to that slave.
#
# If a master has failed, another member will be elected to be master so long as 50% of cluster members are still active.
# The new master will then broadcast its availability to all other members of the cluster. While the new master is
# being selected, no writes will be able to occur. The slave that will be selected will be the slave with the highest
# committed transaction ID and, as a tiebreaker, the lowest ha.server_id value.
#
# - What data is it not ok to lose in your app? What can you do in your commands to mitigate the risk of lost data?
# The data that must be preserved is rentor data, user data, bike data, and trip data, since, assuming users pay the rentors
# based on trips, these entities and the relationships among them are the ones necessary to determine who gets paid
# what. It is okay to lose data on stations and spots and photos and comments, since although these are important to the
# operation of the app, they can individually be re-added at a later date without getting the company into financial or legal
# trouble.
#
# To mitigate the risk of lost data, it would be wise to set up a high-availability cluster with a large number of slaves to allow for failover.
# Trips should be handled atomically - i.e. who took the trip, the date/start time of the trip, the start location, which bike was used, and 
# the actual moving of the bike to the possession of the user from the docking station should all be logged in a single transaction. 
# Similarly, the end time of a trip and the end location and the passing of the bike from the user to the docking station should be logged atomically.
# This way, we don't give someone a bike, and then the database or the operation fails, and we don't know whose bike they're using (and therefore who to 
# pay) - if one action fails, then the entire transaction must fail. You can see this behavior in my start- and end-trip actions.


# driver reference: https://neo4j.com/docs/api/python-driver/1.5/

import datetime
from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "test"))

def make_db():
	with driver.session() as session:
		with session.begin_transaction() as tx:
			tx.run("""
					MATCH (n)
					DETACH DELETE n;
					""")

			tx.run("""
					CREATE (mari:Rentor:User { name: 'Mari' })
					, (salman:User { name: 'Salman' })
					, (zara:User { name: 'Zara' })

					// create Mari's bike
					, (mari)-[:OWNS]->(bike:Bike { id: 0, type: 'Mountain' })

					// create Mari's hipster bike
					, (mari)-[:OWNS]->(hipsterBike:Bike { id: 1, type: 'Fixed-gear' })

					// create Mari's normal bike
					, (mari)-[:OWNS]->(normalBike:Bike { id: 2, type: 'Road' })

					// create a photo, posted by Mari, of Mari's bike
					, (mari)-[:TOOK_PHOTO_OF { path: '/ex/photo1.jpg', date: "2018-05-05 12:38:14" }]->(bike)

					// Create Lakewood station, with two spots
					, (lakewood:Station { name: 'Lakewood' })
					, (lakewood)-[:HAS]->(spot0:Spot { id: 0 })
					, (lakewood)-[:HAS]->(spot1:Spot { id: 1 })

					// Create Highlands Ranch station, with two spots
					, (hr:Station { name: 'Highlands Ranch' })
					, (hr)-[:HAS]->(spot2:Spot { id: 2 })
					, (hr)-[:HAS]->(spot3:Spot { id: 3 })

					// Mari's bike is docked at spot 2 (in HR station)
					, (bike)-[:DOCKED_AT]->(spot2)

					// Mari's normal bike is docked at spot 3 (in HR station)
					, (normalBike)-[:DOCKED_AT]->(spot3)

					// Mari's hipster bike is docked at spot 0 (in Lakewood station)
					, (hipsterBike)-[:DOCKED_AT]->(spot0)

					// Create a trip, taken by Zara on Mari's bike from Lakewood to HR station
					, (zara)-[:TOOK]->(trip1:Trip)-[:USED]->(bike)
					, (trip1)-[:STARTED_AT { on: "2018-05-05 12:38:14"}]->(lakewood)
					, (trip1)-[:ENDED_AT { on: "2018-05-05 12:38:14"}]->(hr)

					// Create a trip, taken by Salman on Mari's bike from HR to Lakewood station
					, (salman)-[:TOOK]->(trip2:Trip)-[:USED]->(bike)
					, (trip2)-[:STARTED_AT { on: "2018-05-05 12:38:14"}]->(hr)
					, (trip2)-[:ENDED_AT { on: "2018-05-05 13:19:20" }]->(lakewood)

					// Create a trip, which Salman is currently taking on Mari's hipster bike from the Lakewood station.
					, (salman)-[:TOOK]->(trip3:Trip)-[:USED]->(hipsterBike)
					, (trip3)-[:STARTED_AT { on: "2018-05-05 13:38:14" }]->(lakewood)

					// Create a review by Salman of Mari's bike
					, (salman)-[:REVIEWED { date: "2018-05-05 14:38:14", rating: 10, comment: "Awesome bike"}]->(bike)

					// Create a photo, posted by Salman, of Mari's bike
					, (salman)-[:TOOK_PHOTO_OF { path: '/ex/photo2.jpg', date: "2018-05-05 14:38:14" }]->(bike);
				""")

def new_user(name):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("CREATE (a:User { name: {name} }) RETURN a.name", name=name):
				print("Successfully created new user: " + record["a.name"])

def bikes_at_station(name):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("""
								MATCH (s:Station)-[:HAS]->(d:Spot)<-[:DOCKED_AT]-(b:Bike)
								WHERE s.name = {name}
			 					RETURN d.id, b.type
			 					ORDER BY d.id
			 					""", name=name):

				print("Spot " + str(record["d.id"]) + " has a " + record["b.type"] + " bike.")

def begin_ride(name, dock):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			record = tx.run("""
							MATCH (b:Bike)-[d:DOCKED_AT]->(s:Spot)<-[:HAS]-(st:Station), (n:User) WHERE s.id = {dock} AND n.name = {name}
							CREATE (n)-[:TOOK]->(t:Trip)-[:USED]->(b)  
									, (t)-[r:STARTED_AT  { on: {date} }]->(st)
							WITH b, d, s, n, st, r
							LIMIT 1
							DELETE d
			 				RETURN b.id, s.id, n.name, st.name, r.on
			 				""", dock=dock, name=name, date="{:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())).single()
			print(record["n.name"] + " has checked out a bike from spot " + str(record["s.id"]) + " at " + record["st.name"] + " station, starting their trip on " + record["r.on"])
			return record["b.id"]

def add_photo(name, bike, path):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("""
								MATCH (n:User), (b:Bike)
								WHERE n.name = {name} AND b.id = {bike}
								CREATE (n)-[p:TOOK_PHOTO_OF { path: {path}, date: {date} }]->(b)
								RETURN n.name, b.id, p.path, p.date
								""", name=name, bike=bike, path=path, date="{:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())):
				print(record["n.name"] + " successfully posted new photo of bike " + str(record["b.id"]) + " on " + record["p.date"] + ": " + record["p.path"])

def get_all_photos(bike):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("""
								MATCH (n)-[p:TOOK_PHOTO_OF]->(b:Bike)
								WHERE b.id = {bike}
								RETURN n.name, b.id, p.path, p.date
								ORDER BY p.date DESC
								""", bike=bike):
				print(record["n.name"] + " took a photo of bike " + str(record["b.id"]) + ": " + record["p.path"] + " (" + record["p.date"] + ")")

def add_review(name, bike, rating, comment):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("""
								MATCH (n:User), (b:Bike)
								WHERE n.name = {name} AND b.id = {bike}
								CREATE (n)-[r:REVIEWED { date: {date}, rating: {rating}, comment: {comment} }]->(b)
								RETURN n.name, b.id, r.rating, r.comment, r.date
								""", name=name, bike=bike, date="{:%Y/%m/%d %H:%M:%S}".format(datetime.datetime.now()), rating=rating, comment=comment):
				print(record["n.name"] + " posted a review of bike " + str(record["b.id"]) + " on " + record["r.date"] + ". Rating: " + str(record["r.rating"]) + "/10, Comment: " + record["r.comment"])

def empty_spots_at_station(name):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("""
								OPTIONAL MATCH (s:Station)-[:HAS]->(d:Spot)
								WHERE s.name = {name}
								WITH d
								WHERE NOT (d)<-[:DOCKED_AT]-()
			 					RETURN d.id
			 					""", name=name):

				print("Spot " + str(record["d.id"]) + " is empty.")

def end_ride(name, bike, dock):
	with driver.session() as session:
		with session.begin_transaction() as tx:
			for record in tx.run("""
								MATCH (s:Spot)<-[:HAS]-(st:Station), (n:User)-[:TOOK]->(t:Trip)-[:USED]->(b:Bike)
								WHERE n.name = {name} AND b.id = {bike} AND s.id = {dock}
								WITH n, t, st, b, s
								ORDER BY t.date DESC LIMIT 1
								CREATE (t)-[r:ENDED_AT {on: {date} }]->(st), (b)-[:DOCKED_AT]->(s)
								RETURN n.name, r.on, st.name, s.id
			 					""", dock=dock, bike=bike, name=name, date="{:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())):
				print(record["n.name"] + " has returned a bike to spot " + str(record["s.id"]) + " at " + record["st.name"] + " station, ending their trip on " + record["r.on"])

# Initialize our DB
make_db()

# Action 1: A new user signs up for an account.
# NOTE: new_user assumes we already know "Martha" is a unique username.
new_user("Martha")

# Action 2: Martha checks which bikes are available at the HR station.
bikes_at_station("Highlands Ranch")

# Action 3: Martha undocks the bike in spot 2, thus beginning her trip.
# NOTE: The spot id would be provided by some sort of hardware in the spot itself (ex. a trigger)
martha_bike = begin_ride("Martha", 2)

# Action 4: Martha snaps a photo of the bike on her ride and uploads it.
add_photo("Martha", martha_bike, "/ex/photo3.jpg")

# Action 5: Martha decides to view all photos of the bike she rode.
# NOTE: the bike Martha wants to get pictures of would be selected (tapped) in the app to get the bike ID
get_all_photos(martha_bike)

# Action 6: Martha decides to leave a review of the bike she rode.
# NOTE: Martha would select the bike in the app to get its ID, and she would provide the rating and the comment
add_review("Martha", martha_bike, 1, "Horrible bike!")

# Action 7: Martha checks which docks are empty at the Lakewood station.
empty_spots_at_station("Lakewood")

# Action 8: Martha returns the bike to dock 1 at the Lakewood station, thus ending her trip.
# NOTE: again, dock ID would be provided by hardware in the dock
end_ride("Martha", martha_bike, 1)

