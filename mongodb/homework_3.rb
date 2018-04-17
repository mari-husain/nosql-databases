# Assigned genre: Comedy

# require the driver package
require 'mongo'
require 'json'

# tell mongo to shut up
Mongo::Logger.logger.level = Logger::FATAL

# Create a client
client = Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'movies')
movies = client[:movies]

# A. Update all movies with "NOT RATED" at the "rated" key to be "Pending rating". The operation must be in-place and atomic.
result = movies.update_many( { :genres => 'Comedy', :rated => 'NOT RATED' }, { "$set" => { :rated => 'Pending rating' } } )
# puts result.modified_count

# B. Find a movie with your genre in imdb and insert it into your database with the fields listed in the hw description.
result = movies.insert_one( { 	
	:title => 'Blockers', 
	:year => 2018,  
	:countries => [ 'USA' ],
	:genres => [ 'Comedy' ],
	:directors => [ 'Kay Cannon' ],
	:imdb => {
		:id => 41581,
		:rating => 6.6,
		:votes => 6168
	} 
} )

# puts result.n

# C. Use the aggregation framework to find the total number of movies in your genre.
aggregation = movies.aggregate([ 	
	{ 
		'$match' => 
			{ 
				'genres' => 'Comedy' 
			} 
	}, 
	{ 
		'$group' => 
		{ 
			'_id' => 'Comedy', 
			'count' => 
				{ 
					'$sum' => 1 
				} 
		} 
	} 
])

# aggregation.each do |doc|
#   puts doc.to_json()
# end

# Example result:
#  => [{"_id"=>"Comedy", "count"=>14046}]


# D. Use the aggregation framework to find the number of movies made in the country you were born in with a rating of "Pending rating".
aggregation = movies.aggregate([ 	
	{ 
		'$match' => 
			{ 
				'countries' => 'USA', 
				'rated' => 'Pending rating' 
			} 
	}, 
	{ 
		'$group' => 	
			{ 
				'_id' => 
					{ 
						'country' => 'USA', 
						'rating' => 
						'Pending rating' 
					}, 
				'count' => 
					{ 
						'$sum' => 1 
					} 
			} 
	} 
])

# aggregation.each do |doc|
#   puts doc.to_json()
# end

# Example result when country is Hungary:
#  => [{"_id"=>{"country"=>"Hungary", "rating"=>"Pending rating"}, "count"=>9}]


# E. Create an example using the $lookup pipeline operator. See hw description for more info.
client[:songs].drop
client[:songs].create
songs = client[:songs]

client[:comments].drop
client[:comments].create
comments = client[:comments]

songs.insert_many([
	{
		:title => 'Santeria',
		:artist => 'Sublime'
	},
	{
		:title => 'Lay Me Down',
		:artist => 'Dirty Heads'
	},
	{
		:title => 'Mrs. Robinson',
		:artist => 'The Lemonheads'
	}
])

comments.insert_many([
	{
		:song => 'Santeria',
		:comment => 'Great song if you ignore the lyrics.'
	},
	{
		:song => 'Lay Me Down',
		:comment => 'Catchy.'
	},
	{
		:song => 'Mrs. Robinson',
		:comment => 'Good cover of a great song in a terrible movie.'
	},
])

aggregation = songs.aggregate([ 	
	{ 
		'$lookup' =>
			{
				'from' => 'comments',
				'localField' => 'title',
				'foreignField' => 'song',
				'as' => 'maris_comments'
			}
	} 
])

# aggregation.each do |doc|
# 	puts doc.to_json()
# end



