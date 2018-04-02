# Require the httparty gem for making the HTTP GET request
# and the json gem for parsing the JSON response
require 'httparty'
require 'json'


# Set up the url and send a GET request to it. The base url is:
# "https://api.nasa.gov/planetary/apod?api_key=yXt3sHucc2KwFerzkUutz1G3XXdO9NlsWwOr1LmV"
# and the date is specified in YYYY-MM-DD format in the "date" parameter.
api_query = "https://api.nasa.gov/planetary/apod?api_key=yXt3sHucc2KwFerzkUutz1G3XXdO9NlsWwOr1LmV&date=2017-05-03"
response = HTTParty.get(api_query, format: :plain)

# parse the response, which comes in JSON format
response_parsed = JSON.parse response

# Print out the "url" key in the response, which is the image url
puts response_parsed["url"]