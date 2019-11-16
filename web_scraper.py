#Import the libs to be used in the script
import urllib.request
import os

#Grabs the website, parses it, then splits it by content container.
page = urllib.request.urlopen('http://insideairbnb.com/get-the-data.html')
content = page.read().decode('utf-8').split('<div class="contentContainer">')
#Only care about the content (different headers for cities
cities = content[1].split('<h2>')
count = 0
download_files = []
paths = []
#Start with the first city (everything before the first <h2> is not a city
#If the city after the split has at least one class="archived" go ahead
#and parse out the lines and grab the link
for city in cities[1:]:
    archived = city.split('<tr class="archived">')
    if len(archived) >= 2:
        #Change which line to pick whic you'd like to have
        #Order goes from 1 to 3 for data:
        #listings, calendar, reviews
        listing = archived[1].split('\n')
        review = archived[3].split('\n')
        #Separate out by the " on the href
        listingurl = listing[3].split('"')
        reviewurl = review[3].split('"')
        #Add the link to the list
        download_files.append(listingurl[1])
        download_files.append(reviewurl[1])
        #Grab everything after the '.com' in order to have a unique filepath for each file
        paths.append(listingurl[1].split('.com')[1])
        paths.append(reviewurl[1].split('.com')[1])
        #Add one to count and skip to the next city
        count += 1
    continue

#Folder to add all the files/folders onto
folder = './csv_files'

#Checks if the filename does not exist, if it doesn't make the path
#Then create the file within the specified path
for link,path in zip(download_files, paths):
    print("File:\t" + folder + path + "\n")
    if not os.path.exists(os.path.dirname(folder + path)):
        os.makedirs(os.path.dirname(folder + path), exist_ok=True)
    urllib.request.urlretrieve(link, folder + path)
#Print the number of files grabbed.
print("\n" + str(count))
