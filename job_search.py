"""
job_search.py
=============

This script is used by the Crest Website to display all current advertised jobs determined
by a given search keyword that is passed to this script by Ajax/JSON POST via a XML file.

"""
import xml.etree.ElementTree as ET
import re
import cgi, os
import urllib3
import xmltodict
import datetime

# Get HTTP POST values from job search form via AJAX/JSON call
form = cgi.FieldStorage()

if "kw" not in form :
  # If no keywords entered/posted, then will show all jobs
  pkeywords = ""
else:
  pkeywords = form["kw"].value

# =================================================================================================
# FUNCTIONS
# =================================================================================================
# strip_leading_and_trailing_spaces_and_newlines
# - Callback function for removing leading and trailing whitespaces and newlines.
# =================================================================================================
def strip_leading_and_trailing_spaces_and_newlines(pstopword):
  pstopword = pstopword.strip()
  pstopword = pstopword.rstrip()
  return pstopword
# =================================================================================================
# stopwords_check
# - Checks for stopwords used in search string keyword, referencing a stop words string.
# Parameters:
# psearchtext - The search text entered by user
# pstopwordslist - list of stop words to be excluded from any given search string
#
# Returns: List of keywords
# =================================================================================================
def stopwords_check(psearchtext, pstopwordslist):
  # Remove line breaks and spaces from stopwords file
  splitstopwords = pstopwordslist.split("\n")
  stopwordslist = map(strip_leading_and_trailing_spaces_and_newlines, splitstopwords)
  
  # Replace all non-word chars with comma for search text and split into search text list
  pattern = '[0-9\W]'
  tsearchtext = re.sub(pattern, ",", psearchtext)
  searchtextlist = tsearchtext.split(",")

  # Remove stopwords from search text and create keyword list without stopwords
  keywords = []
  for searchitem in searchtextlist:
    if searchitem not in stopwordslist:
      keywords.append(searchitem)
      
  return keywords

# =================================================================================================
# is_matched
# - checks for search string matches within a given string, minus any stop words
# Parameters:
# psearchstring - The search text item entered by user
# pstring - The string to be searched
# =================================================================================================
def is_matched(psearchstring, pstring):
  try:
    # Open stop word file
    f = open("stop_words.txt", "r")
  except:
    print("File exception.")
  # Read file contents into string
  stopwordfilecontents = f.read()

  # Remove stop words from search
  keywords = stopwords_check(psearchstring, stopwordfilecontents)
  # Remove empty last item
  keywords.pop()

  # Check for any occurences of searchstring and return true if found, otherwise false
  for word in keywords:
     if word in pstring:
       return True
  return False  

# =================================================================================================

# Read Job feed XML file
http = urllib3.PoolManager()
xmlurl = 'http://www.somesite.com.au/jobfeed.xml'
response = http.request('GET', xmlurl)
xmldata = xmltodict.parse(response.data)

# Get count of jobs posted via XML feed
num_jobs = len(xmldata['Jobs']['Job'])

matches = 0
rcount = 0
i = 0
lstr = []
sstr = []
dstr = []

# Loop through each job
while i < num_jobs:
  # Assign data
  jid = xmldata['Jobs']['Job'][i]['@jid']
  contact_name = xmldata['Jobs']['Job'][i]['Fields']['Field']['#text']
  contact_email = xmldata['Jobs']['Job'][i]['Apply']['EmailTo']
  # Consultant/recruiter phones not given due to possible spam risk
  contact_telephone = "(+61) 08 9215 6200"
  job_reference = xmldata['Jobs']['Job'][i]['@reference']
  job_title = xmldata['Jobs']['Job'][i]['Title']
  job_type = xmldata['Jobs']['Job'][i]['Classifications']['Classification'][3]['@name']
  job_industry = xmldata['Jobs']['Job'][i]['Classifications']['Classification'][0]['#text']
  job_summary = xmldata['Jobs']['Job'][i]['Summary']
  job_description = xmldata['Jobs']['Job'][i]['Description']
  job_location = xmldata['Jobs']['Job'][i]['Classifications']['Classification'][2]['#text']
  job_postdate = xmldata['Jobs']['Job'][i]['@datePosted']

  # If keywords have a match then allocate job details to array accessible via links
  if pkeywords == "" or not pkeywords or is_matched(pkeywords.upper(), job_title.upper()):
    matches += 1
    tempref = str(job_reference)

    # Create clickable link
    # Large browser window
    lstring =  "<TR onclick='document.getElementById(\"id"+str(i)+"\").style.display=\"block\";' title='"+job_summary+"' style=\"cursor:hand\">"
    lstring += "<TD class='jobref'><B>"+tempref+"</B></TD>"
    lstring += "<TD class='jobtitle'><B>"+job_title+"</B></TD>"
    lstring += "<TD>"+job_location+"</TD>"
    lstring += "<TD>"+job_type+"</TD></TR>"
    lstr.append(lstring)
    # Small Browser (eg. mobile)
    sstring =  "<TR onclick='document.getElementById(\"id"+str(i)+"\").style.display=\"block\";' style=\"cursor:hand\">"
    sstring += "<TD class='jobref'><B>"+tempref+"</B></TD>"
    sstring += "<TD class='jobtitle'><B>"+job_title+" ("+job_type+")</B></TD>"
    sstring += "<TD>"+job_location+"</TD>"
    sstring += "</TR>"
    sstr.append(sstring)

    format_str = '%Y-%m-%d'
    dateposted_obj = datetime.datetime.strptime(job_postdate, format_str)
    formatted_date_posted = dateposted_obj.date()
    formatted_date_posted = formatted_date_posted.strftime('%d %B %Y')

    # Create description display pop up info
    dstring =  '<div id="id'+str(i)+'" class="w3-modal w3-small"><div id="errcont" class="w3-modal-content w3-animate-zoom w3-card-8 ">'
    dstring += '<header class="w3-container w3-red ">'
    dstring += '<span onclick="document.getElementById(\'id'+str(i)+'\').style.display=\'none\'"'
    dstring += 'class="w3-closebtn">&times;</span>'
    dstring += '<h5 class="ctr"><b>'+job_title+'</b></h5>'
    dstring += '</header>'
    dstring += '<div class="w3-container">'
    dstring += "<b>Post Date: </b>"+str(formatted_date_posted)+"<BR>"
    dstring += "<b>Industry: </b>"+job_industry+"<BR>"
    dstring += "<b>Job Type: </b>"+job_type+"<BR>"
    dstring += "<b>Job Refno#: </b>"+tempref+"<BR>"
    dstring += "<b>Job location: </b>"+job_location+" "
    dstring += "<p>"+re.sub('<[^<]+?>', '', job_description)+"</p>"
    dstring += "<b>Contact: </b>"+contact_name
    dstring += "<BR><b>Email: </b><a href=\"mailto:"+contact_email+"\" onmouseover=\"this.href=this.href.replace(/x/g,'');\"><img src=\"mail.png\" title=\"Click to E-Mail\" width=\"30\" height=\"17\"></a>"
    dstring += " "+contact_email
    dstring += "<BR><b>Telephone: </b>"+contact_telephone
    dstring += '<div class="ctr"><button onclick="document.getElementById(\'id'+str(i)+'\').style.display=\'none\'" type="button" class="ctr w3-btn w3-white w3-border w3-border-red w3-round-xlarge w3-ripple"><b>Close</b></button></div>'
    dstring += '<BR></div></div></div>'
    dstr.append(dstring)
    rcount += 1
  i += 1 # Increment and loop back

if matches <= 0:
  print("<b>No job matches found...</b><BR>")
else:
  # For large browser windows
  print("<div class='smalldisplay'>")
  print("<TABLE class='w3-table w3-striped w3-bordered w3-card-4 w3-centered w3-hoverable '  id=\"User_Set\" name=\"User_Set\">")
  print("<thead>")
  print('<TR class="w3-red"><th>Refno</th><th>Job</th><TH>Location</TH><TH>Type</TH></TR>')
  print("</thead>")
  print("<tbody>")
  i = 0
  # Print links
  while i < rcount:
    print(lstr[i])
    i += 1
  print("</tbody></TABLE></div>")

  i = 0
  # Print description display (hidden)
  while i < rcount:
    print(dstr[i])
    i += 1

  # For small (mobile) browser windows
  print("<div class='w3-hide-large w3-hide-medium'>")
  print("<TABLE class='w3-table w3-striped w3-bordered w3-card-4 w3-centered w3-hoverable '  id=\"User_Set\" name=\"User_Set\">")
  print("<thead>")
  print('<TR class="w3-red"><th>Refno</th><th>Job</th><TH>Location</TH></TR>')
  print("</thead>")
  print("<tbody>")
  # Print links
  i = 0
  while i < rcount:
    print(sstr[i])
    i += 1
  print("</tbody></TABLE></div>")

# End script
