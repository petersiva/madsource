"""
processemail.py
===============

This script reads from the database all data required for sending email notifications
based on data from email tables.

The notifications currently cater for the timesheet system and Document/certificate expiry notifications.

"""
import mysql.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import time
import datetime

# Open Database Connection
tsdb = mysql.connector.connect(
  host="someserver",
  user="someuser",
  passwd="somepassword",
  database="timesheet"
)
tscursor = tsdb.cursor()

# =================================================
# Email Funtion
# =================================================
def Send_Email(pfrom,pfromname, pto, psubject, pbody):
  # Connect to email server
  s = smtplib.SMTP(host='smtp.jpl.com.au', port=25)
  # Login to email server.
  s.login("peter@jpl.com.au", "Priyani1720#")
  # Construct message
  msg = MIMEMultipart()
  msg['From']=pfrom
  msg['To']=pto
  msg['Subject']=psubject
  message=pbody
  # add message body
  msg.attach(MIMEText(message, 'html'))

  # attach image logo
  fp = open('crestlogo.png', 'rb')
  msgImage = MIMEImage(fp.read())
  fp.close()
  # Define image ID
  msgImage.add_header('Content-ID', '<image1>')
  msg.attach(msgImage)

  # send the message via the server set up earlier.
  s.send_message(msg)

# =================================================
# Log Email function
# =================================================
def Log_Email(pmessage):
  userid=0
  username="Mailer"
  system="Process Email Queue"
  ts = time.time()
  log_date = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
  sql = "INSERT INTO timesheet.Logging (User_ID, User_Name, Log_Date, System, Message) VALUES (%s, %s, %s, %s, %s) "
  log_record = (userid, username, log_date, system, pmessage)
  tscursor.execute(sql, log_record)
  tsdb.commit()

# =================================================

Log_Email("Email queue scan Starting..")

# Read first 5 records from email queue table.
tscursor.execute("select From_Address, To_Address, Subject, Body, consemail, Q_ID from email_queue limit 5")

email_record_result = tscursor.fetchall()

Log_Email("Checking Notification Emails..")
# Loop through all email db queue records
for row in email_record_result:
   from_address = row[0]
   to_address = row[1]
   from_name = ''
   subject = row[2]
   body = row[3]
   recruiter_email = row[4]
   q_id = row[5]

   # If email has recruiter tracking, then send receipt email to recruiter
   if recruiter_email:
     Log_Email("Sending tracking email for recruiter:" + str(to_address))
     Send_Email(from_address, from_name, recruiter_email, subject, body)

   # Send Email
   Log_Email("Sending email to :" + str(to_address) + " from: " + str(from_address) + " Subject: " + str(subject))
   Send_Email(from_address, from_name, to_address, subject, body)

   # Remove email record from DB queue table
   sql = "DELETE from email_queue where Q_ID=%s"
   tscursor.execute(sql, (q_id,))
   tsdb.commit()
   Log_Email("Email deleted from queue.")

# Check for expired Certificates and other documents (one at a time to prevent overloading Exchange server)
tscursor.execute("SELECT certid, name, cert, exp FROM certificate WHERE now() >= exp and expired = 0 limit 1")
cert_record_result = tscursor.fetchall()

Log_Email("Check for Expired Certificates and other Documents..")
for row in cert_record_result:
   cert_id = row[0]
   owner_name = row[1]
   certificate = row[2]
   expiry = row[3]
   from_name = ''
   subjectstr = "WARNING: Certificate ({}) Has Expired for {} (Expriy:{})"
   subject = subjectstr.format(certificate, owner_name, expiry)
   body = "Please be advised."
   Log_Email(subject)
   from_address = "noreply@crestpersonnel.com.au"
   to_address = "office@crestlabour.com.au"
   # Send Certificate warning email
   Send_Email(from_address, from_name, to_address, subject, body)
   # Update expired status
   sql = "UPDATE certificate SET expired = 1 WHERE certid=%s  "
   tscursor.execute(sql, (cert_id,))
   tsdb.commit()

# Check for Pending (14 days from expiry) expired Certificates and other documents (one at a time to prevent overloading Exchange server)
tscursor.execute("SELECT * FROM certificate WHERE now() >= DATE_SUB(exp, INTERVAL 14 DAY) and warn=0 limit 1")
cert_record_result = tscursor.fetchall()

for row in cert_record_result:
   cert_id = row[0]
   owner_name = row[1]
   certificate = row[2]
   expiry = row[3]
   from_name = ''
   subjectstr = "WARNING: Certificate ({}) is about to expire for {} (Expriy:{})"
   subject = subjectstr.format(certificate, owner_name, expiry)
   body = "Please be advised."
   Log_Email(subject)
   from_address = "noreply@crestpersonnel.com.au"
   to_address = "office@crestlabour.com.au"
   # Send Certificate warning email
   Send_Email(from_address, from_name, to_address, subject, body)
   # Update expired status
   sql = "UPDATE certificate SET warn = 1 WHERE certid=%s  "
   tscursor.execute(sql, (cert_id,))
   tsdb.commit()

Log_Email("Email queue scan complete.")


# End script