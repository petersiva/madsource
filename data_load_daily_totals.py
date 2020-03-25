"""
Script: data_load_daily_totals.py

The purpose of this script is to load timesheet data from the timesheet tables as part
of an ETL process, into a single Daily_Contractors_<batch number> to aid in summary reporting.

"""

import mysql.connector
import datetime

# ==========================================================================
# Function: get_hours_worked
# Calculates the total hours worked for the day, given start and finish hours 
# and minutes and taking into account of breaks.
# ==========================================================================
def get_hours_worked(pstart_hour, pstart_minute, pfinish_hour, pfinish_minute, pbreak_hour, pbreak_minute):
  gross_minutes = 0;
  start_hour = int(pstart_hour)
  start_minute = int(pstart_minute)
  finish_hour = int(pfinish_hour)
  finish_minute = int(pfinish_minute)
  break_hour = int(pbreak_hour)
  break_minute = int(pbreak_minute)

  start_minutes = (60*start_hour) + (1*start_minute)
  finish_minutes = (60*finish_hour) + (1*finish_minute)
  break_min = (60*break_hour) + (1*break_minute)

  if finish_minutes > 0:
    gross_minutes = (finish_minutes - start_minutes) - break_min  
  else:
    gross_minutes = start_minutes

  total_hours = float(gross_minutes / 60)

  if total_hours < 0: 
    total_hours *= -1

  return total_hours
# ==========================================================================

conn_db = mysql.connector.connect(
  host="someserver",
  user="someuser",
  passwd="somepassword",
  database="timesheet"
)

batchcursor = conn_db.cursor()

# Get latest batch number from load_record table
batchcursor.execute("select max(rid) rid from load_record")
dbresult = batchcursor.fetchone()
batch = dbresult[0]

# Increment if batch number exists or else reset to 1
if batch:
  batch += 1
else:
  batch = 1

# Create new entry to load table.
current_date = datetime.datetime.now()
current_date = current_date.strftime("%Y-%m-%d")
tablename = "Daily_Contractors_"+batch
sql="Insert INTO load_record (load_date, load_table, status) VALUES (%s, %s, 'running')"
val = (current_date, tablename)
batchcursor.execute(sql, val)
conn_db.commit()

# Create Daily Contractor hours table.
sql = "CREATE TABLE `"+tablename+"` ("+ "`RID` int(11) unsigned NOT NULL AUTO_INCREMENT, "
sql += "`Job_Number` int(11) unsigned DEFAULT NULL, "
sql += "`TSID` int(11) unsigned DEFAULT NULL, "
sql += "`User_ID` int(11) unsigned DEFAULT NULL, "
sql += "`First_Name` varchar(40) DEFAULT NULL, "
sql += "`Middle_Name` varchar(40) DEFAULT NULL, "
sql += "`Surname` varchar(40) DEFAULT NULL, " 
sql += "`Status` varchar(20) DEFAULT NULL, "
sql += "`Approver_ID` int(11) unsigned DEFAULT NULL, "
sql += "`Approval_Date` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
sql += "`Client_ID` int(11) unsigned DEFAULT NULL, "
sql += "`Client_Name` varchar(40) DEFAULT NULL, "
sql += "`Week_Ending` date DEFAULT NULL, "
sql += "`Project` varchar(50) DEFAULT NULL, "
sql += "`Task` varchar(50) DEFAULT NULL, "
sql += "`Rate_Name` varchar(255) DEFAULT NULL, "
sql += "`Rate` decimal(15,4) DEFAULT '0.00', "
sql += "`CRate` decimal(15,4) DEFAULT '0.00', "
sql += "`Date` date DEFAULT NULL, "
sql += "`Hours` decimal(15,2) DEFAULT NULL, "
sql += "`ML` varchar(20) DEFAULT NULL, "
sql += "PRIMARY KEY (`RID`) "
sql += ") ENGINE=InnoDB DEFAULT CHARSET=latin1; "
client_user_cursor.execute(sql)

# Load list of client<->user details.
batchcursor.execute("select Job_Number, User_ID, First_Name, Middle_Name, Surname, Client_ID, Client_Name from vw_client_user_jobs")
dbresult = batchcursor.fetchall()

# Cycle through each client<->user record ignoring 'skipped' status records
for row in dbresult:
  jobnumber = row [0]
  userid = row [1]
  firstname = row[2]
  midname = row[3]
  surname = row[4]
  clientid = row[5]
  clientname = row[6]
  sql = "select TSID, Approved_By, Approved_Date, Weekday1, Weekday2, Weekday3, Weekday4, Weekday5, Weekday6, Weekday7, Status"
  sql += "from timesheet where Job_Number=%d and Status!='Skipped'" % (jobnumber)
  batchcursor.execute(sql)
  tresult = batchcursor.fetchall()
  # Cycle through each timesheet header
  for trow in tresult:
    tsid = row[0]
    approvedby = trow[1]
    approveddate = row[2]
    weekday1 = trow[3]
    weekday2 = trow[4]
    weekday3 = trow[5]
    weekday4 = trow[6]
    weekday5 = trow[7]
    weekday6 = trow[8]
    weekday7 = trow[9]
    status = trow[10]
    sql = "select Start_Hour1, Start_Minute1, Finish_Hour1, Finish_Minute1, Break_Hours1, Break_Minutes1,"
    sql += "Start_Hour2, Start_Minute2, Finish_Hour2, Finish_Minute2, Break_Hours2, Break_Minutes2,"
    sql += "Start_Hour3, Start_Minute3, Finish_Hour3, Finish_Minute3, Break_Hours3, Break_Minutes3,"
    sql += "Start_Hour4, Start_Minute4, Finish_Hour4, Finish_Minute4, Break_Hours4, Break_Minutes4,"
    sql += "Start_Hour5, Start_Minute5, Finish_Hour5, Finish_Minute5, Break_Hours5, Break_Minutes5,"
    sql += "Start_Hour6, Start_Minute6, Finish_Hour6, Finish_Minute6, Break_Hours6, Break_Minutes6,"
    sql += "Start_Hour7, Start_Minute7, Finish_Hour7, Finish_Minute7, Break_Hours7, Break_Minutes7,"
    sql += "CRate, Rate, Rate_Name, Project, Task, ML"
    sql += " from timesheet_row where TSID=%d and TSID > 5000 " % (tsid)
    batchcursor.execute(sql)
    tsr_result = batchcursor.fetchall()
    # Cycle through timesheet data rows
    for tsrow in tsr_result:
      start_hour1 = tsrow[0]
      start_minute1 = tsrow[1]
      finish_hour1 = tsrow[2]
      finish_minute1 = tsrow[3]
      break_hour1 = tsrow[4]
      break_minute1 = tsrow[5]
      start_hour2 = tsrow[6]
      start_minute2 = tsrow[7]
      finish_hour2 = tsrow[8]
      finish_minute2 = tsrow[9]
      break_hour2 = tsrow[10]
      break_minute2 = tsrow[11]
      start_hour3 = tsrow[12]
      start_minute3 = tsrow[13]
      finish_hour3 = tsrow[14]
      finish_minute3 = tsrow[15]
      break_hour3 = tsrow[16]
      break_minute3 = tsrow[17]
      start_hour4 = tsrow[18]
      start_minute4 = tsrow[19]
      finish_hour4 = tsrow[20]
      finish_minute4 = tsrow[21]
      break_hour4 = tsrow[22]
      break_minute4 = tsrow[23]
      start_hour5 = tsrow[24]
      start_minute5 = tsrow[25]
      finish_hour5 = tsrow[26]
      finish_minute5 = tsrow[27]
      break_hour5 = tsrow[28]
      break_minute5 = tsrow[29]
      start_hour6 = tsrow[30]
      start_minute6 = tsrow[31]
      finish_hour6 = tsrow[32]
      finish_minute6 = tsrow[33]
      break_hour6 = tsrow[34]
      break_minute6 = tsrow[35]
      start_hour7 = tsrow[36]
      start_minute7 = tsrow[37]
      finish_hour7 = tsrow[38]
      finish_minute7 = tsrow[39]
      break_hour7 = tsrow[40]
      break_minute7 = tsrow[41]

      crate = row[42]
      rate = row[43]
      ratename = row[44]
      project = row[45]
      task = row[46]
      myleave = row[47]

      monday_total_hours = get_hours_worked(start_hour1, start_minute1, finish_hour1, finish_minute1, break_hour1, break_minute1)
      tuesday_total_hours = get_hours_worked(start_hour2, start_minute2, finish_hour2, finish_minute2, break_hour2, break_minute2)
      wednesday_total_hours = get_hours_worked(start_hour3, start_minute3, finish_hour3, finish_minute3, break_hour3, break_minute3)
      thursday_total_hours = get_hours_worked(start_hour4, start_minute4, finish_hour4, finish_minute4, break_hour4, break_minute4)
      friday_total_hours = get_hours_worked(start_hour5, start_minute5, finish_hour5, finish_minute5, break_hour5, break_minute5)
      saturday_total_hours = get_hours_worked(start_hour6, start_minute6, finish_hour6, finish_minute6, break_hour6, break_minute6)
      sunday_total_hours = get_hours_worked(start_hour7, start_minute7, finish_hour7, finish_minute7, break_hour7, break_minute7)
      row_total = monday_total_hours + tuesday_total_hours + wednesday_total_hours + thursday_total_hours + friday_total_hours + saturday_total_hours + sunday_total_hours

      # Ensure no empty rows are saved to the Daily Contractor hours table.
      if row_total > 0:
        # Insert Monday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday1, monday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()
        # Insert Tuesday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday2, tuesday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()
        # Insert Wednesday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday3, wednesday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()
        # Insert Thursday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday4, thursday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()
        # Insert Friday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday5, friday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()
        # Insert Saturday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday6, saturday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()
        # Insert Sunday
        sql = 'INSERT INTO '+tablename
        sql += ' (Job_Number, TSID, User_ID, Status, Approver_ID, Approval_Date, First_Name, Middle_Name, Surname, Client_ID, Client_Name, Week_Ending, Project, Task, Rate_Name, Rate, CRate, Date, Hours, ML) '
        sql += 'VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %f, %f, %s, %f, %d)'
        val = (jobnumber, tsid, userid, status, approvedby, approveddate, firstname, midname, surname, clientid, clientname, weekday7, project, task, rate, crate, weekday7, sunday_total_hours, myleave)
        batchcursor.execute(sql, val)
        conn_db.commit()

# Update the batch load status on database to "complete"
sql = "Update load_record set status = 'complete' where rid=%d"
val = (batch)
batchcursor.execute(sql, val)
conn_db.commit()


# End script