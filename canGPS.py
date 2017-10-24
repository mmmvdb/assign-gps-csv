import csv
from datetime import datetime as dt

# For a while, before opening the example csv, I was assuming I'd have to create objects to store GPS and CAN data, then create
# a loader to load them both.  After doing so I could use the objects to gather the data requested in the assignment and be done.
# However the data I am looking at is huge, so it is probably better from a performance/memory standpoint to do the processing
# on each line and print our requested results at the end.  The tradeoff here is that makes this script fairly single serving, just
# doing what is requested and is not very reusable, but should perform fairly well.






# In order to read the CSV line by line and process each row, I'll set up a function to iterate across the file.
def getLine(filename):
    with open(filename, 'r') as csvfile:
        filereader = csv.DictReader(csvfile)
        
        for row in filereader:
            yield row


            
            
# I was doing a test of the big example CSV to make sure that it is ordered by ts.  If it is not, then I have problems
# with the below logic, but it appears as I can rely on this here for this example csv at least.
#oldDate = dt.datetime(1000, 1, 1, 0, 0, 0)
#print(oldDate)



gpsCount = 0      # Running total of gps messages in the iteration
canCount = 0      # Running total of can messages in the iteration
curGPSMessage = 0 # Since GPS messages have following rows that are cans tied to it, this is the last GPS I saw


firstRow = True   # Identifies the first time I am iterating so I can store the begin ts

# Dictionaries to handle the grouping of CANS, GPS, and TS groups I need 
canMessages = {}
gpsMessages = {}
tsMessages = {}


# I made a smaller version of the example.csv for some eye checking/testing (proof of correctness?)
# of some of this before I ran it through the bigger CSV. As each run took on the order of 20ish seconds or so.  
# I just toggled these two lines.
#for row in getLine('gps_can_data_small.csv'):
for row in getLine('gps_can_data.csv'):
    # DEVNOTE: Ordering of timestamp, gps and can, is important here.  I had to have the currentRowTS so that we can use it 
    #          for tsMessages' key. And I do work to store off the curGPSMessage in when we find a gpsID so that we can group 
    #          under the same gps.
    
    
    # I use the messageID and gpsID from the row a lot, so I save them off for use later in the for loop
    # I need a count of each type of message.  Luckilly I can just check message_id
    # and gps_id for this
    messageID = row["message_id"]
    gpsID = row["gps_id"]

    
    # Since I determined with the test below that the whole file is ordered by ts
    if firstRow:
        # I have to worry about if the days and months are zero padded and am unsure about that with the example
        # These also raise valueExceptions but that doesn't seem to be a problem for this CSV if it was, I could try catch around
        # these statments and handle the exception
        startTS = dt.strptime(row["ts"], '%Y-%m-%d %H:%M:%S')
        firstRow = False
    
    # I have to worry about if the days and months are zero padded and am unsure about that with the example
    # These also raise valueExceptions but that doesn't seem to be a problem for this CSV if it was, I could try catch around
    # these statments and handle the exception
    # I use the current row's ts as a key to group ts messages, but it doubles as my end ts for the run time
    currentRowTS = dt.strptime(row["ts"], '%Y-%m-%d %H:%M:%S')
    
    
    if gpsID:
        # Set up a dictionary with the gpsID and a list of corrisponding CAN messages for the GPS average calculation 
        # I need.
        curGPSMessage = gpsID
        gpsMessages[curGPSMessage] = 0
        
        # Tally the GPS count
        gpsCount += 1
        
    if messageID:
        # Tally the CAN count
        canCount += 1
        
        # Also store the message so I can determine unique message count
        # This might be overkill for gathering the unique CAN Messages, but I'll just lean on a python Dict to do this for me.
        if messageID in canMessages:
            canMessages[messageID] += 1
        else:
            canMessages[messageID] = 1
        
        # I found a CAN under the current GPS, so increment that
        gpsMessages[curGPSMessage] += 1

        # I found a CAN under the current TS, so increment that
        # I'm just using the string representation of the ts here, there might be better options for doing this
        tsString = currentRowTS.strftime('%Y-%m-%d %H:%M:%S')
        if tsString in tsMessages:
            tsMessages[tsString] += 1
        else:
            tsMessages[tsString] = 0
        
    
    # Part of the ts ordering test of the big CSV file
    # The fact some timestamps are equal shouldn't be a problem here
    #currentDate = dt.strptime(row["ts"], '%Y-%m-%d %H:%M:%S')
    #if oldDate > currentDate:
    #    print("%s : Bigger than : %s" % (oldDate, currentDate))
    
    #oldDate = currentDate
    
runTime = currentRowTS - startTS

# It may be possible to use a better method of working with the CSV than my current solution of creating dictionaries to iterate 
# across (something I was trying to avoid by doing this iterativly rather than loading into objects).  
# Pandas might be an option but I would need to do some research to learn how to use that module, so at the moment
# I'm going for a "good enough" approach

# Find the gps Average count of CANS
total = 0
for key, value in gpsMessages.items():
    total += value

gpsAvg = total / len(gpsMessages)


# Find the min/max CANS per ts and the average CANS per ts
total = 0

firstEle = True
minTSVal = 0
maxTSVal = 0
minTS = ""
maxTS = ""

for key, value in tsMessages.items():
    # rather than loop twice over this, let's find the min and max as well as store their positions.  I already know that the
    # messages are in ts order, so the first time I find a min or a max, that is the value that is requested of us

    # Make the min and max the first element, compare from there.
    if firstEle:
        firstEle = False
        
        minTSVal = value
        maxTSVal = value
        minTS = key
        maxTS = key
    else:
        if value < minTSVal:
            minTSVal = value
            minTS = key
        
        if value > maxTSVal:
            maxTSVal = value
            maxTS = key
    
    total += value
    

tsAvg = total / len(tsMessages)
    
print("Number of GPS Messages: %i" % (gpsCount))
print("Number of CAN Messages: %i" % (canCount))
print("Number of Unique CAN Messages: %i" % (len(canMessages)))
print("Total Runtime: %s" % (runTime))
print("Average Number of CANs per GPS: %f" % (gpsAvg))
print("Average Number of CANs per ts: %f" % (tsAvg))
print("First Timestamp with most CAN messages (at %i): %s" % (maxTSVal, maxTS))
print("First Timestamp with least CAN messages (at %i): %s" % (minTSVal, minTS))
