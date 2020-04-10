from pathlib import Path
import subprocess
import fnmatch
import json 
import numpy as np
import shutil
import time


import argparse
import os
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join
import pandas as pd 
from pandas.io.json import json_normalize 



startTime = time.time()

#list of checksums to trace duplicates
checkSums = []
#list of duplicated fiile names to be printed 
duplicates = []

#define positional & optional arguments
parser = argparse.ArgumentParser()
parser.add_argument("path", help = "Enter the directory path please")
parser.add_argument("-u", "--unix", action="store_true", dest="isUNIX", default=False, help="Change the time stamp into UNIX format")
args = parser.parse_args()


#get the files from the directory that was passed to the script
files = [item for item in listdir(args.path) if isfile(join(args.path, item ))]

#check for duplicates
for fileName in files:
		if fnmatch.fnmatch(fileName, '*.json'):	
	    # Use Popen to call the md5sum utility
		    with Popen(["md5sum", fileName], stdout=PIPE) as proc:
	    		checkSum, _ = proc.stdout.read().split()
		    	if checkSum in checkSums:
		    		duplicates.append(fileName)
		    		print(f'file {fileName} was ignored because of duplication ..')
	        # if new file then append in checksums list
	        # and transform this file to csv
	    		else:    
	        		checkSums.append(checkSum)
	        		os.chdir(args.path)
	        		records = [json.loads(line) for line in open(fileName)]
	        		df = json_normalize(records)
		        	web_browser =df['a'].str.split('/', n = 5, expand = True)
		        	final = pd.DataFrame(web_browser[0].to_list() , columns = ['web_browser'])
		        	operatingSys = df['a'].str.split(r" \(", n = 5, expand = True)
		        	operatingSys = operatingSys[1].str.split(" ", n = 5, expand = True)
		        	final['operating system'] = operatingSys[0]
		        	to_url = df['u'].str.split("/", n = 5, expand = True)
		        	final['to_url'] = to_url[2]
		        	from_url = df['r'].str.split("/", n = 5, expand = True) 
		        	from_url.reset_index(inplace=True)
		        	final['from_url'] =from_url[2] 
		        	final['city'] =df['cy']
		        	final['longitude'] = df['ll'].str[0]
		        	final['latitude'] = df['ll'].str[1]
		        	final['time_zone'] = df['tz']
		        	final['time_in'] = df['t']
		        	final['time_out'] = df['hc']
		        	final.dropna( inplace = True)
		       		#check if the optional parameter was passed
		       		if args.isUNIX:
		       			print('time is saved in UNIX format')
		       		else:
		       			tempTime_in = []
		       			tempTime_out = []
		       			for i, row in final.iterrows():
		       				tempTime_in.append(pd.to_datetime(row['time_in'], unit='s').tz_localize(row['time_zone']).tz_convert('UTC'))
		       			final['time_in']= tempTime_in

		       			for i, row in final.iterrows():
		       				tempTime_out.append(pd.to_datetime(row['time_out'], unit='s').tz_localize(row['time_zone']).tz_convert('UTC'))
		       			final['time_out']= tempTime_out
		       		newFileNameSplit= os.path.splitext(fileName)
		       		newFileName= newFileNameSplit[0]+".csv"
		       		final.to_csv(newFileName, index = False)
		       		print(f"Number of rows transformed =  {final['city'].count()}")
		       		print(f"File path = {args.path} {newFileName}")
		       		print("Execution =  %s s " % (time.time() - startTime))