# extract_cabled_receiver_data
Program for extracting detections from Vemco VR2W receivers.

Vemco VR2W receivers output a lot of data in their logs, and it can take a lot of time to parse each daily file
for detections over a long period of time. This program takes in the raw log files and puts them in an appropriate
database format.

I only had sample files for three projects to base the parsing function on, so this may not work for all VR2W outputs.

The Jupyter notebook utilizes a folder titled "dbtools" which is not provided, but the only file you really need is
the extract_cabled_receivers.py file, which is provided. Modify the database table name and structure as you see fit.
