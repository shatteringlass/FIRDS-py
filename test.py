#!/bin/bash
python3 worker.py --wdir ~/firds_tempdata --cleanup xml csv zip --cutoff 2018-04-01T00:00:00Z --hst localhost --dbn postgres --uid postgres --pwd postgres
rm -rf ~/firds_tempdata
