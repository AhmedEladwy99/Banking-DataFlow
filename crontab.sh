#!/bin/bash

# add cron job
echo "0 * * * * /usr/bin/python3 ~/Banking-DataFlow/utils/generator.py" | crontab -
echo "generator job added to run generator.py every hour."


echo "15 * * * * /usr/bin/python3 ~/Banking-DataFlow/workflow.py" | crontab -
echo "workflow job added to run workflow.py every hour."

# check if cron job is added
crontab -l | grep "generator.py"
crontab -l | grep "workflow.py"
