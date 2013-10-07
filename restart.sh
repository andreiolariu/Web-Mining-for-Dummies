#!/bin/sh
if ps aux | grep "ipad.py" | grep -v grep ; then
	exit 0
else
	cd /home/andrei/stuff/wmfd
	nohup python ipad.py & 
	exit 0
fi

