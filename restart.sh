#!/bin/sh
if ps aux | grep "ipad.py" | grep -v grep ; then
	exit 0
else
	cd /home/olariu/wmfd
	nohup python2.7 ipad.py & 
	exit 0
fi

