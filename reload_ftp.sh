ps aux | grep [h]arbor_ftp |awk '{print $2}' |xargs  kill
python3 /home/uwsgi/iharbor/ftpserver/harbor_ftp.py &
