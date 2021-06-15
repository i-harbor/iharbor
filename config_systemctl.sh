cp /home/uwsgi/iharbor/iharbor.service /usr/lib/systemd/system/ -f
cp /home/uwsgi/iharbor/iharbor_ftp.service /usr/lib/systemd/system/ -f
systemctl daemon-reload
systemctl enable iharbor
systemctl enable iharbor_ftp
