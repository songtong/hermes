NonProd Jenkins: http://10.3.6.90:9136/
Prod    Jenkins: http://10.8.113.220:8080/

These zips are built by Jenkins plugin: ThinBackup.


To Restore:
In Jenkins, System Management -> ThinBackup(if not shown, install this plugin first. 
Then configure the backup file folder.) -> Restore

To Backup:
Now we need to copy the zip file into git repository manually and periodically.



