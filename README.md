# postgresql-drop-time-series-table-partitions

More info at [imperialwicket.com](http://imperialwicket.com/postgresql-automating-monthly-table-partitions).

Imperial Wicket's solution to automating the creation of time-based table partitions is fantastic, however there is no capability to automate the deletion of table partitions. This companion function will automatically drop old table partitions created using the standard naming convention used by Imperial Wicket's function.
