# health-stats
Scripts to go from exported Apple Health data to MySQL

# Database Setup:
Create your MySQL/MariaDB Database, then use the following SQL command to create the needed schema:

CREATE TABLE `health_stats` (
  `datetime` datetime NOT NULL,
  `type` varchar(20) NOT NULL,
  `value` float NOT NULL,
  KEY `type` (`type`,`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

# To run:

```python get_health.py path_to_file_or_zip_archive```
