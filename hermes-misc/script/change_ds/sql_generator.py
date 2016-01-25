#!/usr/bin/env python

def to_offset_message_row_insert_string(row):
    _id = row[0]
    priority = row[1]
    group_id = row[2]
    offset = row[3]
    creation_date = row[4]
    last_modified_date = row[5]
    
    return "({0},{1},{2},{3},'{4}','{5}')".format(_id, priority, group_id, offset, creation_date, last_modified_date) 

def to_offset_resend_row_insert_string(row):
    _id = row[0]
    group_id = row[1]
    last_schedule_date = row[2]
    last_id = row[3]
    creation_date = row[4]
    last_modified_date = row[5]
    
    return "({0},{1},'{2}',{3},'{4}','{5}')".format(_id, group_id, last_schedule_date, last_id, creation_date, last_modified_date)

class SqlGenerator:
    def select_resend_max_id(self, t, p, g):
        return "select max(id) from {0}_{1}_resend_{2};".format(t, p, g)
    
    def select_message_max_id(self, t, p, pr):
        return "select max(id) from {0}_{1}_message_{2};".format(t, p, pr)
    
    def select_offset_message(self, t, p):
        return "select `id`, `priority`, `group_id`, `offset`, `creation_date`, `last_modified_date` from {0}_{1}_offset_message;".format(t, p)
    
    def select_offset_resend(self, t, p):
        return "select `id`, `group_id`, `last_schedule_date`, `last_id`, `creation_date`, `last_modified_date` from {0}_{1}_offset_resend;".format(t, p)
    
    def insert_offset_message(self, t, p, row):
        formatter = "INSERT INTO `{0}_{1}_offset_message` (`id`, `priority`, `group_id`, `offset`, `creation_date`, `last_modified_date`) VALUES {2};"
        return formatter.format(t, p, to_offset_message_row_insert_string(row))

    def insert_offset_resend(self, t, p, row):
        formatter = "INSERT INTO `{0}_{1}_offset_resend` (`id`, `group_id`, `last_schedule_date`, `last_id`, `creation_date`, `last_modified_date`) VALUES {2};"
        return formatter.format(t, p, to_offset_resend_row_insert_string(row))
    
    def create_message_table(self, t, p, pr, max_id, buf, psize):
        formatter = "CREATE TABLE `{0}_{1}_message_{2}` (`id` bigint(11) NOT NULL AUTO_INCREMENT, `producer_ip` varchar(15) NOT NULL DEFAULT '', `producer_id` int(11) NOT NULL, `ref_key` varchar(100) DEFAULT NULL, `attributes` blob, `codec_type` varchar(20) DEFAULT '', `creation_date` datetime NOT NULL, `payload` mediumblob NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB AUTO_INCREMENT={3} DEFAULT CHARSET=utf8 PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN ({4}));"
        return formatter.format(t, p, pr, max_id + buf, max_id + buf + psize)
    
    def create_dead_letter_table(self, t, p, psize):
        formatter = "CREATE TABLE `{0}_{1}_dead_letter` (`id` bigint(11) NOT NULL AUTO_INCREMENT, `producer_ip` varchar(15) NOT NULL DEFAULT '', `producer_id` int(11) NOT NULL, `ref_key` varchar(100) DEFAULT NULL, `attributes` blob, `codec_type` varchar(20) DEFAULT '', `creation_date` datetime NOT NULL, `payload` mediumblob NOT NULL, `dead_date` datetime NOT NULL, `group_id` int(11) DEFAULT NULL, `priority` tinyint(4) NOT NULL, `origin_id` bigint(20) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN ({2}));"
        return formatter.format(t, p, psize)
        
    def create_offset_message(self, t, p):
        formatter = "CREATE TABLE `{0}_{1}_offset_message` (`id` bigint(11) NOT NULL AUTO_INCREMENT, `priority` tinyint(11) NOT NULL, `group_id` int(100) NOT NULL, `offset` bigint(11) NOT NULL, `creation_date` datetime NOT NULL, `last_modified_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        return formatter.format(t, p)
    
    def create_offset_resend(self, t, p):
        formatter = "CREATE TABLE `{0}_{1}_offset_resend` (`id` bigint(11) NOT NULL AUTO_INCREMENT, `group_id` int(30) NOT NULL, `last_schedule_date` datetime NOT NULL, `last_id` bigint(11) NOT NULL, `creation_date` datetime NOT NULL, `last_modified_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        return formatter.format(t, p)
    
    def create_resend_table(self, t, p, g, max_id, buf, psize):
        formatter = "CREATE TABLE `{0}_{1}_resend_{2}` (`id` bigint(11) NOT NULL AUTO_INCREMENT, `producer_ip` varchar(15) NOT NULL DEFAULT '', `producer_id` int(11) NOT NULL, `ref_key` varchar(100) DEFAULT NULL, `attributes` blob, `codec_type` varchar(20) DEFAULT '', `creation_date` datetime NOT NULL, `payload` mediumblob NOT NULL, `schedule_date` datetime NOT NULL, `remaining_retries` tinyint(11) NOT NULL, `priority` tinyint(4) NOT NULL, `origin_id` bigint(20) NOT NULL, PRIMARY KEY (`id`), KEY `schedule_date` (`schedule_date`) ) ENGINE=InnoDB AUTO_INCREMENT={3} DEFAULT CHARSET=utf8 PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN ({4}));"
        return formatter.format(t, p, g, max_id + buf, max_id + buf + psize)

sql_generator = SqlGenerator()
