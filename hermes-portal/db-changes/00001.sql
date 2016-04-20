CREATE TABLE `tag` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `group` varchar(25) NOT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=31 DEFAULT CHARSET=utf8;

CREATE TABLE `datasource_tag` (
  `datasource_id` varchar(200) NOT NULL DEFAULT '',
  `tag_id` int(11) NOT NULL,
  PRIMARY KEY (`datasource_id`,`tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `users` (
  `username` varchar(25) NOT NULL,
  `phone` varchar(11) DEFAULT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modifed_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `user_roles` (
  `username` varchar(25) NOT NULL,
  `role_name` varchar(50) NOT NULL,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`username`,`role_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `roles_permissions` (
  `role_name` varchar(50) NOT NULL,
  `permission` varchar(50) NOT NULL,
  PRIMARY KEY (`role_name`,`permission`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

alter table application add column `polished` varchar(4000) DEFAULT NULL after `comment`;

insert into users_roles values ('lxteng', 'admin', current_timestamp, current_timestamp), 
('qingyang', 'admin', current_timestamp, timestamp), 
('song_t', 'admin', current_timestamp, timestamp), 
('jhliang', 'admin', current_timestamp, timestamp);


