ALTER TABLE `meta`
	ADD COLUMN `version` BIGINT NULL AFTER `value`;
	

CREATE TABLE `app` (
	`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`meta_id` BIGINT(11) NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_app_meta` (`meta_id`),
	CONSTRAINT `FK_app_meta` FOREIGN KEY (`meta_id`) REFERENCES `meta` (`id`)
);

CREATE TABLE `codec` (
	`type` VARCHAR(50) NOT NULL COMMENT 'json/avro',
	`properties` VARCHAR(5000) NULL DEFAULT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`meta_id` BIGINT(20) NOT NULL,
	PRIMARY KEY (`type`),
	INDEX `FK_codec_meta` (`meta_id`),
	CONSTRAINT `FK_codec_meta` FOREIGN KEY (`meta_id`) REFERENCES `meta` (`id`)
);

CREATE TABLE `endpoint` (
	`id` VARCHAR(50) NOT NULL,
	`type` VARCHAR(500) NOT NULL,
	`host` VARCHAR(500) NULL DEFAULT NULL,
	`port` SMALLINT(6) NULL DEFAULT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`meta_id` BIGINT(20) NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_endpoint_meta` (`meta_id`),
	CONSTRAINT `FK_endpoint_meta` FOREIGN KEY (`meta_id`) REFERENCES `meta` (`id`)
);

CREATE TABLE `server` (
	`id` VARCHAR(50) NOT NULL,
	`host` VARCHAR(500) NOT NULL,
	`port` SMALLINT(6) NOT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`meta_id` BIGINT(20) NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_server_meta` (`meta_id`),
	CONSTRAINT `FK_server_meta` FOREIGN KEY (`meta_id`) REFERENCES `meta` (`id`)
);

CREATE TABLE `storage` (
	`type` VARCHAR(50) NOT NULL,
	`default` BIT(1) NOT NULL,
	`properties` VARCHAR(5000) NOT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`meta_id` BIGINT(20) NOT NULL,
	PRIMARY KEY (`type`),
	INDEX `FK_storage_meta` (`meta_id`),
	CONSTRAINT `FK_storage_meta` FOREIGN KEY (`meta_id`) REFERENCES `meta` (`id`)
);

CREATE TABLE `datasource` (
	`id` VARCHAR(500) NOT NULL,
	`properties` VARCHAR(5000) NOT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`storage_type` VARCHAR(50) NULL DEFAULT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_datasource_storage` (`storage_type`),
	CONSTRAINT `FK_datasource_storage` FOREIGN KEY (`storage_type`) REFERENCES `storage` (`type`)
);

CREATE TABLE `topic` (
	`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(500) NOT NULL,
	`partition_count` SMALLINT(6) NULL DEFAULT '0',
	`storage_type` VARCHAR(50) NULL DEFAULT NULL,
	`description` VARCHAR(5000) NULL DEFAULT NULL,
	`status` VARCHAR(50) NULL DEFAULT NULL,
	`create_time` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`schema_id` BIGINT(20) UNSIGNED NULL DEFAULT NULL,
	`consumer_retry_policy` VARCHAR(500) NULL DEFAULT NULL,
	`create_by` VARCHAR(500) NULL DEFAULT NULL,
	`endpoint_type` VARCHAR(500) NULL DEFAULT NULL,
	`ack_timeout_seconds` INT(11) NULL DEFAULT NULL,
	`codec_type` VARCHAR(50) NULL DEFAULT NULL,
	`other_info` VARCHAR(5000) NULL DEFAULT NULL,
	`storage_partition_size` BIGINT(20) UNSIGNED NULL DEFAULT NULL,
	`resend_partition_size` BIGINT(20) UNSIGNED NULL DEFAULT NULL,
	`storage_partition_count` INT(10) UNSIGNED NULL DEFAULT NULL,
	`properties` VARCHAR(5000) NULL DEFAULT NULL,
	`priority_message_enabled` BIT(1) NULL DEFAULT NULL,
	`meta_id` BIGINT(20) NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_topic_meta` (`meta_id`),
	INDEX `FK_topic_schema` (`schema_id`),
	CONSTRAINT `FK_topic_meta` FOREIGN KEY (`meta_id`) REFERENCES `meta` (`id`)
);

CREATE TABLE `partition` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`read_datasource_id` VARCHAR(500) NOT NULL,
	`write_datasource_id` VARCHAR(500) NOT NULL,
	`endpoint_id` VARCHAR(50) NOT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`topic_id` BIGINT(20) UNSIGNED NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_partition_datasource` (`read_datasource_id`),
	INDEX `FK_partition_datasource_2` (`write_datasource_id`),
	INDEX `FK_partition_endpoint` (`endpoint_id`),
	INDEX `FK_partition_topic` (`topic_id`),
	CONSTRAINT `FK_partition_topic` FOREIGN KEY (`topic_id`) REFERENCES `topic` (`id`),
	CONSTRAINT `FK_partition_datasource` FOREIGN KEY (`read_datasource_id`) REFERENCES `datasource` (`id`),
	CONSTRAINT `FK_partition_datasource_2` FOREIGN KEY (`write_datasource_id`) REFERENCES `datasource` (`id`),
	CONSTRAINT `FK_partition_endpoint` FOREIGN KEY (`endpoint_id`) REFERENCES `endpoint` (`id`)
);

CREATE TABLE `producer` (
	`app_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
	`topic_id` BIGINT(20) UNSIGNED NOT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`app_id`),
	INDEX `FK_producer_topic` (`topic_id`),
	CONSTRAINT `FK_producer_topic` FOREIGN KEY (`topic_id`) REFERENCES `topic` (`id`)
);

CREATE TABLE `consumer_group` (
	`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
	`name` VARCHAR(500) NOT NULL,
	`appids` VARCHAR(500) NULL DEFAULT NULL,
	`retry_policy` VARCHAR(500) NULL DEFAULT NULL,
	`ack_timeout_seconds` INT(11) NULL DEFAULT NULL,
	`ordered_consume` BIT(1) NULL DEFAULT NULL,
	`owner` VARCHAR(500) NULL DEFAULT NULL,
	`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`topic_id` BIGINT(20) UNSIGNED NOT NULL,
	PRIMARY KEY (`id`),
	INDEX `FK_consumer_group_topic` (`topic_id`),
	CONSTRAINT `FK_consumer_group_topic` FOREIGN KEY (`topic_id`) REFERENCES `topic` (`id`)
);