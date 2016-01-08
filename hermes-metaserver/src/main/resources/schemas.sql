/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

CREATE DATABASE IF NOT EXISTS `fxhermesmetadb` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `fxhermesmetadb`;


CREATE TABLE IF NOT EXISTS `meta` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT 'pk:id',
  `value` text NOT NULL COMMENT 'value',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hello';

CREATE TABLE IF NOT EXISTS `schema` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk:id',
  `name` varchar(500) NOT NULL DEFAULT '-1' COMMENT 'SchemaRegistry里的subject，应为{topic}-value命名',
  `type` varchar(500) NOT NULL DEFAULT '-1' COMMENT 'JSON/AVRO',
  `topicId` bigint(20) NOT NULL DEFAULT '-1' COMMENT 'topicId',
  `version` int(11) unsigned NOT NULL DEFAULT '1' COMMENT '如果是Avro，应该与SchemaRegistry里一致',
  `description` varchar(5000) DEFAULT NULL COMMENT 'description on this schema',
  `compatibility` varchar(50) DEFAULT NULL COMMENT 'NONE, FULL, FORWARD, BACKWARD',
  `create_time` datetime DEFAULT NULL COMMENT 'this schema create time',
  `schema_content` mediumblob COMMENT 'JSON/AVRO 描述文件',
  `schema_properties` text COMMENT 'schema properties',
  `jar_content` mediumblob COMMENT 'JAR 下载使用',
  `jar_properties` text COMMENT 'jar properties',
  `avroid` int(11) unsigned DEFAULT NULL COMMENT '关联到Schema-Registry',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'changed time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='for schema';

CREATE TABLE IF NOT EXISTS `subscription` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'name',
  `topic` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'topic',
  `group` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'group',
  `endpoints` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'endpoint',
  `status` varchar(500) NOT NULL DEFAULT 'null' COMMENT 'status',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='subscription';

CREATE TABLE IF NOT EXISTS `monitor_event` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `event_type` int(11) NOT NULL,
  `create_time` datetime DEFAULT NULL,
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  `key1` varchar(500) DEFAULT NULL,
  `key2` varchar(500) DEFAULT NULL,
  `key3` varchar(500) DEFAULT NULL,
  `key4` varchar(500) DEFAULT NULL,
  `message` longtext,
  `notify_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='monitor_event';

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;

/* Meta Refactor */

ALTER TABLE `meta`
         ADD COLUMN `version` BIGINT NULL DEFAULT NULL COMMENT '版本号' AFTER `DataChange_LastTime`;

CREATE TABLE `app` (
         `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='接入应用，备用'
ENGINE=InnoDB
;
CREATE TABLE `codec` (
         `type` VARCHAR(50) NOT NULL DEFAULT 'json' COMMENT 'json/avro',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT '属性',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`type`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='codec'
ENGINE=InnoDB
;
CREATE TABLE `datasource` (
         `id` VARCHAR(200) NOT NULL DEFAULT 'ds0' COMMENT 'pk',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT '属性',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         `storage_type` VARCHAR(50) NULL DEFAULT NULL COMMENT '存储类型',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='datasource'
ENGINE=InnoDB
;
CREATE TABLE `endpoint` (
         `id` VARCHAR(50) NOT NULL DEFAULT 'br0' COMMENT 'pk',
         `type` VARCHAR(500) NOT NULL DEFAULT 'broker' COMMENT 'type',
         `host` VARCHAR(500) NULL DEFAULT NULL COMMENT 'host',
         `port` SMALLINT(6) NULL DEFAULT NULL COMMENT 'port',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='endpoint'
ENGINE=InnoDB
;
CREATE TABLE `producer` (
         `app_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `topic_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'topic id',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`app_id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='producer'
ENGINE=InnoDB
;
CREATE TABLE `server` (
         `id` VARCHAR(50) NOT NULL DEFAULT 'host1' COMMENT 'pk',
         `host` VARCHAR(500) NOT NULL DEFAULT 'localhost' COMMENT 'host',
         `port` SMALLINT(6) NOT NULL DEFAULT '1248' COMMENT 'port',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='server'
ENGINE=InnoDB
;
CREATE TABLE `storage` (
         `type` VARCHAR(50) NOT NULL DEFAULT 'mysql' COMMENT 'pk',
         `default` BIT(1) NOT NULL DEFAULT b'0' COMMENT '是否缺省',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT '属性',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`type`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='storage'
ENGINE=InnoDB
;
CREATE TABLE `topic` (
         `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `name` VARCHAR(500) NOT NULL DEFAULT 'name' COMMENT 'name',
         `partition_count` SMALLINT(6) NULL DEFAULT '0' COMMENT 'partition_count',
         `storage_type` VARCHAR(50) NULL DEFAULT NULL COMMENT 'storage_type',
         `description` VARCHAR(5000) NULL DEFAULT NULL COMMENT 'description',
         `status` VARCHAR(50) NULL DEFAULT NULL COMMENT 'status',
         `create_time` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'create_time',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         `schema_id` BIGINT(20) UNSIGNED NULL DEFAULT NULL COMMENT 'schema_id',
         `consumer_retry_policy` VARCHAR(500) NULL DEFAULT NULL COMMENT 'consumer_retry_policy',
         `create_by` VARCHAR(500) NULL DEFAULT NULL COMMENT 'create_by',
         `endpoint_type` VARCHAR(500) NULL DEFAULT NULL COMMENT 'endpoint_type',
         `ack_timeout_seconds` INT(11) NULL DEFAULT NULL COMMENT 'ack_timeout_seconds',
         `codec_type` VARCHAR(50) NULL DEFAULT NULL COMMENT 'codec_type',
         `other_info` VARCHAR(5000) NULL DEFAULT NULL COMMENT 'other_info',
         `storage_partition_size` BIGINT(20) UNSIGNED NULL DEFAULT NULL COMMENT 'storage_partition_size',
         `resend_partition_size` BIGINT(20) UNSIGNED NULL DEFAULT NULL COMMENT 'resend_partition_size',
         `storage_partition_count` INT(10) UNSIGNED NULL DEFAULT NULL COMMENT 'storage_partition_count',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT 'properties',
         `priority_message_enabled` BIT(1) NULL DEFAULT NULL COMMENT 'priority_message_enabled',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='topic'
ENGINE=InnoDB
;
CREATE TABLE `partition` (
         `partition_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `topic_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'topic id',
         `id` INT(10) UNSIGNED NOT NULL DEFAULT '1' COMMENT 'start from 1',
         `read_datasource` VARCHAR(500) NOT NULL DEFAULT 'ds0' COMMENT 'read ds',
         `write_datasource` VARCHAR(500) NOT NULL DEFAULT 'ds0' COMMENT 'write ds',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`partition_id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`),
         INDEX `topic_id` (`topic_id`),
         INDEX `id` (`id`)
)
COMMENT='partition'
ENGINE=InnoDB
;
CREATE TABLE `consumer_group` (
         `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `name` VARCHAR(500) NOT NULL DEFAULT 'name' COMMENT 'name',
         `appids` VARCHAR(500) NULL DEFAULT NULL COMMENT '应用id',
         `retry_policy` VARCHAR(500) NULL DEFAULT NULL COMMENT '重试',
         `ack_timeout_seconds` INT(11) NULL DEFAULT NULL COMMENT 'ack超时',
         `ordered_consume` BIT(1) NULL DEFAULT NULL COMMENT '是否有序',
         `owner` VARCHAR(500) NULL DEFAULT NULL COMMENT '负责人',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         `topic_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'topic id',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`),
         INDEX `topic_id` (`topic_id`)
)
COMMENT='consumer_group'
ENGINE=InnoDB
;
