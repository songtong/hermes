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

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
