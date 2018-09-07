SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for mq_help
-- ----------------------------
DROP TABLE IF EXISTS `mq_help`;
CREATE TABLE `mq_help` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `next_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for mq_store
-- ----------------------------
DROP TABLE IF EXISTS `mq_store`;
CREATE TABLE `mq_store` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `status` tinyint(5) NOT NULL DEFAULT '0' COMMENT '0 待执行, 1 执行完成, 2 执行中',
  `topic` varchar(255) NOT NULL,
  `payload` blob,
  `version` bigint(20) unsigned NOT NULL,
  `start_time` bigint(20) unsigned DEFAULT NULL,
  `end_time` bigint(20) unsigned DEFAULT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
