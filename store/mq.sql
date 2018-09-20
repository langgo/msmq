SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for mq
-- ----------------------------
DROP TABLE IF EXISTS `mq`;
CREATE TABLE `mq` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0 待执行, 1 执行中, 2 执行完成',
  `topic` varchar(255) NOT NULL,
  `payload` blob NOT NULL,
  `create_time` datetime NOT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=31003 DEFAULT CHARSET=utf8;
