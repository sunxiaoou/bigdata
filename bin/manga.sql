
DROP TABLE IF EXISTS `fruit`;
SET character_set_client = utf8mb4;
CREATE TABLE `fruit` (
    `fruit_id` smallint unsigned not null,
    `name` varchar(10) not null,
    `price` float not null,
    primary key (`fruit_id`)
) engine = innodb default charset = utf8mb4;

DROP TABLE IF EXISTS `export`;
SET character_set_client = utf8mb4;
CREATE TABLE `export` (
    `export_id` smallint unsigned not null,
    `name` varchar(10) not null,
    primary key (`export_id`)
) engine = innodb default charset = utf8mb4;

DROP TABLE IF EXISTS `form`;
SET character_set_client = utf8mb4;
CREATE TABLE `form` (
    `form_id` smallint unsigned not null,
    `date` date not null,
    `export_id` smallint unsigned not null,
    primary key (`form_id`)
) engine = innodb default charset = utf8mb4;

DROP TABLE IF EXISTS `form_detail`;
SET character_set_client = utf8mb4;
CREATE TABLE `form_detail` (
    `form_id` smallint unsigned not null,
    `fruit_id` smallint unsigned not null,
    `quantity` smallint unsigned not null,
    primary key (`form_id`, `fruit_id`)
) engine = innodb default charset = utf8mb4;


LOCK TABLES `fruit` WRITE;
INSERT INTO `fruit` VALUES
    (101, '香瓜', 800),
    (102, '草莓', 150),
    (103, '苹果', 120),
    (104, '柠檬', 200);
UNLOCK TABLES;

LOCK TABLES `export` WRITE;
INSERT INTO `export` VALUES
    (12, '米纳米王国'),
    (23, '阿尔法帝国'),
    (25, '理陀儿王国');
UNLOCK TABLES;

LOCK TABLES `form` WRITE;
INSERT INTO `form` VALUES
    (1101, '2019-03-05', 12),
    (1102, '2019-03-07', 23),
    (1103, '2019-03-08', 25),
    (1104, '2019-03-10', 12),
    (1105, '2019-03-12', 25);
UNLOCK TABLES;

LOCK TABLES `form_detail` WRITE;
INSERT INTO `form_detail` VALUES
    (1101, 101, 1100),
    (1101, 102, 300),
    (1102, 103, 1300),
    (1103, 104, 500),
    (1104, 101, 2500),
    (1105, 103, 2000),
    (1105, 104, 300);
UNLOCK TABLES;
