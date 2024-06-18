-- https://clickhouse.com/docs/zh/sql-reference/statements/drop
-- #1 CREATE DATABASE
CREATE DATABASE IF NOT EXISTS `dev-orm`;
CREATE DATABASE `dev-orm-mysql` ENGINE = MySQL('10.123.27.170:3306', 'dev-orm', 'root', '123456');

-- drop db
DROP DATABASE IF EXISTS `dev-orm`;

use `dev-orm`;

-- #2 create table
CREATE TABLE IF NOT EXISTS `User`
(
    `id` Int32,
    `name` Nullable(String),
    `birth` Nullable(DateTime),
    `fatherId` Nullable(Int32),
    `motherId` Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY `id`;

-- #3 insert
insert into `User`(`id`,`name`,`birth`,`fatherId`,`motherId`) values(1,'u1',null,4,6);


-- #4 delete
ALTER TABLE `User` DELETE WHERE  id in ( 7 ) ;
