create table `customer`
(
    `id`        mediumint unsigned not null auto_increment primary key,
    `firstName` varchar(255) default null,
    `lastName`  varchar(255) default null,
    `birthdate` varchar(255)
) auto_increment = 1;

create table `customer2`
(
    `id`        mediumint unsigned not null auto_increment primary key,
    `firstName` varchar(255) default null,
    `lastName`  varchar(255) default null,
    `birthdate` varchar(255)
) auto_increment = 1;

