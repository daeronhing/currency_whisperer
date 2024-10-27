CREATE SCHEMA currency_whisperer;

USE currency_whisperer;

CREATE TABLE userinfo (
	id int NOT NULL auto_increment,
	username varchar(255),
    chat_id int NOT NULL,
    from_currency varchar(5) NOT NULL,
    to_currency varchar(5) NOT NULL,
    is_active boolean NOT NULL,
    alert_time time,
    activated_datetime datetime,
    deactivated_datetime datetime,
    CONSTRAINT PK_subscription PRIMARY KEY (id, chat_id, from_currency, to_currency)
);

CREATE TABLE history_rate (
	id int NOT NULL auto_increment,
	from_currency varchar(5) NOT NULL,
    to_currency varchar(5) NOT NULL,
    exchange_rate float NOT NULL,
    created_date datetime DEFAULT current_timestamp,
    PRIMARY KEY (id)
);