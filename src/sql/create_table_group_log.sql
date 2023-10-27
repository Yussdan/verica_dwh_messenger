CREATE table STV2023100614__STAGING.group_log(
	group_id INT NOT NULL,
	user_id INT NOT NULL,
	user_id_from INT,
	event VARCHAR(80) NOT NULL,
	"datetime" datetime NOT NULL
);