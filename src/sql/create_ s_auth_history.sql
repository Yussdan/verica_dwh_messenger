CREATE TABLE STV2023100614__DWH.s_auth_history(
	hk_l_user_group_activity INT REFERENCES STV2023100614__DWH.l_user_group_activity(hk_l_user_group_activity),
	user_id_from INT,
	event VARCHAR(80),
	event_dt datetime,
	load_dt datetime,
	load_src VARCHAR(20)
);

