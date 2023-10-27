CREATE TABLE STV2023100614__DWH.l_user_group_activity(
	hk_l_user_group_activity INT PRIMARY KEY,
	hk_user_id INT REFERENCES STV2023100614__DWH.h_users(hk_user_id),
	hk_group_id INT REFERENCES STV2023100614__DWH.h_groups(hk_group_id),
	load_dt datetime,
	load_src VARCHAR(20)
);