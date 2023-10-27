INSERT INTO STV2023100614__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
SELECT distinct
	hash(hu.hk_user_id,hg.hk_group_id) AS hk_l_user_group_activity,
	hu.hk_user_id AS hk_user_id,
	hg.hk_group_id  AS hk_group_id,
	now() AS load_dt,
	's3' AS load_src
from STV2023100614__STAGING.group_log AS gl
LEFT JOIN STV2023100614__DWH.h_users hu  ON hu.user_id=gl.user_id
LEFT JOIN STV2023100614__DWH.h_groups hg  ON hg.group_id=gl.group_id
WHERE hash(hu.hk_user_id,hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity FROM STV2023100614__DWH.l_user_group_activity); 