WITH user_group_messages AS (
    SELECT 
       lgd.hk_group_id,
        COUNT(DISTINCT hk_user_id) AS cnt_users_in_group_with_messages
	FROM l_groups_dialogs lgd
	INNER JOIN l_user_message USING(hk_message_id)
    GROUP BY lgd.hk_group_id
),
user_group_log AS (
	SELECT 
		luga.hk_group_id, 
		COUNT(DISTINCT hk_user_id) AS cnt_added_users,
		MIN(registration_dt) AS registration_dt
	FROM STV2023100614__DWH.l_user_group_activity luga 
	INNER JOIN STV2023100614__DWH.s_auth_history sah USING(hk_l_user_group_activity)
	INNER JOIN STV2023100614__DWH.h_groups USING(hk_group_id)
	WHERE sah.event='add'
	GROUP BY luga.hk_group_id
	ORDER BY registration_dt
)
SELECT 
	DISTINCT 
	ugm.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages/ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC; 