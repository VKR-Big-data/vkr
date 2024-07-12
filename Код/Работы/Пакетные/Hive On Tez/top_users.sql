SELECT user, COUNT(*) AS message_count
FROM large
GROUP BY user
ORDER BY message_count DESC;