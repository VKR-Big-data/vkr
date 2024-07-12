SELECT streamer, COUNT(*) AS message_count
FROM large
GROUP BY streamer
ORDER BY message_count DESC;