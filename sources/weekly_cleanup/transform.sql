DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days';

DELETE FROM sessions WHERE last_active < NOW() - INTERVAL '7 days';