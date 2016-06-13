-- require r/pool_of_threads.require
disable_query_log;
select @@thread_handling;
enable_query_log;

