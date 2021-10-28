hdfs dfs -rm -r /usr/Tran_Minh_Khoi_18520947/homework/out || : && \

hadoop jar ./target/hadoop-reducejoin-1.0.jar com.hadoop_job1.Job1 \
    /usr/Tran_Minh_Khoi_18520947/homework/in/cust.txt \
    /usr/Tran_Minh_Khoi_18520947/homework/in/trans.txt \
    /usr/Tran_Minh_Khoi_18520947/homework/out && \

hdfs dfs -cat /usr/Tran_Minh_Khoi_18520947/homework/out/job1/part-r-* | hdfs dfs -put - /usr/Tran_Minh_Khoi_18520947/homework/out/job1.txt && \

hadoop jar ./target/hadoop-reducejoin-1.0.jar com.hadoop_job2.Job2 \
    /usr/Tran_Minh_Khoi_18520947/homework/out/job1.txt \
    /usr/Tran_Minh_Khoi_18520947/homework/out && \

hdfs dfs -cat /usr/Tran_Minh_Khoi_18520947/homework/out/job2/part-r-00000
