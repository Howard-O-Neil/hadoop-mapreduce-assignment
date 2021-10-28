hdfs dfs -rm -r /usr/Tran_Minh_Khoi_18520947/homework/out || : && \
    hadoop jar ./target/hadoop-reducejoin-1.0.jar com.hadoop_dev.App \
        /usr/Tran_Minh_Khoi_18520947/homework/in/cust.txt \
        /usr/Tran_Minh_Khoi_18520947/homework/in/trans.txt \
        /usr/Tran_Minh_Khoi_18520947/homework/out && \
    hdfs dfs -cat /usr/Tran_Minh_Khoi_18520947/homework/out/part-r-00000
