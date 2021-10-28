HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar \
    hadoop com.sun.tools.javac.Main ReduceJoin.java && \
        jar cf out.jar App*.class
