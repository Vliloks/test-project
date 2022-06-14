FROM hseeberger/scala-sbt:8u312_1.6.2_2.12.15

RUN wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.3-bin-hadoop3.2.tgz && \
    mv spark-3.1.3-bin-hadoop3.2/ /opt/spark 

ENV SPARK_HOME=/opt/spark

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY . .

RUN sbt assembly

CMD spark-submit --num-executors 6  --executor-memory 6G --executor-cores 6 --driver-memory 6G  --class Main /root/target/scala-2.12/test-project-assembly-0.1.jar