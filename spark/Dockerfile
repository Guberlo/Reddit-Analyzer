FROM openjdk:8-jre

ENV PATH $SPARK_DIR/bin:$PATH
ENV SPARK_VERSION=2.4.5
ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH

ADD setup/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz /opt

RUN apt-get update && apt-get -y install bash python3 python3-pip netcat

ENV PATH $PATH:/usr/local/bin/python3

RUN pip3 install pyspark==2.4.6 numpy elasticsearch pandas nltk
RUN pip3 install -U scikit-learn

ENV PYSPARK_PYTHON=/usr/bin/python3
# Create Sym Link 
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop2.7 ${SPARK_DIR} 

RUN export PYSPARK_PYTHON=/usr/local/bin/python3


ADD dataset /opt/tap/spark/dataset
# Add Python Code
ADD code/*  /opt/tap/
# Add Spark Manager
ADD spark-manager.sh $SPARK_DIR/bin/spark-manager

RUN chmod +x $SPARK_DIR/bin/spark-manager

WORKDIR ${SPARK_DIR}
ENTRYPOINT [ "spark-manager" ]