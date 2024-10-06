FROM bitnami/spark:3.5.1

USER root

WORKDIR /app

RUN apt-get update && apt-get install -y wget curl
RUN apt-get update && apt-get install -y procps

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.231/aws-java-sdk-bundle-1.12.231.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.231.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar
RUN curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar --output /opt/bitnami/spark/jars/jets3t-0.9.4.jar

COPY ./src /app

COPY ./requirements_copy.txt /app/requirements_copy.txt
RUN pip install -r /app/requirements_copy.txt

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

ENTRYPOINT [ "sleep", "infinity" ]