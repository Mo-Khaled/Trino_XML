FROM scb-quay-quay-enterprise.apps.ocp-hub.scb.local/scbhq/spark:3.5.5

# Download ojdbc8
RUN curl -L -o /opt/spark/jars/ojdbc8.jar https://download.oracle.com/otn-pub/otn_software/jdbc/237/ojdbc8.jar

# Download xmlparserv2-19.3.0.0
RUN curl -L -o /opt/spark/jars/xmlparserv2-19.3.0.0.jar https://repo1.maven.org/maven2/com/oracle/ojdbc/xmlparserv2/19.3.0.0/xmlparserv2-19.3.0.0.jar

#Download xdb-19.3.0.0
RUN curl -L -o /opt/spark/jars/xdb-19.3.0.0.jar https://repo1.maven.org/maven2/com/oracle/ojdbc/xdb/19.3.0.0/xdb-19.3.0.0.jar

# Download iceberg jar
RUN curl -L -o /opt/spark/jars/iceberg-aws-bundle-1.6.1.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar

# Download iceberg spark runtime jar
RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

RUN curl -L -o /opt/spark/jars/iceberg-spark-extensions-3.5_2.12-1.6.1.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-extensions-3.5_2.12/1.6.1/iceberg-spark-extensions-3.5_2.12-1.6.1.jar

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
USER root
RUN pip install PyYAML==6.0.2
RUN curl -L -o /opt/spark/jars/spark-xml_2.12-0.15.0.jar https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar
RUN chmod 644 /opt/spark/jars/*