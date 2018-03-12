FROM java

ADD target/blazegraph-1.0.0.jar /blazegraph/blazegraph-1.0.0.jar
ADD repository/blazegraph.jar /blazegraph/repository/
ADD RWStore.properties /blazegraph/RWStore.properties
ADD log4j.properties /blazegraph/

WORKDIR /blazegraph

CMD java -cp "blazegraph-1.0.0.jar" -Dlog4j.configuration=file:/blazegraph/log4j.properties org.hobbit.core.run.ComponentStarter eu.project_hobbit.mocha.systems.blazegraph.BlazegraphAdapter
