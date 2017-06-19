To build:
   mvn assembly:assembly -DdescriptorId=jar-with-dependencies

To run:
   cd target;java -cp cluster-joiner-0.0.1-jar-with-dependencies.jar ru.itis.suc.NodeAgent /home/user/nifi/nifi-1.2.0 8085

8085 - port to use by agent


