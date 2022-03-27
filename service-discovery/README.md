# To build the ServiceRegistry
Run `mvn clean package`

## To run the ServiceRegistry
Run `java -jar service-discovery-1.0-SNAPSHOT-jar-with-dependencies.jar <port number>`

If `port number` is not specified, it will run on `8080`

### Note
The following classes are not used, `LeaderElection`, `ServiceRegistry` and `Application` classes.
I added them to illustrate the difference that we did on the previous `leader-election` project's files.

Instead  `LeaderElectionIntegrated`, `ServiceRegistryIntegrated` and `ApplicationIntegrated` classes are used.