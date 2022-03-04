# To build the Autohealer
Run `mvn clean install`

## To run the autohealer, which in turn would launch and maintain 10 workers
Run `java -jar target/autohealer-1.0-SNAPSHOT-jar-with-dependencies.jar <number of workers> <path to woker jar>
Example: `java -jar target/autohealer-1.0-SNAPSHOT-jar-with-dependencies.jar 10 "../flakyworker/target/flaky.worker-1.0-SNAPSHOT-jar-with-dependencies.jar"`
