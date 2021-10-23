.PHONY: test run assembly sparksubmit

TRAFFIC_CSV?=/workspaces/traffic-counter/src/test/resources/traffic_count.csv

sparksubmit: assembly
	spark-submit --master=local[*] --deploy-mode client --class au.com.thetko.trafficcounter.Main /workspaces/traffic-counter/target/scala-2.12/traffic-counter-assembly-1.0.jar ${TRAFFIC_CSV}

assembly:
	sbt assembly

test:
	sbt test

run:
	sbt -J-Dspark.master=local[*] "run ${TRAFFIC_CSV}"