# Traffic Counter
Given a csv file of datetime (30 min interval) and count, the application will output to the **console**:

1. Total Cars 
2. Total Cars by date
3. Top three 30 min periods with most amount of cars
4. 1.5 hour period with least amount of cars (<em>Has to be 3 contiguous half hour records</em>)

## Requirements
This project is setup and run using VsCode and [devcontainers](https://code.visualstudio.com/docs/remote/containers). 

**Note:** The initial load can take up to 5 minutes to setup all the build tools.

## Usage
```bash
# Run in local
sbt "run -J-Dspark.master=local[*] ${input_file}"

# Example
sbt -J-Dspark.master=local[*] "run /workspaces/traffic-counter/src/test/resources/traffic_count.csv"
```

## Spark Submit
Testing with sparksubmit
```bash
TRAFFIC_CSV=${path_to_csv} make sparksubmit

# Example
TRAFFIC_CSV=/workspaces/traffic-counter/src/test/resources/traffic_count.csv make sparksubmit
```

## Unit Tests
```bash
sbt test
```