# CloudSim 7 Project

This project is a simulation framework built on top of **CloudSim 7**, designed to model and evaluate cloud computing infrastructures and services. It allows researchers and developers to test scheduling algorithms, resource allocation strategies, and data center behavior under various scenarios — all without needing access to a real cloud.


## Technologies Used

* **CloudSim 7** – Core simulation framework for cloud environments
* **JDK 21** – Compiled and tested with Java Development Kit 21
* **Apache Maven 3.9.9** – Used for project build, dependency management, and execution

## Requirements

* Java 21 or higher
* Apache Maven 3.9.9 or compatible version

## Build and Run

Clone the repository:

```bash
git clone https://github.com/alihamidzadeh/NOSF-Cloudsim.git
```

Put NOSF source codes `src/main/java/org/cloudbus/cloudsim/examples/nosf` on your cloudsim-7 Project then build the project with Maven:

```bash
mvnd clean install
```

Run the simulation:

```bash
mvnd exec:java -Dexec.mainClass="org.cloudbus.cloudsim.examples.nosf.Main"
#OR
mvnd exec:java "-Dexec.mainClass=org.cloudbus.cloudsim.examples.nosf.Main"
```

⚠️ This project has been tested on Ubuntu 20.04 and Windows 11. It performs better on Ubuntu, and some log entries may not be visible when running on Windows systems!


## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE v3.0.

