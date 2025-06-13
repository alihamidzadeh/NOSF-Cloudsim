package org.cloudbus.cloudsim.examples.nosf;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class NOSFScheduler {
    private final List<Workflow> workflows = new ArrayList<>();
    private final PriorityQueue<Task> readyTasks = new PriorityQueue<>(Comparator.comparingDouble(Task::getPriority));
    private final VMFactory vmFactory;
    private static double currentTime = 0.0;
    private double totalCost = 0.0;
    private double totalEnergyConsumption = 0.0;
    private double resourceUtilization = 0.0;
    private double deadlineViolationProbability = 0.0;
    private final double billingPeriod;
    private static int bandwidthMbps;
    private static int normalizationFactor;
    private static double varianceFactorAlpha;
    private final double deadlineFactorBeta;
    private final double estimationFactorEta;
    private static final Logger LOGGER = Logger.getLogger(NOSFScheduler.class.getName());

    static {
        try {
            LOGGER.setLevel(Level.INFO);
            LOGGER.setUseParentHandlers(false);

            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            consoleHandler.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord record) {
                    return String.format("%s%n", record.getMessage());
                }
            });
            LOGGER.addHandler(consoleHandler);

            FileHandler fileHandler = new FileHandler("simulation.log", false);
            fileHandler.setLevel(Level.INFO);
            fileHandler.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord record) {
                    return String.format("%s%n", record.getMessage());
                }
            });
            LOGGER.addHandler(fileHandler);
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("Simulation started at: " + java.time.LocalDateTime.now());
    }

    public NOSFScheduler(String configFile) {
        Document doc;
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(configFile);
            Element simParams = (Element) doc.getElementsByTagName("simulationParameters").item(0);
            int maxVMs = Integer.parseInt(simParams.getElementsByTagName("maxVMs").item(0).getTextContent());
            this.normalizationFactor = Integer.parseInt(simParams.getElementsByTagName("NormalizationFactor").item(0).getTextContent());
            this.bandwidthMbps = Integer.parseInt(simParams.getElementsByTagName("bandwidthMbps").item(0).getTextContent());
            this.billingPeriod = Double.parseDouble(simParams.getElementsByTagName("billingPeriod").item(0).getTextContent());
            this.varianceFactorAlpha = Double.parseDouble(simParams.getElementsByTagName("varianceFactorAlpha").item(0).getTextContent());
            this.deadlineFactorBeta = Double.parseDouble(simParams.getElementsByTagName("deadlineFactorBeta").item(0).getTextContent());
            this.estimationFactorEta = Double.parseDouble(simParams.getElementsByTagName("estimationFactorEta").item(0).getTextContent());
            this.vmFactory = new VMFactory(configFile, maxVMs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load simulation config", e);
        }
    }

    public void submitWorkflow(Workflow workflow) {
        workflows.add(workflow);
        preprocessWorkflow(workflow);
        workflow.getTasks().stream()
                .filter(Task::isReady)
                .forEach(task -> {
                    task.setPriority(calculateEarliestCompletionTime(task));
                    readyTasks.add(task);
                });
        // System.out.println("Ready Task: " + readyTasks.poll().getId());
        // System.out.println("ID02 EarliestCompletionTime: "+calculateEarliestCompletionTime(workflow.getTaskById("ID00002")));
        // System.out.println("ID13 EarliestCompletionTime: "+calculateEarliestCompletionTime(workflow.getTaskById("ID00013")));

    }

    private void preprocessWorkflow(Workflow workflow) {
        for (Task task : workflow.getTasks()) {
            task.setEarliestStartTime(calculateEarliestStartTime(task));
            task.setLatestCompletionTime(calculateLatestCompletionTime(task));
            task.setSubDeadline(calculateSubDeadline(task));
        }
    }

    private double calculateEarliestStartTime(Task task) {
        if (task.getPredecessors().isEmpty()) {
            return task.getWorkflow().getArrivalTime();
        }
        return task.getPredecessors().stream()
                .mapToDouble(pred -> calculateEarliestCompletionTime(pred) + pred.getDataTransferTime())
                .max().orElse(0.0);
    }

    private double calculateEarliestCompletionTime(Task task) {
        double baseExecutionTime = task.getMeanExecutionTime() + Math.sqrt(task.getVarianceExecutionTime()) * estimationFactorEta;
        double deadlineUrgency = task.getSubDeadline() > 0 ? baseExecutionTime / task.getSubDeadline() : 1.0;
        return task.getEarliestStartTime() + baseExecutionTime * (1.0 + deadlineUrgency);
    }

    private double calculateLatestCompletionTime(Task task) {
        if (task.getSuccessors().isEmpty()) {
            return task.getWorkflow().getDeadline();
        }
        return task.getSuccessors().stream()
                .mapToDouble(succ -> calculateLatestCompletionTime(succ) - succ.getMeanExecutionTime() - succ.getDataTransferTime())
                .min().orElse(task.getWorkflow().getDeadline());
    }

    private double calculateSubDeadline(Task task) {
        Workflow workflow = task.getWorkflow();
        double pathDeadline = calculateLatestCompletionTime(task) - calculateEarliestStartTime(workflow.getEntryTask());
        double taskExecutionTime = task.getMeanExecutionTime() + Math.sqrt(task.getVarianceExecutionTime());
        double pathExecutionTime = workflow.getTasks().stream()
                .mapToDouble(t -> t.getMeanExecutionTime() + Math.sqrt(t.getVarianceExecutionTime()))
                .sum();
        return task.getEarliestStartTime() + (taskExecutionTime / pathExecutionTime) * pathDeadline * deadlineFactorBeta;
    }

    public void runSimulation() {
        while (!readyTasks.isEmpty() || workflows.stream().anyMatch(w -> !w.isCompleted())) {
            while (!readyTasks.isEmpty()) {
                Task task = readyTasks.poll();
                scheduleTask(task);
            }
        }
        calculatePerformanceMetrics();
        printSimulationSummary();
    }

    private void scheduleTask(Task task) {
        Vm vm = vmFactory.createVM(task, currentTime);
        if (vm == null) {
            LOGGER.info("No VM available for task " + task.getId());
            readyTasks.add(task);
            return;
        }

        double startTime = vmFactory.calculatePredictedStartTime(task, vm, currentTime);
        double executionTime = vmFactory.calculatePredictedExecutionTime(task, vm);
        double completionTime = startTime + executionTime;

        task.setStartTime(startTime);
        task.setExecutionTime(executionTime);
        task.setCompletionTime(completionTime);
        task.setAssignedVM(vm);

        double cost = vm.getCostPerHour() * (executionTime / billingPeriod);
        double energy = vm.getEnergyPerSecond() * executionTime;
        task.setCost(cost);
        task.setEnergyConsumption(energy);
        totalCost += cost;
        totalEnergyConsumption += energy;

        vm.updateActiveTime(executionTime);
        vm.addCost(cost);
        vm.addEnergyConsumption(energy);
        vm.setPredictedCompletionTime(completionTime);

        LOGGER.info(String.format("Scheduled Task %s on VM %s: Start=%.2f, End=%.2f, Execution=%.2f, Cost=$%.4f, Energy=%.2f Ws",
                task.getId(), vm.getId(), startTime, completionTime, executionTime, cost, energy));

        updateCurrentTime();
        feedbackProcessing(task);
    }

    private void feedbackProcessing(Task task) {
        for (Task successor : task.getSuccessors()) {
            if (successor.isReady()) {
                double newEarliestStartTime = successor.getPredecessors().stream()
                        .mapToDouble(pred -> pred.getCompletionTime() + pred.getDataTransferTime())
                        .max().orElse(0.0);
                double newEarliestCompletionTime = newEarliestStartTime + successor.getMeanExecutionTime() +
                        Math.sqrt(successor.getVarianceExecutionTime()) * estimationFactorEta;
                successor.setEarliestStartTime(newEarliestStartTime);
                successor.setPriority(newEarliestCompletionTime);

                double delta = successor.getSubDeadline() - successor.getEarliestStartTime();
                double newSubDeadline = newEarliestStartTime + delta;
                if (newSubDeadline < successor.getLatestCompletionTime()) {
                    successor.setSubDeadline(newSubDeadline);
                } else {
                    successor.setSubDeadline(successor.getLatestCompletionTime());
                }
                readyTasks.add(successor);
            }
        }

        if (task.getSuccessors().isEmpty() && task.getWorkflow().isCompleted()) {
            vmFactory.releaseVM(task.getAssignedVM(), currentTime);
        }
    }

    private void updateCurrentTime() {
        double minReadyTime = readyTasks.stream()
                .mapToDouble(Task::getEarliestStartTime)
                .min()
                .orElse(Double.MAX_VALUE);
        double maxCompletionTime = workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getCompletionTime)
                .filter(t -> t > 0)
                .max()
                .orElse(currentTime);
        currentTime = Math.min(minReadyTime, maxCompletionTime);
        if (readyTasks.isEmpty() && maxCompletionTime > currentTime) {
            currentTime = maxCompletionTime;
        }
    }

    private void calculatePerformanceMetrics() {
        int violatedWorkflows = (int) workflows.stream()
                .filter(Workflow::hasDeadlineViolation)
                .count();
        deadlineViolationProbability = workflows.isEmpty() ? 0.0 : (double) violatedWorkflows / workflows.size();

        double totalActiveTime = vmFactory.getActiveVMs().stream()
                .mapToDouble(Vm::getTotalActiveTime)
                .sum();
        double totalExecutionTime = workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getExecutionTime)
                .sum();
        double simulationDuration = workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getCompletionTime)
                .max()
                .orElse(0.0);
        double totalAvailableTime = simulationDuration * vmFactory.getActiveVMs().size();
        resourceUtilization = totalAvailableTime > 0 ? (totalExecutionTime / totalAvailableTime) * 100 : 0.0;
    }

    private void printSimulationSummary() {
        DecimalFormat df = new DecimalFormat("#.##");
        double simulationDuration = workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getCompletionTime)
                .max()
                .orElse(0.0);
        
        for (Vm vm : vmFactory.getActiveVMs()) {
            vm.resetIdleTime(simulationDuration);
        }

        LOGGER.info("\n=== Comprehensive Simulation Summary ===");
        LOGGER.info("Simulation Duration: " + df.format(simulationDuration) + " sec");
        LOGGER.info("Total VM Rental Cost: $" + df.format(totalCost));
        LOGGER.info("Total Energy Consumption: " + df.format(totalEnergyConsumption) + " Watt-seconds");
        LOGGER.info("Resource Utilization Efficiency: " + df.format(resourceUtilization) + "%");
        LOGGER.info("Deadline Violation Probability: " + df.format(deadlineViolationProbability * 100) + "%");

        LOGGER.info("\nWorkflow Details:");
        for (Workflow workflow : workflows) {
            LOGGER.info("  Workflow: " + workflow.getId());
            LOGGER.info("    Arrival Time: " + df.format(workflow.getArrivalTime()) + " sec");
            LOGGER.info("    Deadline: " + df.format(workflow.getDeadline()) + " sec");
            LOGGER.info("    Makespan: " + df.format(workflow.getMakespan()) + " sec");
            LOGGER.info("    Status: " + (workflow.hasDeadlineViolation() ? "Violated" : "Met"));

            LOGGER.info("    Tasks:");
            for (Task task : workflow.getTasks()) {
                LOGGER.info(String.format("      Task %s: Sub-Deadline=%.2f sec, Start=%.2f sec, End=%.2f sec, " +
                                "Execution=%.2f sec, VM=%s, Cost=$%.4f, Energy=%.2f Ws",
                        task.getId(), task.getSubDeadline(), task.getStartTime(), task.getCompletionTime(),
                        task.getExecutionTime(), task.getAssignedVM() != null ? task.getAssignedVM().getId() : "None",
                        task.getCost(), task.getEnergyConsumption()));
            }
        }

        LOGGER.info("\nVM Usage Details:");
        for (Vm vm : vmFactory.getActiveVMs()) {
            LOGGER.info(String.format("  VM %s: Capacity=%.2f MIPS, Active Time=%.2f sec, Idle Time=%.2f sec, " +
                            "Energy=%.2f Ws, Cost=$%.4f",
                    vm.getId(), vm.getProcessingCapacity(), vm.getTotalActiveTime(), vm.getTotalIdleTime(),
                    vm.getEnergyConsumption(), vm.getCost()));
        }

        LOGGER.info("\nAdvanced Performance Metrics:");
        LOGGER.info("  Average Task Execution Delay: " + df.format(calculateAverageTaskDelay()) + " sec");
        LOGGER.info("  Average VM Idle Time: " + df.format(calculateAverageVMIdleTime()) + " sec");
        LOGGER.info("  Total Data Transfer Time: " + df.format(calculateTotalDataTransferTime()) + " sec");
        // LOGGER.info("  Number of VMs Used: " + vmFactory.getActiveVMs().size());
        LOGGER.info("  Number of VMs Used: " + vmFactory.getVMCounter());

    }

    private double calculateAverageTaskDelay() {
        double totalDelay = 0.0;
        int taskCount = 0;
        for (Workflow workflow : workflows) {
            for (Task task : workflow.getTasks()) {
                if (task.getCompletionTime() > 0) {
                    totalDelay += Math.max(0, task.getCompletionTime() - task.getSubDeadline());
                    taskCount++;
                }
            }
        }
        return taskCount > 0 ? totalDelay / taskCount : 0.0;
    }

    private double calculateAverageVMIdleTime() {
        double totalIdleTime = vmFactory.getActiveVMs().stream()
                .mapToDouble(Vm::getTotalIdleTime)
                .sum();
        int vmCount = vmFactory.getActiveVMs().size();
        return vmCount > 0 ? totalIdleTime / vmCount : 0.0;
    }

    private double calculateTotalDataTransferTime() {
        return workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getDataTransferTime)
                .sum();
    }

    public static double getCurrentTime(){
        return currentTime;
    }

    public static int getBandwidthMbps(){
        return bandwidthMbps;
    }

    public static double getVarianceFactorAlpha(){
        return varianceFactorAlpha;
    }

    public static int getNormalizationFactor(){
        return normalizationFactor;
    }
}