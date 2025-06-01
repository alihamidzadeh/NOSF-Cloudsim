package org.cloudbus.cloudsim.examples.nosf;

public class Vm {
    private final String id;
    private final double processingCapacity;
    private final double costPerHour;
    private final double energyPerSecond;
    private final double bootTime;
    private boolean active;
    private double predictedCompletionTime;
    private double totalActiveTime;
    private double totalIdleTime;
    private double cost;
    private double energyConsumption;

    public Vm(String id, double processingCapacity, double costPerHour, double energyPerSecond, double bootTime) {
        this.id = id;
        this.processingCapacity = processingCapacity;
        this.costPerHour = costPerHour;
        this.energyPerSecond = energyPerSecond;
        this.bootTime = bootTime;
        this.active = true;
        this.predictedCompletionTime = 0.0;
        this.totalActiveTime = 0.0;
        this.totalIdleTime = 0.0;
        this.cost = 0.0;
        this.energyConsumption = 0.0;
    }

    public String getId() {
        return id;
    }

    public double getProcessingCapacity() {
        return processingCapacity;
    }

    public double getCostPerHour() {
        return costPerHour;
    }

    public double getEnergyPerSecond() {
        return energyPerSecond;
    }

    public double getBootTime() {
        return bootTime;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public double getPredictedCompletionTime() {
        return predictedCompletionTime;
    }

    public void setPredictedCompletionTime(double predictedCompletionTime) {
        this.predictedCompletionTime = predictedCompletionTime;
    }

    public double getTotalActiveTime() {
        return totalActiveTime;
    }

    public double getTotalIdleTime() {
        return totalIdleTime;
    }

    public double getCost() {
        return cost;
    }

    public double getEnergyConsumption() {
        return energyConsumption;
    }

    public void updateActiveTime(double executionTime) {
        this.totalActiveTime += executionTime;
    }

    public void updateIdleTime(double idleTime) {
        if (idleTime > 0) {
            this.totalIdleTime += idleTime;
        }
    }

    public void addCost(double cost) {
        this.cost += cost;
    }

    public void addEnergyConsumption(double energy) {
        this.energyConsumption += energy;
    }

    public void resetIdleTime(double simulationDuration) {
        if (totalIdleTime > simulationDuration - totalActiveTime) {
            totalIdleTime = Math.max(0, simulationDuration - totalActiveTime);
        }
    }
}