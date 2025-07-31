package org.cloudbus.cloudsim.examples.nosf;

import java.util.ArrayList;
import java.util.List;

public class Task {
    private final String id;
    private final double meanExecutionTime;
    private final double varianceExecutionTime;
    private final double dataTransferTime;
    private final Workflow workflow;
    private final List<Task> predecessors = new ArrayList<>();
    private final List<Task> successors = new ArrayList<>();
    private double earliestStartTime;
    private double latestCompletionTime;
    private double subDeadline;
    private double priority;
    private double startTime;
    private double executionTime;
    private double completionTime;
    private Vm assignedVM;
    private double cost;
    private double energyConsumption;

    public Task(String id, double meanExecutionTime, double varianceExecutionTime, double dataTransferTime, Workflow workflow) {
        this.id = id;
        this.meanExecutionTime = meanExecutionTime;
        this.varianceExecutionTime = varianceExecutionTime;
        this.dataTransferTime = dataTransferTime;
        this.workflow = workflow;
    }

    public void addPredecessor(Task predecessor) {
        predecessors.add(predecessor);
        predecessor.addSuccessor(this);
    }

    public void addSuccessor(Task successor) {
        successors.add(successor);
    }

    public boolean isReady() {
        return predecessors.stream().allMatch(pred -> pred.completionTime > 0);
    }

    public String getId() {
        return id;
    }

    public double getMeanExecutionTime() {
        return meanExecutionTime;
    }

    public double getVarianceExecutionTime() {
        return varianceExecutionTime;
    }

    public double getDataTransferTime() {
        return dataTransferTime;
    }

    public Workflow getWorkflow() {
        return workflow;
    }

    public List<Task> getPredecessors() {
        return predecessors;
    }

    public List<Task> getSuccessors() {
        return successors;
    }

    public double getEarliestStartTime() {
        return earliestStartTime;
    }

    public void setEarliestStartTime(double earliestStartTime) {
        this.earliestStartTime = earliestStartTime;
    }

    public double getLatestCompletionTime() {
        return latestCompletionTime;
    }

    public void setLatestCompletionTime(double latestCompletionTime) {
        this.latestCompletionTime = latestCompletionTime;
    }

    public double getSubDeadline() {
        return subDeadline;
    }

    public void setSubDeadline(double subDeadline) {
        this.subDeadline = subDeadline;
    }

    public double getPriority() {
        return priority;
    }

    public void setPriority(double priority) {
        this.priority = priority;
    }

    public double getStartTime() {
        return startTime;
    }

    public void setStartTime(double startTime) {
        this.startTime = startTime;
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(double executionTime) {
        this.executionTime = executionTime;
    }

    public double getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(double completionTime) {
        this.completionTime = completionTime;
    }

    public Vm getAssignedVM() {
        return assignedVM;
    }

    public void setAssignedVM(Vm assignedVM) {
        this.assignedVM = assignedVM;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getEnergyConsumption() {
        return energyConsumption;
    }

    public void setEnergyConsumption(double energyConsumption) {
        this.energyConsumption = energyConsumption;
    }

    // متد جدید برای محاسبه زمان انتقال داده به یک تسک خاص
    public double getDataTransferTime(Task targetTask) {
        // این یک پیاده‌سازی ساده است. در حالت واقعی ممکن است به حجم داده خروجی بستگی داشته باشد.
        // فرض می‌کنیم زمان انتقال داده متعلق به تسک مبدا است.
        return this.dataTransferTime;
    }
}