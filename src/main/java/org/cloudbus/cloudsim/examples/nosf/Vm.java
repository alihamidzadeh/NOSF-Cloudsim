package org.cloudbus.cloudsim.examples.nosf;

import java.util.ArrayList;
import java.util.List;

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


    private String typeId;
    private double leaseStartTime;
    private double leaseEndTime;
    private double nextReleaseCheckTime;
    private final List<Task> runningTasks = new ArrayList<>();
    private final List<Task> completedTasks = new ArrayList<>();

    public Vm(String id, String typeId, double processingCapacity, double costPerHour, double energyPerSecond, double bootTime) {
        this.id = id;
        this.typeId = typeId;
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

    public String getTypeId() { return typeId; }
    public void setLeaseStartTime(double time) { this.leaseStartTime = time; }
    public void setLeaseEndTime(double time) { this.leaseEndTime = time; }

    public double getTotalLeaseTime() {
        return leaseEndTime > leaseStartTime ? leaseEndTime - leaseStartTime : 0;
    }

    public double getAvailableTime(double currentTime) {
        if (runningTasks.isEmpty()) {
            // اگر VM بوت نشده، زمان در دسترس بودن پس از بوت است
            return leaseStartTime + bootTime;
        }
        // در غیر این صورت، زمان اتمام آخرین تسک در حال اجراست
        return runningTasks.stream().mapToDouble(Task::getCompletionTime).max().orElse(leaseStartTime + bootTime);
    }

    public boolean isAvailable(double currentTime) {
        return getAvailableTime(currentTime) <= currentTime;
    }

    public void addTask(Task task) {
        runningTasks.add(task);
        this.totalActiveTime += task.getExecutionTime();
        // محاسبه idle time بین اتمام تسک قبلی و شروع تسک جدید
        double lastCompletion = completedTasks.isEmpty() ? (leaseStartTime + bootTime) : completedTasks.get(completedTasks.size()-1).getCompletionTime();
        this.totalIdleTime += task.getStartTime() - lastCompletion;
    }

    public List<Task> updateStatus(double currentTime) {
        List<Task> justCompleted = new ArrayList<>();
        for (Task task : new ArrayList<>(runningTasks)) {
            if (task.getCompletionTime() <= currentTime) {
                runningTasks.remove(task);
                completedTasks.add(task);
                justCompleted.add(task);
            }
        }
        return justCompleted;
    }

    public List<Task> getRunningTasks() { return runningTasks; }

    public double getCostForDuration(double duration) {
        return (duration / 3600.0) * this.costPerHour;
    }

    public double getEnergyForDuration(double duration) {
        return duration * this.energyPerSecond;
    }

    public double getRemainingBillingTime(double currentTime) {
        if (leaseStartTime < 0) return 0;
        double elapsedTime = currentTime - leaseStartTime;
        double billingPeriod = NOSFScheduler.getBillingPeriod();
        return billingPeriod - (elapsedTime % billingPeriod);
    }

    /**
     * زمان بررسی بعدی برای release خودکار (n×billingPeriod).
     */
    public void setNextReleaseCheckTime(double time) {
        this.nextReleaseCheckTime = time;
    }

    public double getNextReleaseCheckTime() {
        return this.nextReleaseCheckTime;
    }

    /**
     * افزایش نقطهٔ چک به اندازهٔ یک دورهٔ صورتحساب (ساعتی).
     */
    public void advanceNextReleaseCheckTime(double billingPeriod) {
        this.nextReleaseCheckTime += billingPeriod;
}

}