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
    // --- اصلاح شد: اولویت‌بندی بر اساس زودترین زمان شروع منطقی‌تر است ---
    private final PriorityQueue<Task> readyTasks = new PriorityQueue<>(Comparator.comparingDouble(Task::getEarliestStartTime));
    private final VMFactory vmFactory;
    private static double currentTime = 0.0;
    private double totalCost = 0.0;
    private double totalEnergyConsumption = 0.0;
    private double resourceUtilization = 0.0;
    // --- حذف شد: این مقدار باید در انتها محاسبه شود ---
    // private double deadlineViolationProbability = 0.0; 
    private static double billingPeriod;
    private static int bandwidthMbps;
    private static int normalizationFactor;
    private static double varianceFactorAlpha;
    private final double deadlineFactorBeta;
    private static double estimationFactorEta;
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
                .forEach(readyTasks::add);
    }

    private void preprocessWorkflow(Workflow workflow) {
        // --- اصلاح شد: ابتدا باید تمام مقادیر پایه محاسبه شوند ---
        for (Task task : workflow.getTasks()) {
            task.setEarliestStartTime(calculateEarliestStartTime(task));
            // --- اصلاح شد: اولویت دیگر اینجا ست نمی‌شود ---
        }
        for (Task task : workflow.getTasks()) {
            task.setLatestCompletionTime(calculateLatestCompletionTime(task));
        }
        for (Task task : workflow.getTasks()) {
            task.setSubDeadline(calculateSubDeadline(task));
        }
    }

    private double getEstimatedExecutionTime(Task task) {
        // --- اصلاح شد: این فرمول معادل w(λ) در مقاله است ---
        return task.getMeanExecutionTime() + Math.sqrt(task.getVarianceExecutionTime());
    }

    private double calculateEarliestStartTime(Task task) {
        if (task.getPredecessors().isEmpty()) {
            return task.getWorkflow().getArrivalTime();
        }
        // --- اصلاح شد: محاسبه EST بر اساس EFT پدران است ---
        return task.getPredecessors().stream()
                .mapToDouble(pred -> 
                    calculateEarliestStartTime(pred) + getEstimatedExecutionTime(pred) + pred.getDataTransferTime(task))
                .max().orElse(0.0);
    }

    private double calculateLatestCompletionTime(Task task) {
        if (task.getSuccessors().isEmpty()) {
            return task.getWorkflow().getDeadline();
        }
        // --- اصلاح شد: محاسبه LCT بر اساس LCT فرزندان و زمان اجرای خودشان است ---
        return task.getSuccessors().stream()
                .mapToDouble(succ -> 
                    calculateLatestCompletionTime(succ) - getEstimatedExecutionTime(succ) - task.getDataTransferTime(succ))
                .min().orElse(task.getWorkflow().getDeadline());
    }
    
    // --- اصلاح شد: این متد به طور کامل بازنویسی شد تا منطق توزیع slack را پیاده‌سازی کند ---
    private double calculateSubDeadline(Task task) {
        Workflow workflow = task.getWorkflow();
        double workflowSlack = workflow.getDeadline() - workflow.getCriticalPathLength();

        if (workflowSlack < 0) {
           workflowSlack = 0; // اگر ددلاین خیلی فشرده باشد، اسلک منفی را صفر در نظر می‌گیریم
        }

        double taskWeight = getEstimatedExecutionTime(task) / workflow.getTotalExecutionTime();
        double taskSlack = workflowSlack * taskWeight;
        
        return task.getEarliestStartTime() + getEstimatedExecutionTime(task) + taskSlack;
    }

    // --- اصلاح شد: حلقه اصلی شبیه‌سازی برای مدیریت صحیح رویدادها بازنویسی شد ---
    public void runSimulation() {
        while (workflows.stream().anyMatch(w -> !w.isCompleted())) {
            
            // اگر تسک آماده‌ای برای اجرا وجود دارد، زمان‌بندی کن
            if (!readyTasks.isEmpty()) {
                Task taskToSchedule = readyTasks.poll();

                // اگر زمان فعلی از زمان آماده بودن تسک عقب‌تر است، زمان را جلو ببر
                if (currentTime < taskToSchedule.getEarliestStartTime()) {
                    currentTime = taskToSchedule.getEarliestStartTime();
                }
                
                scheduleTask(taskToSchedule);
            } else {
                // اگر تسک آماده‌ای نیست، زمان را به اتمام نزدیک‌ترین تسک در حال اجرا منتقل کن
                double nextCompletionTime = vmFactory.getNextVmCompletionTime(currentTime);
                if (nextCompletionTime < Double.MAX_VALUE) {
                    currentTime = nextCompletionTime;
                    // پردازش تسک‌هایی که در این زمان تمام شده‌اند
                    processFinishedTasks();
                } else {
                    // اگر هیچ تسک آماده و در حال اجرایی نیست، شبیه‌سازی تمام است
                    break;
                }
            }
        }
        
        // آزادسازی تمام VM های باقیمانده در انتهای شبیه‌سازی
        for (Vm vm : vmFactory.getActiveVMs()) {
            vmFactory.releaseVM(vm, currentTime);
        }

        calculatePerformanceMetrics();
        printSimulationSummary();
    }

    private void scheduleTask(Task task) {
        Vm vm = vmFactory.findOrCreateVM(task, currentTime);
        if (vm == null) {
            LOGGER.warning("Could not schedule Task " + task.getId() + ": No suitable VM found or limit reached. Re-queuing.");
            // --- اصلاح شد: اگر VM پیدا نشد، تسک با کمی تاخیر دوباره به صف برمی‌گردد ---
            task.setEarliestStartTime(currentTime + 1.0); // برای جلوگیری از حلقه بی‌نهایت
            readyTasks.add(task);
            return;
        }

        double startTime = vmFactory.calculatePredictedStartTime(task, vm, currentTime);
        double executionTime = vmFactory.calculatePredictedExecutionTime(task, vm); // این زمان واقعی اجرای تسک است
        double completionTime = startTime + executionTime;

        task.setStartTime(startTime);
        task.setExecutionTime(executionTime);
        task.setCompletionTime(completionTime);
        task.setAssignedVM(vm);

        double cost = vm.getCostForDuration(executionTime);
        double energy = vm.getEnergyForDuration(executionTime);
        task.setCost(cost);
        task.setEnergyConsumption(energy);
        totalCost += cost;
        totalEnergyConsumption += energy;
        
        // بروزرسانی وضعیت VM
        vm.addTask(task);
        
        LOGGER.info(String.format("Scheduled Task %s on VM %s: Start=%.2f, End=%.2f, Execution=%.2f, Cost=$%.4f, Energy=%.2f Ws",
                task.getId(), vm.getId(), startTime, completionTime, executionTime, cost, energy));

        // --- اصلاح شد: پس از زمانبندی، باید تسک‌های تمام شده را پردازش کنیم ---
        processFinishedTasks();
    }

    // --- جدید: متد برای پردازش تسک‌های تمام‌شده و فعال‌سازی فاز بازخورد ---
    private void processFinishedTasks() {
        List<Task> justCompletedTasks = vmFactory.updateVmsAndGetCompletedTasks(currentTime);

        for (Task completedTask : justCompletedTasks) {
            LOGGER.info(String.format("Task %s completed on VM %s at time %.2f", completedTask.getId(), completedTask.getAssignedVM().getId(), completedTask.getCompletionTime()));
            feedbackProcessing(completedTask);
        }
    }


    // private void feedbackProcessing(Task completedTask) {
    //     for (Task successor : completedTask.getSuccessors()) {
    //         // --- اصلاح شد: چک کردن isReady باید بر اساس وضعیت پدران باشد ---
    //         if (successor.getPredecessors().stream().allMatch(p -> p.getCompletionTime() > 0)) {
                
    //             // --- اصلاح شد: منطق فاز بازخورد بر اساس مقاله ---
    //             double newEarliestStartTime = successor.getPredecessors().stream()
    //                     .mapToDouble(pred -> pred.getCompletionTime() + pred.getDataTransferTime(successor))
    //                     .max().orElse(0.0);

    //             successor.setEarliestStartTime(newEarliestStartTime);
                
    //             // تنظیم مجدد زیرمهلت بر اساس فرمول (18) مقاله
    //             double originalDuration = successor.getSubDeadline() - (successor.getEarliestStartTime() - getEstimatedExecutionTime(successor));
    //             double newSubDeadline = newEarliestStartTime + originalDuration;

    //             if (newSubDeadline < successor.getLatestCompletionTime()) {
    //                 successor.setSubDeadline(newSubDeadline);
    //             } else {
    //                 successor.setSubDeadline(successor.getLatestCompletionTime());
    //             }
    //             readyTasks.add(successor);
    //             LOGGER.info(String.format("Feedback: Successor %s is now ready. EST=%.2f, SubDeadline=%.2f", successor.getId(), newEarliestStartTime, successor.getSubDeadline()));
    //         }
    //     }
    // }
    
    
    private void feedbackProcessing(Task completedTask) {
        for (Task successor : completedTask.getSuccessors()) {
            // چک می‌کنیم که تمام پدران تسک جانشین، تمام شده باشند
            if (successor.getPredecessors().stream().allMatch(p -> p.getCompletionTime() > 0)) {
                
                // زمان واقعی آماده به کار شدن تسک را محاسبه می‌کنیم (ζ_r)
                double newEarliestStartTime = successor.getPredecessors().stream()
                        .mapToDouble(pred -> pred.getCompletionTime() + pred.getDataTransferTime(successor))
                        .max().orElse(0.0);

                // --- شروع اصلاحیه ---
                // مقدار تخمینی اولیه زمان شروع (M^{est}) را قبل از بازنویسی ذخیره می‌کنیم
                double originalEst = successor.getEarliestStartTime();

                // زمان شروع تسک را با مقدار واقعی جدید به‌روز می‌کنیم
                successor.setEarliestStartTime(newEarliestStartTime);
                
                // زیرمهلت را بر اساس فرمول مقاله و با استفاده از M^{est} اولیه تنظیم می‌کنیم
                // فرمول: l_sub^r = ζ_r + (l_sub - (M^{est} - w))
                double originalAllocatedDuration = successor.getSubDeadline() - (originalEst - getEstimatedExecutionTime(successor));
                double newSubDeadline = newEarliestStartTime + originalAllocatedDuration;
                // --- پایان اصلاحیه ---

                if (newSubDeadline < successor.getLatestCompletionTime()) {
                    successor.setSubDeadline(newSubDeadline);
                } else {
                    successor.setSubDeadline(successor.getLatestCompletionTime());
                }
                
                readyTasks.add(successor);
                LOGGER.info(String.format("Feedback: Successor %s is now ready. EST=%.2f, SubDeadline=%.2f", successor.getId(), newEarliestStartTime, newSubDeadline));
            }
        }
    }
    // --- حذف شد: منطق زمان دیگر به این شکل نیست ---
    // private void updateCurrentTime() { ... }

    private void calculatePerformanceMetrics() {
        double totalVmLeaseTime = vmFactory.getAllVMs().stream()
                .mapToDouble(Vm::getTotalLeaseTime)
                .sum();
        double totalExecutionTime = workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getExecutionTime)
                .sum();

        resourceUtilization = totalVmLeaseTime > 0
                ? (totalExecutionTime / totalVmLeaseTime) * 100
                : 0.0;
    }

    private void printSimulationSummary() {
        DecimalFormat df = new DecimalFormat("#.##");
        double simulationDuration = workflows.stream()
                .flatMap(w -> w.getTasks().stream())
                .mapToDouble(Task::getCompletionTime)
                .max()
                .orElse(0.0);
        
        long deadlineViolations = workflows.stream().filter(Workflow::hasDeadlineViolation).count();

        LOGGER.info("\n=== Comprehensive Simulation Summary ===");
        LOGGER.info("Simulation Duration: " + df.format(simulationDuration) + " sec");
        LOGGER.info("Total VM Rental Cost: $" + df.format(totalCost));
        LOGGER.info("Total Energy Consumption: " + df.format(totalEnergyConsumption) + " Watt-seconds");
        LOGGER.info("Resource Utilization Efficiency: " + df.format(resourceUtilization) + "%");
        LOGGER.info("Deadline Violation Count: " + deadlineViolations + " out of " + workflows.size());


        LOGGER.info("\nWorkflow Details:");
        for (Workflow workflow : workflows) {
            LOGGER.info("  Workflow: " + workflow.getId() + (workflow.hasDeadlineViolation() ? " (DEADLINE VIOLATED)" : ""));
            LOGGER.info("    Arrival Time: " + df.format(workflow.getArrivalTime()) + " sec");
            LOGGER.info("    Deadline: " + df.format(workflow.getDeadline()) + " sec");
            LOGGER.info("    Makespan: " + df.format(workflow.getMakespan()) + " sec");

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
        for (Vm vm : vmFactory.getAllVMs()) {
            LOGGER.info(String.format("  VM %s: Type=%s, Active Time=%.2f sec, Idle Time=%.2f sec, " +
                            "Total Lease Time=%.2f sec, Energy=%.2f Ws, Cost=$%.4f",
                    vm.getId(), vm.getTypeId(), vm.getTotalActiveTime(), vm.getTotalIdleTime(), vm.getTotalLeaseTime(),
                    vm.getEnergyConsumption(), vm.getCost()));
        }

        LOGGER.info("\nAdvanced Performance Metrics:");
        LOGGER.info("  Average Task Delay (from sub-deadline): " + df.format(calculateAverageTaskDelay()) + " sec");
        LOGGER.info("  Number of VMs Used: " + vmFactory.getVMCounter());
    }

    private double calculateAverageTaskDelay() {
        double totalDelay = 0.0;
        long taskCount = 0;
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
    
    // --- متدهای استاتیک بدون تغییر باقی می‌مانند ---
    public static double getCurrentTime() { return currentTime; }
    public static int getBandwidthMbps() { return bandwidthMbps; }
    public static double getVarianceFactorAlpha() { return varianceFactorAlpha; }
    public static int getNormalizationFactor() { return normalizationFactor; }
    public static double getEstimationFactorEta() { return estimationFactorEta; }
    public static double getBillingPeriod() { return billingPeriod; }
}