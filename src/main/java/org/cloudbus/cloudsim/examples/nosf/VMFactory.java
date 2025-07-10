package org.cloudbus.cloudsim.examples.nosf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class VMFactory {
    private static final Logger LOGGER = Logger.getLogger(VMFactory.class.getName());

    // Add below static section cause logs of (creation and releasion) doesnt print in Windows!
    static {
        LOGGER.setLevel(Level.INFO);
        LOGGER.setUseParentHandlers(false);  // جلوگیری از پراکندگی به بالاتر

        try {
            FileHandler fileHandler = new FileHandler("simulation.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.INFO);
            LOGGER.addHandler(fileHandler);

            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setFormatter(new SimpleFormatter());
            consoleHandler.setLevel(Level.INFO);
            LOGGER.addHandler(consoleHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class VMType {
        String id;
        double processingCapacity;
        double costPerHour;
        double energyPerSecond;
        double bootTime;
    }

    private final List<VMType> vmTypes = new ArrayList<>();
    private final List<Vm> activeVMs = new ArrayList<>();
    private List<Vm> allVMs = new ArrayList<>();

    private final int maxVMs;
    private int vmCounter = 0;

    public VMFactory(String configFile, int maxVMs) {
        this.maxVMs = maxVMs;
        loadVMTypes(configFile);
    }

    private void loadVMTypes(String configFile) {
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(configFile);
            NodeList vmTypeNodes = doc.getElementsByTagName("vmType");
            for (int i = 0; i < vmTypeNodes.getLength(); i++) {
                Element vmTypeElement = (Element) vmTypeNodes.item(i);
                VMType vmType = new VMType();
                vmType.id = vmTypeElement.getAttribute("id");
                vmType.processingCapacity = Double.parseDouble(vmTypeElement.getAttribute("processingCapacity"));
                vmType.costPerHour = Double.parseDouble(vmTypeElement.getAttribute("costPerHour"));
                vmType.energyPerSecond = Double.parseDouble(vmTypeElement.getAttribute("energyPerSecond"));
                vmType.bootTime = Double.parseDouble(vmTypeElement.getAttribute("bootTime"));
                vmTypes.add(vmType);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Vm createVM(Task task, double currentTime) {
        Vm suitableVM = findSuitableVM(task, currentTime);
        if (suitableVM != null) {
            return suitableVM;
        }

        if (activeVMs.size() >= maxVMs) {
            LOGGER.info("Cannot create new VM: Maximum VM limit reached.");
            return null;
        }
        VMType vmType = selectBestVMType(task, currentTime);
        String vmId = "vm-" + (++vmCounter);
        Vm vm = new Vm(vmId, vmType.processingCapacity, vmType.costPerHour, vmType.energyPerSecond, vmType.bootTime);
        activeVMs.add(vm);
        allVMs.add(vm);
        vm.setPredictedCompletionTime(currentTime + vm.getBootTime());
        LOGGER.info(String.format("Created new VM %s at time %.2f with capacity %.2f MIPS, Boot Time %.2f s", vmId, currentTime, vmType.processingCapacity, vm.getBootTime()));
        return vm;
    }

    private Vm findSuitableVM(Task task, double currentTime) {
        Vm bestVM = null;
        double minCostGrowth = Double.MAX_VALUE;
        double minIdleTime = Double.MAX_VALUE;
        double bestStartTime = Double.MAX_VALUE;
    
        for (Vm vm : activeVMs) {
            if (!vm.isActive()) continue;
    
            double predictedStartTime = calculatePredictedStartTime(task, vm, currentTime);
            double predictedExecutionTime = calculatePredictedExecutionTime(task, vm);
            double predictedCompletionTime = predictedStartTime + predictedExecutionTime;
    
            // اصلاح: فقط sub-deadline اصلی را در نظر بگیر
            // if (predictedCompletionTime > task.getSubDeadline()) continue;
            if (predictedCompletionTime > task.getLatestCompletionTime()) continue;
    
            double costPerSecond = vm.getCostPerHour() / 3600.0;
            double costGrowth = costPerSecond * predictedExecutionTime;
            double idleTime = Math.max(0.0, predictedStartTime - vm.getPredictedCompletionTime());
    
            if (costGrowth < minCostGrowth
             || (costGrowth == minCostGrowth && idleTime < minIdleTime)
             || (costGrowth == minCostGrowth && idleTime == minIdleTime && predictedStartTime < bestStartTime)) {
                minCostGrowth = costGrowth;
                minIdleTime = idleTime;
                bestStartTime = predictedStartTime;
                bestVM = vm;
            }
        }
    
        // اگر هیچ VM فعالی پیدا نشد، برگردان null تا در createVM VM جدید ساخته شود
        return bestVM;
    }
    

    // private Vm findSuitableVM(Task task, double currentTime) {
    //     double slackFactor = 1.3; // تا X٪ تاخیر نسبت به sub-deadline مجاز است
    //     Vm bestVM = null;
    //     double minCostGrowth = Double.MAX_VALUE;
    //     double minIdleTime = Double.MAX_VALUE;
    //     double bestStartTime = Double.MAX_VALUE;
    //     double THRESHOLD_FOR_PARALLELISM = 300;

    //     for (Vm vm : activeVMs) {
    //         if (!vm.isActive()) continue;
    
    //         double predictedStartTime = calculatePredictedStartTime(task, vm, currentTime);
    //         double predictedExecutionTime = calculatePredictedExecutionTime(task, vm);
    //         double predictedCompletionTime = predictedStartTime + predictedExecutionTime;
    
    //         if (predictedCompletionTime > task.getSubDeadline() * slackFactor) continue;
    
    //         double costPerSecond = vm.getCostPerHour() / 3600.0;
    //         double costGrowth = costPerSecond * predictedExecutionTime;
    //         double idleTime = Math.max(0.0, predictedStartTime - vm.getPredictedCompletionTime());
    
    //         // مهم: اولویت به VMهایی که زودتر آماده هستند 
    //         if (costGrowth < minCostGrowth ||
    //             (costGrowth == minCostGrowth && idleTime < minIdleTime) ||
    //             (costGrowth == minCostGrowth && idleTime == minIdleTime && predictedStartTime < bestStartTime)) {
                
    //             minCostGrowth = costGrowth;
    //             minIdleTime = idleTime;
    //             bestStartTime = predictedStartTime;
    //             bestVM = vm;
    //         }

    //         if (predictedStartTime - currentTime > THRESHOLD_FOR_PARALLELISM) {
    //             bestVM = null;
    //         }
    //     }

    //     // اگر زمان شروع خیلی دیر است، VM جدید بهتر است
    //     if (bestVM != null && minIdleTime > 0)
    //         bestVM.updateIdleTime(minIdleTime);
    

    //     return bestVM;
    // }

    // private VMType selectBestVMType(Task task, double currentTime) {
    //     return vmTypes.stream()
    //         .min((t1, t2) -> {
    //             double costPerSecond1 = t1.costPerHour / 3600.0;
    //             double costPerSecond2 = t2.costPerHour / 3600.0;
    
    //             double execTimeOnT1 = task.getCompletionTime() / t1.processingCapacity;
    //             double execTimeOnT2 = task.getCompletionTime() / t2.processingCapacity;
    
    //             double cost1 = execTimeOnT1 * costPerSecond1;
    //             double cost2 = execTimeOnT2 * costPerSecond2;
    
    //             return Double.compare(cost1, cost2); // کمینه هزینه اجرا
    //         })
    //         .orElse(vmTypes.get(0)); // fallback
    // }

    private VMType selectBestVMType(Task task, double currentTime) {
        double subDeadline = task.getSubDeadline();
        double bestCost = Double.MAX_VALUE;
        VMType bestType = null;
    
        for (VMType type : vmTypes) {
            double execTime = estimateExecutionTime(task, type);
            double predictedCompletion = currentTime + type.bootTime + execTime;
    
            if (predictedCompletion > subDeadline) continue;
    
            double costPerSecond = type.costPerHour / 3600.0;
            double taskCost = execTime * costPerSecond;
    
            // 👇 هزینه اجاره VM بر مبنای billingPeriod
            double billingPeriodCost = NOSFScheduler.getBillingPeriod() * costPerSecond;
    
            // 👇 هزینه مؤثر = هزینه حداقلی‌ای که در واقع پرداخت میشه
            double effectiveCost = Math.max(taskCost, billingPeriodCost);
    
            if (effectiveCost < bestCost) {
                bestCost = effectiveCost;
                bestType = type;
            }
        }
    
        return bestType != null ? bestType :
            vmTypes.stream().min(Comparator.comparingDouble(vm -> vm.costPerHour)).orElse(vmTypes.get(0));
    }
    
    private double estimateExecutionTime(Task task, VMType type) {
        double baseTime = task.getMeanExecutionTime(); 
        double alpha = NOSFScheduler.getVarianceFactorAlpha();      
        double eta = NOSFScheduler.getEstimationFactorEta();        
    
        double mean = baseTime;
        double std = baseTime * alpha;
        double estimated = eta * (mean + std); // همان w(λsi) × η
    
        return estimated / type.processingCapacity; // چون execTime = workload / capacity
    }
    
    public double calculatePredictedStartTime(Task task, Vm vm, double currentTime) {
        double latestPredecessorCompletion = task.getPredecessors().stream()
                .mapToDouble(pred -> pred.getCompletionTime() + pred.getDataTransferTime())
                .max().orElse(0.0);
        return Math.max(currentTime, Math.max(vm.getPredictedCompletionTime(), latestPredecessorCompletion));
    }

    public double calculatePredictedExecutionTime(Task task, Vm vm) {
        double baseExecutionTime = task.getMeanExecutionTime() + Math.sqrt(task.getVarianceExecutionTime());
        return baseExecutionTime * (NOSFScheduler.getNormalizationFactor() / vm.getProcessingCapacity());
    }

// Commented cause dont mention in Article
    public void releaseVM(Vm vm, double currentTime) {
        vm.setActive(false);
        activeVMs.remove(vm);
        LOGGER.info(String.format("Released VM %s at time %.2f", vm.getId(), currentTime));
    }

    public List<Vm> getActiveVMs() {
        return new ArrayList<>(activeVMs);
    }

    public List<Vm> getAllVMs() {
    return new ArrayList<>(allVMs);
}

    public int getVMCounter(){
        return vmCounter;
    } 
}