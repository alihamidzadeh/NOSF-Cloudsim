package org.cloudbus.cloudsim.examples.nosf;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class VMFactory {
    private static final Logger LOGGER = Logger.getLogger(VMFactory.class.getName());
    private static class VMType {
        String id;
        double processingCapacity;
        double costPerHour;
        double energyPerSecond;
        double bootTime;
    }

    private final List<VMType> vmTypes = new ArrayList<>();
    private final List<Vm> activeVMs = new ArrayList<>();
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
        if (activeVMs.size() >= maxVMs) {
            LOGGER.info("Cannot create new VM: Maximum VM limit reached.");
            return null;
        }
        Vm suitableVM = findSuitableVM(task, currentTime);
        if (suitableVM != null) {
            return suitableVM;
        }
        VMType vmType = selectBestVMType(task);
        String vmId = "vm-" + (++vmCounter);
        Vm vm = new Vm(vmId, vmType.processingCapacity, vmType.costPerHour, vmType.energyPerSecond, vmType.bootTime);
        activeVMs.add(vm);
        LOGGER.info(String.format("Created new VM %s at time %.2f with capacity %.2f MIPS", vmId, currentTime, vmType.processingCapacity));
        return vm;
    }

    private Vm findSuitableVM(Task task, double currentTime) {
        Vm bestVM = null;
        double earliestCompletionTime = Double.MAX_VALUE;
        double minIdleTime = Double.MAX_VALUE;

        for (Vm vm : activeVMs) {
            if (!vm.isActive()) continue;
            double predictedStartTime = calculatePredictedStartTime(task, vm, currentTime);
            double predictedExecutionTime = calculatePredictedExecutionTime(task, vm);
            double predictedCompletionTime = predictedStartTime + predictedExecutionTime;
            double idleTime = predictedStartTime > vm.getPredictedCompletionTime() ? 
                             predictedStartTime - vm.getPredictedCompletionTime() : 0.0;

            if (predictedCompletionTime <= task.getSubDeadline() && predictedCompletionTime < earliestCompletionTime) {
                earliestCompletionTime = predictedCompletionTime;
                minIdleTime = idleTime;
                bestVM = vm;
            } else if (predictedCompletionTime <= task.getSubDeadline() && 
                       predictedCompletionTime == earliestCompletionTime && idleTime < minIdleTime) {
                minIdleTime = idleTime;
                bestVM = vm;
            }
        }

        if (bestVM != null && minIdleTime > 0) {
            bestVM.updateIdleTime(minIdleTime);
        }
        return bestVM;
    }

    private VMType selectBestVMType(Task task) {
    return vmTypes.stream()
            .max((t1, t2) -> {
                double efficiency1 = t1.processingCapacity / t1.costPerHour;
                double efficiency2 = t2.processingCapacity / t2.costPerHour;
                return Double.compare(efficiency1, efficiency2);
            })
            .orElse(vmTypes.get(0));
    }

    public double calculatePredictedStartTime(Task task, Vm vm, double currentTime) {
        double latestPredecessorCompletion = task.getPredecessors().stream()
                .mapToDouble(pred -> pred.getCompletionTime() + pred.getDataTransferTime())
                .max().orElse(0.0);
        return Math.max(currentTime, Math.max(vm.getPredictedCompletionTime(), latestPredecessorCompletion));
    }

    public double calculatePredictedExecutionTime(Task task, Vm vm) {
        double baseExecutionTime = task.getMeanExecutionTime() + Math.sqrt(task.getVarianceExecutionTime());
        return baseExecutionTime * (2000.0 / vm.getProcessingCapacity());
    }

    public void releaseVM(Vm vm, double currentTime) {
        vm.setActive(false);
        activeVMs.remove(vm);
        LOGGER.info(String.format("Released VM %s at time %.2f", vm.getId(), currentTime));
    }

    public List<Vm> getActiveVMs() {
        return new ArrayList<>(activeVMs);
    }
}