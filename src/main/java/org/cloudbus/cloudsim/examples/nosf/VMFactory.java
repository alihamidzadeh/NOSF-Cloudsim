package org.cloudbus.cloudsim.examples.nosf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.Iterator;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class VMFactory {
    private static final Logger LOGGER = Logger.getLogger(VMFactory.class.getName());
    
    // --- حذف شد: لاگر تکراری بود و در کلاس اصلی تعریف شده بود ---

    private static class VMType {
        String id;
        double processingCapacity;
        double costPerHour;
        double energyPerSecond;
        double bootTime;
    }

    private final List<VMType> vmTypes = new ArrayList<>();
    private final List<Vm> activeVMs = new ArrayList<>();
    private final List<Vm> allVMs = new ArrayList<>();
    private final Random random = new Random(); // برای شبیه‌سازی نوسان عملکرد

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
    
    // --- اصلاح شد: نام متد برای وضوح بیشتر تغییر کرد ---
    public Vm findOrCreateVM(Task task, double currentTime) {
        Vm suitableVM = findSuitableVM(task, currentTime);
        if (suitableVM != null) {
            return suitableVM;
        }

        if (activeVMs.size() >= maxVMs) {
            LOGGER.warning("Cannot create new VM: Maximum VM limit reached.");
            return null;
        }
        
        VMType vmType = selectBestVMTypeForNewLease(task, currentTime);
        if (vmType == null) {
            LOGGER.warning("No suitable new VMType found for task " + task.getId());
            return null;
        }

        String vmId = "vm-" + (++vmCounter);
        Vm vm = new Vm(vmId, vmType.id, vmType.processingCapacity, vmType.costPerHour, vmType.energyPerSecond, vmType.bootTime);
        vm.setLeaseStartTime(currentTime); // زمان شروع اجاره
        vm.setNextReleaseCheckTime(currentTime + NOSFScheduler.getBillingPeriod());
        activeVMs.add(vm);
        allVMs.add(vm);
        LOGGER.info(String.format("Created new VM %s (Type: %s) at time %.2f. Booting...", vmId, vmType.id, currentTime));
        return vm;
    }

    private Vm findSuitableVM(Task task, double currentTime) {
        Vm bestVM = null;
        double minCostGrowth = Double.MAX_VALUE;
    
        for (Vm vm : activeVMs) {
            // ابتدا زمان شروع و پایان پیش‌بینی‌شده را محاسبه می‌کنیم
            double predictedStartTime = calculatePredictedStartTime(task, vm, currentTime);
            double predictedExecutionTime = calculatePredictedExecutionTime(task, vm);
            double predictedCompletionTime = predictedStartTime + predictedExecutionTime;
    
            // اگر این VM تا زمان شروع پیش‌بینی‌شده آزاد نباشد، حذفش کن
            if (vm.getAvailableTime(currentTime) > predictedStartTime) {
                continue;
            }
    
            // اگر نتواند قبل از زیرمهلت تمامش کند، کنار بگذار
            if (predictedCompletionTime > task.getSubDeadline()) {
                continue;
            }
    
            // میزان رشد هزینه پس از اتمام دوره‌ٔ صورتحساب
            double remainingBillingTime = vm.getRemainingBillingTime(predictedStartTime);
            double costGrowth = vm.getCostForDuration(Math.max(0, predictedExecutionTime - remainingBillingTime));
    
            // کمترین رشد هزینه را انتخاب کن (در صورت تساوی، VM با کمتر بودن زمان بیکاری)
            if (costGrowth < minCostGrowth
                || (costGrowth == minCostGrowth && (bestVM == null || vm.getTotalIdleTime() < bestVM.getTotalIdleTime()))
            ) {
                minCostGrowth = costGrowth;
                bestVM = vm;
            }
        }
    
        if (bestVM != null) {
            LOGGER.info("Found suitable existing VM " + bestVM.getId() + " for task " + task.getId());
        }
        return bestVM;
    }

    // --- اصلاح شد: نام متد برای وضوح بیشتر تغییر کرد ---
    private VMType selectBestVMTypeForNewLease(Task task, double currentTime) {
        VMType bestType = null;
        double minCost = Double.MAX_VALUE;

        for (VMType type : vmTypes) {
            double predictedExecTime = (task.getMeanExecutionTime() / type.processingCapacity) * NOSFScheduler.getNormalizationFactor();
            double predictedCompletionTime = currentTime + type.bootTime + predictedExecTime;

            // اگر حتی سریع‌ترین VM هم نتواند در زیرمهلت کار را تمام کند، آن را در نظر نگیر
            if (predictedCompletionTime > task.getSubDeadline()) {
                continue;
            }

            double costForTask = (Math.ceil((type.bootTime + predictedExecTime) / NOSFScheduler.getBillingPeriod())) *
                                (type.costPerHour / 3600.0) * NOSFScheduler.getBillingPeriod();
            
            if (costForTask < minCost) {
                minCost = costForTask;
                bestType = type;
            }
        }
        
        // --- اصلاح شد: اگر هیچ نوعی مناسب نبود، سریعترین نوع را به عنوان آخرین راه حل انتخاب کن ---
        if (bestType == null) {
             bestType = vmTypes.stream().max(Comparator.comparingDouble(t -> t.processingCapacity)).orElse(null);
        }

        return bestType;
    }

    public double calculatePredictedStartTime(Task task, Vm vm, double currentTime) {
        // برای هر پیشینی، اگر روی همین VM اجرا شده باشه، فقط منتظر اتمامش می‌مونیم
        double dataReadyTime = task.getPredecessors().stream()
            .mapToDouble(pred -> 
                pred.getAssignedVM() != null && pred.getAssignedVM().equals(vm)
                    ? pred.getCompletionTime()
                    : pred.getCompletionTime() + pred.getDataTransferTime(task)
            )
            .max()
            .orElse(currentTime);
    
        double vmReadyTime = vm.getAvailableTime(currentTime);
        return Math.max(dataReadyTime, vmReadyTime);
    }
    
    // --- اصلاح شد: این متد زمان واقعی اجرا را با کمی نوسان شبیه‌سازی می‌کند ---
    public double calculatePredictedExecutionTime(Task task, Vm vm) {
        double meanExecutionOnVm = (task.getMeanExecutionTime() / vm.getProcessingCapacity()) * NOSFScheduler.getNormalizationFactor();
        double stdDev = meanExecutionOnVm * NOSFScheduler.getVarianceFactorAlpha();
        
        // تولید یک عدد تصادفی با توزیع نرمال برای شبیه‌سازی نوسان عملکرد
        double actualExecutionTime = random.nextGaussian() * stdDev + meanExecutionOnVm;

        return Math.max(0.1, actualExecutionTime); // حداقل زمان اجرا برای جلوگیری از مقادیر منفی
    }

    public void releaseVM(Vm vm, double currentTime) {
        if (vm.isActive()) {
            vm.setLeaseEndTime(currentTime);
            vm.setActive(false);
            activeVMs.remove(vm);
            LOGGER.info(String.format("Released VM %s at time %.2f", vm.getId(), currentTime));
        }
    }
    
    // --- جدید: متدی برای پیدا کردن زمان اتمام بعدی در شبیه‌سازی ---
    public double getNextVmCompletionTime(double currentTime) {
        return activeVMs.stream()
                .flatMap(vm -> vm.getRunningTasks().stream())
                .mapToDouble(Task::getCompletionTime)
                .filter(t -> t > currentTime)
                .min()
                .orElse(Double.MAX_VALUE);
    }

    // --- جدید: متدی برای بروزرسانی وضعیت VMها و گرفتن تسک‌های تمام شده ---
    public List<Task> updateVmsAndGetCompletedTasks(double currentTime) {
        List<Task> completedTasks = new ArrayList<>();
        for (Vm vm : activeVMs) {
            completedTasks.addAll(vm.updateStatus(currentTime));
        }
        return completedTasks;
    }

    public List<Vm> getActiveVMs() {
        return new ArrayList<>(activeVMs);
    }

    public List<Vm> getAllVMs() {
        return new ArrayList<>(allVMs);
    }

    public int getVMCounter() {
        return vmCounter;
    }

    /**
     * هر بار که clock جلو می‌رود (مثلاً پس از هر تسک یا event)، این را صدا بزن.
     * VMهایی که به نقطهٔ n×billingPeriod رسیده و در آن لحظه idle هستند را release می‌کند.
     */
    public void checkIdleVMs(double currentTime) {
        for (Iterator<Vm> iterator = activeVMs.iterator(); iterator.hasNext();) {
            Vm vm = iterator.next();
            double scheduledTime = vm.getNextReleaseCheckTime();
            // تا زمانی که currentTime از nextReleaseCheckTime بگذرد
            // while (currentTime >= scheduledTime) {
            if (currentTime >= scheduledTime) {
                if (vm.getRunningTasks().isEmpty()) {
                    LOGGER.info("Debug ==> vm.getRunningTasks on VM: " + vm.getRunningTasks().toString());
                    // آزادسازی در همان لحظهٔ برنامه‌ریزی‌شده
                    LOGGER.info(
                        "Releasing idle VM " + vm.getId() +
                        " at time " + currentTime +
                        " (idle since last task end)"
                      );
                    releaseVM(vm, currentTime);
                    iterator.remove();  // از activeVMs هم حذف کن
                    break;  // این VM دیگر فعال نیست
                } else {
                    // هنوز تسک داشته؛ یک ساعت دیگر صبر کن
                    LOGGER.info(
                        "VM " + vm.getId() +
                        " still busy at time " + currentTime + ", NextReleaseCheckTime " + vm.getNextReleaseCheckTime() +
                        ", delaying release to next billing period"
                      );
                    vm.advanceNextReleaseCheckTime(NOSFScheduler.getBillingPeriod());
                }
            }
        }
    }
}