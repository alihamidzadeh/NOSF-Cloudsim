package org.cloudbus.cloudsim.examples.nosf;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;


public class Workflow {
    private final String id;
    private final double arrivalTime;
    private final double deadline;
    private final List<Task> tasks = new ArrayList<>();
    private final Map<String, Task> taskMap = new HashMap<>();
    private Task entryTask;
    private static int workflowCounter = 0;

    public Workflow(String id, double arrivalTime, double deadline) {
        this.id = id;
        this.arrivalTime = arrivalTime;
        this.deadline = deadline;
    }

    public static List<Workflow> loadFromXML(String[] workflowFiles) {
        List<Workflow> workflows = new ArrayList<>();
        try {
            for (int i = 0; i < workflowFiles.length; i++) {
                //NEW
                File xmlFile = new File(workflowFiles[i]);
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document document = builder.parse(xmlFile);
                document.getDocumentElement().normalize();

                Map<String, Double> jobRuntimes = parseJobsRuntimes(document);
                Map<String, List<String>> dependencies = parseDependencies(document);
                String workflowId = "wf-" + workflowCounter++;
                double PCPDeadline = computePCPDeadline_1(jobRuntimes, dependencies);
                double deadline = 2 * PCPDeadline;
                System.out.println("PCP Runtime for " + workflowId + ": " + PCPDeadline);
                double arrivalTime = NOSFScheduler.getCurrentTime();
                Workflow workflow = new Workflow(workflowId, arrivalTime, deadline);
                PCPDeadline = computePCPDeadline(workflow);
                
                // Load tasks
                parseJobTasks(document, workflow);
                
                // Load dependencies
                applyDependencies(dependencies, workflow);

                // Set entry task
                workflow.tasks.stream()
                .filter(task -> task.getPredecessors().isEmpty())
                .findFirst()
                .ifPresent(task -> workflow.entryTask = task);

                workflows.add(workflow);

                // Print Entry task
                // if (workflow.entryTask != null)
                //     System.out.println("Entry task ID: " + workflow.entryTask.getId());
                // else
                //     System.out.println("Entry task not found.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return workflows;
    }

    public static Map<String, Double> parseJobsRuntimes(Document document) {
        Map<String, Double> jobRuntimes = new HashMap<>();
        try {
            NodeList jobList = document.getElementsByTagName("job");
            for (int i = 0; i < jobList.getLength(); i++) {
                Element jobElement = (Element) jobList.item(i);
                String taskId = jobElement.getAttribute("id");
                double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));
                jobRuntimes.put(taskId, runtime);
            }
        } catch (Exception e) {
            System.out.println("Error parsing jobs: " + e);
        }
        return jobRuntimes;
    }

    public static void parseJobTasks(Document document, Workflow workflow){
        try {
            NodeList jobList = document.getElementsByTagName("job");
            for (int i = 0; i < jobList.getLength(); i++) {
                Element jobElement = (Element) jobList.item(i);
                String taskId = jobElement.getAttribute("id");
                double meanExecutionTime = Double.parseDouble(jobElement.getAttribute("runtime"));
                long totalFileSize = 0;
                NodeList usesList = jobElement.getElementsByTagName("uses");
                for (int j = 0; j < usesList.getLength(); j++) {
                    Element usesElement = (Element) usesList.item(j);
                    String linkType = usesElement.getAttribute("link");
                    String sizeUses = usesElement.getAttribute("size");
                    if (linkType.equals("input") && !sizeUses.isEmpty())
                        totalFileSize += Long.parseLong(sizeUses);
                }
                double varianceExecutionTime = Math.pow(NOSFScheduler.getVarianceFactorAlpha() * meanExecutionTime, 2);
                double dataTransferTime = (totalFileSize) / (NOSFScheduler.getBandwidthMbps() * 1_000_000.0); // bandwidth تبدیل به bit/sec
                
                Task task = new Task(taskId, meanExecutionTime, varianceExecutionTime, dataTransferTime, workflow);
                workflow.addTask(task);
                // System.out.println("info: " + taskId+ " - "+ meanExecutionTime+ " - "+ varianceExecutionTime+ " - "+ dataTransferTime);
            }
        } catch (Exception e) {
            System.out.println("Error parsing tasks: " + e);
        }
    }

    public static Map<String, List<String>> parseDependencies(Document document) {
        Map<String, List<String>> dependencies = new HashMap<>();
        try {
            NodeList childList = document.getElementsByTagName("child");
            for (int i = 0; i < childList.getLength(); i++) {
                Element childElement = (Element) childList.item(i);
                String childId = childElement.getAttribute("ref");

                List<String> parentList = new ArrayList<>();
                NodeList parentNodes = childElement.getElementsByTagName("parent");
                for (int j = 0; j < parentNodes.getLength(); j++) {
                    Element parentElement = (Element) parentNodes.item(j);
                    parentList.add(parentElement.getAttribute("ref"));
                }

                dependencies.put(childId, parentList);
            }
        } catch (Exception e) {
            System.out.println("Error parsing dependencies: " + e);
        }
        return dependencies;
    }

    public static void applyDependencies(Map<String, List<String>> dependencies, Workflow workflow) {
        for (Map.Entry<String, List<String>> entry : dependencies.entrySet()) {
            String childId = entry.getKey();
            Task toTask = workflow.getTaskById(childId);
    
            for (String parentId : entry.getValue()) {
                Task fromTask = workflow.getTaskById(parentId);
                toTask.addPredecessor(fromTask);
                // System.out.println("Parent: " + fromTask.getId() + " - Child: " +toTask.getId());
            }
        }
    }

    public static double computePCPDeadline_1(Map<String, Double> jobs, Map<String, List<String>> dependencies) {
    // Compute reverse graph
    Map<String, List<String>> reverseGraph = new HashMap<>();
    Map<String, Integer> inDegree = new HashMap<>();
    Map<String, Double> earliestStart = new HashMap<>();

    // Initialize
    for (String job : jobs.keySet()) {
        reverseGraph.put(job, new ArrayList<>());
        inDegree.put(job, 0);
        earliestStart.put(job, 0.0);
    }

    // Fill graph
    for (Map.Entry<String, List<String>> entry : dependencies.entrySet()) {
        String child = entry.getKey();
        for (String parent : entry.getValue()) {
            reverseGraph.get(parent).add(child);
            inDegree.put(child, inDegree.get(child) + 1);
        }
    }

    // Topological sort (Kahn's Algorithm)
    Queue<String> queue = new LinkedList<>();
    for (String job : inDegree.keySet()) {
        if (inDegree.get(job) == 0) {
            queue.add(job);
        }
    }

    while (!queue.isEmpty()) {
        String current = queue.poll();
        double finishTime = earliestStart.get(current) + jobs.get(current);

        for (String child : reverseGraph.get(current)) {
            earliestStart.put(child, Math.max(earliestStart.get(child), finishTime));
            inDegree.put(child, inDegree.get(child) - 1);
            if (inDegree.get(child) == 0) {
                queue.add(child);
            }
        }
    }

    // Compute max deadline
    double maxDeadline = 0;
    for (String job : jobs.keySet()) {
        maxDeadline = Math.max(maxDeadline, earliestStart.get(job) + jobs.get(job));
    }
    return maxDeadline;
}

    /**
     * طول مسیر بحرانی (طولانی‌ترین مسیر) در ورک‌فلو را محاسبه می‌کند.
     * این متد از یک رویکرد بازگشتی به همراه مموایزیشن (memoization) برای جلوگیری از محاسبات تکراری استفاده می‌کند.
     * مقدار بازگشتی، مجموع زمان اجرای تسک‌ها در طولانی‌ترین مسیر از یک تسک ورودی تا یک تسک خروجی است.
     *
     * @param workflow ورک‌فلویی که باید مسیر بحرانی آن محاسبه شود.
     * @return طول مسیر بحرانی (makespan) به عنوان یک مقدار double.
     */
    public static double computePCPDeadline(Workflow workflow) {
        // از یک نقشه برای ذخیره نتایج محاسبات قبلی استفاده می‌کنیم (Memoization)
        Map<String, Double> memo = new HashMap<>();
        double maxDeadline = 0.0;

        // طولانی‌ترین مسیر را برای هر تسک خروجی (exit task) محاسبه می‌کنیم
        for (Task task : workflow.getTasks()) {
            if (task.getSuccessors().isEmpty()) {
                double pathLength = getLongestPathTo(task, memo);
                if (pathLength > maxDeadline) {
                    maxDeadline = pathLength;
                }
            }
        }
        return maxDeadline;
    }

    /**
     * یک متد کمکی بازگشتی برای پیدا کردن طولانی‌ترین مسیر تا یک تسک مشخص.
     *
     * @param task تسکی که طولانی‌ترین مسیر تا آن باید محاسبه شود.
     * @param memo نقشه‌ای برای ذخیره و بازیابی نتایج محاسبات قبلی.
     * @return طولانی‌ترین مسیر تا تسک مشخص شده.
     */
    private static double getLongestPathTo(Task task, Map<String, Double> memo) {
        // اگر قبلاً طول مسیر برای این تسک محاسبه شده، از مقدار ذخیره‌شده استفاده کن
        if (memo.containsKey(task.getId())) {
            return memo.get(task.getId());
        }

        // محاسبه طول مسیر برای پدران (predecessors)
        double maxPredPathLength = 0.0;
        if (!task.getPredecessors().isEmpty()) {
            maxPredPathLength = task.getPredecessors().stream()
                    .mapToDouble(pred -> getLongestPathTo(pred, memo))
                    .max()
                    .orElse(0.0);
        }

        // طول مسیر تا تسک فعلی برابر است با زمان اجرای خودش + طولانی‌ترین مسیر پدرانش
        double currentPathLength = task.getMeanExecutionTime() + maxPredPathLength;

        // نتیجه را در نقشه ذخیره کن تا دوباره محاسبه نشود
        memo.put(task.getId(), currentPathLength);

        return currentPathLength;
    }

    public void addTask(Task task) {
        tasks.add(task);
        taskMap.put(task.getId(), task);
    }

    public Task getTaskById(String id) {
        return taskMap.get(id);
    }

    public String getId() {
        return id;
    }

    public double getArrivalTime() {
        return arrivalTime;
    }

    public double getDeadline() {
        return deadline;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public Task getEntryTask() {
        return entryTask;
    }

    public double getMakespan() {
        return tasks.stream()
                .mapToDouble(Task::getCompletionTime)
                .max()
                .orElse(0.0);
    }

    public boolean isCompleted() {
        return tasks.stream().allMatch(task -> task.getCompletionTime() > 0);
    }

    public boolean hasDeadlineViolation() {
        return getMakespan() > deadline;
    }

    // دو فیلد جدید برای کش کردن مقادیر محاسبه‌شده
    private double criticalPathLength = -1;
    private double totalExecutionTime = -1;

    public double getCriticalPathLength() {
        if (this.criticalPathLength == -1) {
            // از متد قبلی شما که PCPDeadline نام داشت استفاده می‌کنیم
            this.criticalPathLength = Workflow.computePCPDeadline(this);
        }
        return this.criticalPathLength;
    }

    public double getTotalExecutionTime() {
        if (this.totalExecutionTime == -1) {
            this.totalExecutionTime = getTasks().stream()
                .mapToDouble(t -> t.getMeanExecutionTime() + Math.sqrt(t.getVarianceExecutionTime()))
                .sum();
        }
        return this.totalExecutionTime;
    }

}