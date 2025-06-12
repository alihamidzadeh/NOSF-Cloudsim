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
            //OLD
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(workflowFiles[0]);
            NodeList workflowNodes = doc.getElementsByTagName("workflow");
            //
            for (int i = 0; i < workflowFiles.length; i++) {
                //NEW
                Map<String, Double> jobRuntimes = parseJobs(workflowFiles[i]);
                Map<String, List<String>> dependencies = parseDependencies(workflowFiles[i]);
                double deadline2 = computePCPDeadline(jobRuntimes, dependencies);
                double arrivalTime2 = NOSFScheduler.getCurrentTime();
                String workflowId2 = "wf-" + workflowCounter++;
                System.out.println("Info: "+ workflowId2 + "-" + arrivalTime2 + "-" + deadline2);
                Workflow workflow2 = new Workflow(workflowId2, arrivalTime2, deadline2);
                //

                //OLD
                Element workflowElement = (Element) workflowNodes.item(i);
                String workflowId = workflowElement.getAttribute("id");
                double arrivalTime = Double.parseDouble(workflowElement.getAttribute("arrivalTime"));
                double deadline = Double.parseDouble(workflowElement.getAttribute("deadline"));
                Workflow workflow = new Workflow(workflowId, arrivalTime, deadline);

                // Load tasks
                NodeList taskNodes = workflowElement.getElementsByTagName("task");
                for (int j = 0; j < taskNodes.getLength(); j++) {
                    Element taskElement = (Element) taskNodes.item(j);
                    String taskId = taskElement.getAttribute("id");
                    double meanExecutionTime = Double.parseDouble(taskElement.getAttribute("meanExecutionTime"));
                    double varianceExecutionTime = Double.parseDouble(taskElement.getAttribute("varianceExecutionTime"));
                    double dataTransferTime = Double.parseDouble(taskElement.getAttribute("dataTransferTime"));
                    Task task = new Task(taskId, meanExecutionTime, varianceExecutionTime, dataTransferTime, workflow);
                    workflow.addTask(task);
                }

                // Load dependencies
                NodeList dependencyNodes = workflowElement.getElementsByTagName("dependency");
                for (int j = 0; j < dependencyNodes.getLength(); j++) {
                    Element depElement = (Element) dependencyNodes.item(j);
                    String fromId = depElement.getAttribute("from");
                    String toId = depElement.getAttribute("to");
                    Task fromTask = workflow.getTaskById(fromId);
                    Task toTask = workflow.getTaskById(toId);
                    toTask.addPredecessor(fromTask);
                }

                // Set entry task
                workflow.tasks.stream()
                        .filter(task -> task.getPredecessors().isEmpty())
                        .findFirst()
                        .ifPresent(task -> workflow.entryTask = task);

                workflows.add(workflow);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return workflows;
    }

    public static Map<String, Double> parseJobs(String path) {
    Map<String, Double> jobRuntimes = new HashMap<>();
    try {
        File xmlFile = new File(path);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        document.getDocumentElement().normalize();

        NodeList jobList = document.getElementsByTagName("job");
        for (int i = 0; i < jobList.getLength(); i++) {
            Element jobElement = (Element) jobList.item(i);
            String id = jobElement.getAttribute("id");
            double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));
            jobRuntimes.put(id, runtime);
        }
    } catch (Exception e) {
        System.out.println("Error parsing jobs: " + e);
    }
    return jobRuntimes;
}

    public static Map<String, List<String>> parseDependencies(String path) {
        Map<String, List<String>> dependencies = new HashMap<>();
        try {
            File xmlFile = new File(path);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(xmlFile);
            document.getDocumentElement().normalize();

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

    public static double computePCPDeadline(Map<String, Double> jobs, Map<String, List<String>> dependencies) {
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
}