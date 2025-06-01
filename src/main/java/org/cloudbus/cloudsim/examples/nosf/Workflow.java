package org.cloudbus.cloudsim.examples.nosf;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Workflow {
    private final String id;
    private final double arrivalTime;
    private final double deadline;
    private final List<Task> tasks = new ArrayList<>();
    private final Map<String, Task> taskMap = new HashMap<>();
    private Task entryTask;

    public Workflow(String id, double arrivalTime, double deadline) {
        this.id = id;
        this.arrivalTime = arrivalTime;
        this.deadline = deadline;
    }

    public static List<Workflow> loadFromXML(String file) {
        List<Workflow> workflows = new ArrayList<>();
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file);
            NodeList workflowNodes = doc.getElementsByTagName("workflow");
            for (int i = 0; i < workflowNodes.getLength(); i++) {
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