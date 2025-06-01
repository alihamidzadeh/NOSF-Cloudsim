package org.cloudbus.cloudsim.examples.nosf;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        String configFile = "src/main/resources/simulation_config.xml";
        String workflowFile = "src/main/resources/workflow.xml";
        
        NOSFScheduler scheduler = new NOSFScheduler(configFile);
        
        // بارگذاری تمام گردش‌کارها
        List<Workflow> workflows = Workflow.loadFromXML(workflowFile);
        for (Workflow workflow : workflows) {
            scheduler.submitWorkflow(workflow);
        }
        
        // اجرای شبیه‌سازی
        scheduler.runSimulation();
    }
}