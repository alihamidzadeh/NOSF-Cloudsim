package org.cloudbus.cloudsim.examples.nosf;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        String configFile = "..\\resources\\simulation_config.xml";

        String[] workflowFiles = {"..\\resources\\workflows\\CyberShake_30.xml"};
        // String[] workflowFiles = {"..\\resources\\workflows\\Test_5.xml"};
        // String[] workflowFiles = {"..\\resources\\workflows\\Test_5.xml", "..\\resources\\workflows\\CyberShake_30.xml"};
        
        NOSFScheduler scheduler = new NOSFScheduler(configFile);
        
        // بارگذاری تمام گردش‌کارها
        List<Workflow> workflows = Workflow.loadFromXML(workflowFiles);
        for (Workflow workflow : workflows) {
            scheduler.submitWorkflow(workflow);
        }
        // اجرای شبیه‌سازی
        scheduler.runSimulation();
    }
}