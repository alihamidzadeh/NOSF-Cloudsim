package org.cloudbus.cloudsim.examples.nosf;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        // String configFile = "src\\main\\resources\\simulation_config.xml";
        String configFile = "E:\\University\\Term10\\Cloud - Project\\NOSF\\NOSF\\cloudsim\\modules\\cloudsim-examples\\src\\main\\resources\\simulation_config.xml";


        String[] workflowFiles = {"E:\\University\\Term10\\Cloud - Project\\NOSF\\NOSF\\cloudsim\\modules\\cloudsim-examples\\src\\main\\resources\\workflows\\CyberShake_30.xml"};
        
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