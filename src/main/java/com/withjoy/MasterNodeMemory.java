/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.withjoy;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 * @author akiramadono
 */
public class MasterNodeMemory {
    public static void main(String args[]){
        final ParameterTool params = ParameterTool.fromArgs(args);
       
        BatchFlinkJob job = new BatchFlinkJob(params);
        
    }
}
