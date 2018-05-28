package com.datacube.alter.utils;

import com.alibaba.fastjson.JSONObject;
import com.datacube.alter.domain.ExecutionFlows;
import com.datacube.alter.domain.Projects;
import com.datacube.alter.mapper.ExecutionFlowsMapper;
import com.datacube.alter.mapper.ProjectsMapper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @Author Mr.An
 * @Date 18/5/25 上午10:07
 * 常驻警报线程
 * 一个project包涵多个flow，一个flow包涵多个job，要做到job级别的alter,这个只是flow级别的等待更新
 * 以最大的提交日期为分界线区分新的job监测界线
 */
public class JobFailAlter implements Runnable {

    private final Logger LOGGER=LoggerFactory.getLogger(JobFailAlter.class);
    private int pre_max_exec_id=0;
    private static String token_str="";
    private RestTemplate restTemplate;
    @Autowired
    private ExecutionFlowsMapper executionFlowsMapper;
    @Autowired
    private ProjectsMapper projectsMapper;

    private HashMap<ExecutionFlows,Integer> rerunMap=new HashMap();

    private BlockingQueue<ExecutionFlows> waitForAlter = new ArrayBlockingQueue(10);

    private String login_url="http://127.0.0.1:8081";

    public final BlockingQueue<ExecutionFlows> wait_for_info = new ArrayBlockingQueue(10);


    /**
     * Azkaban assumes the following request header

     *   Content-Type:     application/x-www-form-urlencoded
         X-Requested-With: XMLHttpRequest
     */

    private void catchNewRunableFlows(){
        //TODO mapper新增查询大于pre_max_exec_id的新的列表，并将列表中的item加入rerunMap中去
        List<ExecutionFlows> executionJobs = executionFlowsMapper.selectNewRunnable(pre_max_exec_id);
        for (ExecutionFlows item:executionJobs){
            rerunMap.put(item,0);
        }

    }

    public void run(){
        while(true){
            for (ExecutionFlows key:rerunMap.keySet()){
                if (rerunMap.get(key)>2){
                    try {
                        wait_for_info.put(key);
                        rerunMap.remove(key);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else {

                    try {
                        int execid = rerun(key);
                        Thread.sleep(30000);//运行失败任务，等待30s
                        if (execid>pre_max_exec_id){
                            pre_max_exec_id=execid;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    rerunMap.put(key,rerunMap.get(key)+1);

                }

            }

            try {
                Thread.sleep(60000);//循环rerunMap后，休息1分钟,然后再去mysql库查询新的失败的flow，加入到rerunMap
                catchNewRunableFlows();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }
    //TODO 重新api调用运行azkban job，根据获得的
    public int rerun(ExecutionFlows executionFlows){
       return exec_flow(executionFlows);


    }

    @Before
    public void getToken(){
        restTemplate=new RestTemplate();
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap();
        linkedMultiValueMap.add("action", "login");
        linkedMultiValueMap.add("username", "azkaban");
        linkedMultiValueMap.add("password", "azkaban");
        HttpEntity httpEntity = new HttpEntity(linkedMultiValueMap, hs);
        String postForObject = restTemplate.postForObject(login_url, httpEntity, String.class);
        JSONObject jsonObject = JSONObject.parseObject(postForObject);
        if (jsonObject.getString("status").equals("success")){
            token_str=jsonObject.getString("session.id");
            LOGGER.info(token_str);
        }
    }

    public int exec_flow(ExecutionFlows executionFlows){return 1;};
    @Test
    public void execFlow(){
        ExecutionFlows executionFlows=null;
        Integer projectId = executionFlows.getProjectId();
        String flowId = executionFlows.getFlowId();
        restTemplate=new RestTemplate();
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        String exec_url=login_url+"/executor?ajax=executeFlow"+"&session.id={token_str}"+"&project={project}"
                +"&flow={flow}";
        HttpEntity httpEntity=new HttpEntity(hs);
        String projectid="32";
        Projects projects = projectsMapper.selectByPrimaryKey(Integer.valueOf(projectid));
        String project=projects.getName();
        String flow="limit11_1517478859000";
        ResponseEntity<String> exchange = restTemplate.exchange(exec_url, HttpMethod.GET, httpEntity, String.class, token_str, project, flow);
        JSONObject jsonObject = JSONObject.parseObject(exchange.getBody());
        if (jsonObject.getString("error")=="session"){
            getToken();
            exchange = restTemplate.exchange(exec_url, HttpMethod.GET, httpEntity, String.class, token_str, project, flow);
            JSONObject jsonObject2=JSONObject.parseObject(exchange.getBody());
            if (jsonObject2.getString("message").contains("successfully")){
                System.out.println("execu successful");
                String execid = jsonObject2.getString("execid");
            }

        }


        LOGGER.info(exchange.getBody()+exchange.getStatusCode());

    }


}

//TODO not finished 
