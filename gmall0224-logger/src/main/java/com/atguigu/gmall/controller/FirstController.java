package com.atguigu.gmall.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: Felix
 * Date: 2021/7/28
 * Desc: 回顾SpringMVC    Controller
 * @Controller  将类对象的创建交给Spring容器，但是用Controller标记的类，如果方法的返回值是String，
 *          那么认为是进行页面的跳转；如果要是将字符串直接响应的话，需要在方法上加@ResponseBody
 *
 * @RestController = @Controller + @ResponseBody
 */
@RestController
public class FirstController {
    @RequestMapping("/first")
    public String first(@RequestParam("haha") String username,
                        @RequestParam("heihei") String password){
        System.out.println(username +":::" + password);
        return "success";
    }
}