package gciantispider_web;

import java.util.Set;

import redis.clients.jedis.Jedis;

public class TestRedis {
@SuppressWarnings("resource")
public static void main(String[] args) {
	Jedis jedis1 = new Jedis("192.168.80.81",7001);
	Jedis jedis2 = new Jedis("192.168.80.81",7002);
	Jedis jedis3 = new Jedis("192.168.80.81",7003);
	Jedis jedis4 = new Jedis("192.168.80.81",7004);
	Jedis jedis5 = new Jedis("192.168.80.81",7005);
	Jedis jedis6 = new Jedis("192.168.80.81",7006);
	Set<String> keys1 = jedis1.keys("CSANTI_MONITOR_DP*");
	Set<String> keys2 = jedis2.keys("CSANTI_MONITOR_DP*");
	Set<String> keys3 = jedis3.keys("CSANTI_MONITOR_DP*");
	Set<String> keys4 = jedis4.keys("CSANTI_MONITOR_DP*");
	Set<String> keys5 = jedis5.keys("CSANTI_MONITOR_DP*");
	Set<String> keys6 = jedis6.keys("CSANTI_MONITOR_DP*");
	for (String string : keys1) {
		jedis1.del(string);
	}
	for (String string : keys2) {
		jedis2.del(string);
	}
	for (String string : keys3) {
		jedis3.del(string);
	}
	for (String string : keys4) {
		jedis4.del(string);
	}
	for (String string : keys5) {
		jedis5.del(string);
	}
	for (String string : keys6) {
		jedis6.del(string);
	}
	jedis1.close();
	jedis2.close();
	jedis3.close();
	jedis4.close();
	jedis5.close();
	jedis6.close();
}
}
