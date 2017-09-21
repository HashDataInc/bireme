package cn.hashdata.bireme.provider;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TestException {
  public TestException() {
    String s =
        "{\"PM_SEQ_NO\":null,\"BIZ_PM_SEQ_NO\":\"2017092004018920133\",\"RELATE_PM_SEQ_NO\":null,\"PM_TYPE\":\"FT\",\"GMT_PM\":\"2017-09-20 03:55:44\",\"GMT_MODIFIED\":\"2017-09-20 03:54:43\",\"GMT_CREATE\":\"2017-09-20 03:54:43\",\"MEMO\":\"2017092011552570735005004\",\"PAY_CHANNEL\":\"01\",\"BIZ_PRODUCT_CODE\":\"20201\"}";
    JsonParser jsonParser = new JsonParser();
    JsonObject value = (JsonObject) jsonParser.parse(s);
    JsonElement element = value.get("PM_SEQ_NO");
    System.out.println(element.isJsonNull());
    element = value.get("aaa");
    System.out.println(element.isJsonNull());
  }

  boolean testEx() throws Exception {
    boolean ret = true;
    try {
      ret = testEx1();
      throw new Exception();
    } catch (Exception e) {
      System.out.println("testEx, catch exception");
      ret = false;
      throw e;
    }
  }

  boolean testEx1() throws Exception {
    boolean ret = true;
    try {
      ret = testEx2();
      if (!ret) {
        return false;
      }
      System.out.println("testEx1, at the end of try");
      return ret;
    } catch (Exception e) {
      System.out.println("testEx1, catch exception");
      ret = false;
      throw e;
    } finally {
      System.out.println("testEx1, finally; return value=" + ret);
    }
  }

  boolean testEx2() throws Exception {
    boolean ret = true;
    try {
      int b = 12;
      int c;
      for (int i = 2; i >= -2; i--) {
        c = b / i;
        System.out.println("i=" + i);
      }
      return true;
    } catch (Exception e) {
      System.out.println("testEx2, catch exception");
      ret = false;
      throw e;
    } finally {
      System.out.println("testEx2, finally; return value=" + ret);
      return ret;
    }
  }

  public static void main(String[] args) {
    TestException testException1 = new TestException();
    try {
      testException1.testEx();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
