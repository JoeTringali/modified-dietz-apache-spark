package com.tricosoft.app.test

import com.tricosoft.app.CalculateModifiedDietz
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit._
import Assert._

@Test
class CalculateModifiedDietzTest
{
  var sc: SparkContext = _
  
  @Before
  def initialize()
  {
    val config = new SparkConf().setAppName("CalculateModifiedDietz").setMaster("local")
    sc = new SparkContext(config)
  }
  
  @After
  def tearDown()
  {
    sc.stop() 
  }
  
  @Test
  def testCalculateModifiedTest()
  {
    val job = new CalculateModifiedDietz(sc)
    val result = job.run(
        "/Users/hadoop/Documents/workspace/CalculateModifiedDietzInScala/files/input/balance/", 
        "/Users/hadoop/Documents/workspace/CalculateModifiedDietzInScala/files/input/flow")
    assertTrue(result.count() == 4833);
  }
}