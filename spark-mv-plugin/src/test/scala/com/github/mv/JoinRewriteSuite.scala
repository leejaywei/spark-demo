package com.github.mv

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for JOIN query rewriting against materialized views.
 *
 * Covers StarRocks-style MV join rewrite capabilities:
 *   Exact join match       – query's join tree matches MV's join tree
 *   Join + aggregate match – aggregated join query served by MV
 *   Join + predicate comp. – extra WHERE on join query compensated
 *   Join + column subset   – query selects fewer columns from join
 *   Negative cases         – different join type, different tables, etc.
 */
class JoinRewriteSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private var warehouseDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehouseDir = Files.createTempDirectory("mv-join-test-warehouse").toFile
    spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.sql.extensions", classOf[MaterializedViewExtensions].getName)
      .config("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${warehouseDir.getAbsolutePath}/metastore_db;create=true")
      .enableHiveSupport()
      .getOrCreate()

    // Create source tables: orders and customers
    spark.sql(
      """CREATE TABLE orders (
        |  order_id INT,
        |  customer_id INT,
        |  amount DOUBLE,
        |  region STRING
        |) USING parquet""".stripMargin)

    spark.sql(
      """INSERT INTO orders VALUES
        |  (1, 101, 100.0, 'east'),
        |  (2, 102, 200.0, 'west'),
        |  (3, 101, 150.0, 'east'),
        |  (4, 103, 300.0, 'west'),
        |  (5, 102, 250.0, 'east')""".stripMargin)

    spark.sql(
      """CREATE TABLE customers (
        |  id INT,
        |  name STRING,
        |  city STRING
        |) USING parquet""".stripMargin)

    spark.sql(
      """INSERT INTO customers VALUES
        |  (101, 'Alice', 'new_york'),
        |  (102, 'Bob', 'sf'),
        |  (103, 'Charlie', 'la')""".stripMargin)
  }

  override def afterEach(): Unit = {
    MaterializedViewCatalog.listAll().foreach { meta =>
      try { spark.sql(s"DROP TABLE IF EXISTS ${meta.backingTable}") } catch { case _: Exception => }
    }
    MaterializedViewCatalog.clear()
    super.afterEach()
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.sql("DROP TABLE IF EXISTS orders")
        spark.sql("DROP TABLE IF EXISTS customers")
        spark.stop()
      }
    } finally {
      deleteRecursively(warehouseDir)
      super.afterAll()
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file != null && file.exists()) {
      if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
      file.delete()
    }
  }

  // ══════════════════════════════════════════════════════════════
  //  EXACT JOIN MATCH
  // ══════════════════════════════════════════════════════════════

  test("Join rewrite: exact inner join match redirects to MV") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_order_customer AS
        |SELECT o.order_id, o.amount, c.name, c.city
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_order_customer").get

    // Same query should be rewritten
    val result = spark.sql(
      """SELECT o.order_id, o.amount, c.name, c.city
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val planStr = result.queryExecution.analyzed.toString()
    assert(planStr.contains(meta.backingTable),
      s"Expected join rewrite to reference ${meta.backingTable}, got: $planStr")
  }

  test("Join rewrite: correct results for inner join match") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_oc_result AS
        |SELECT o.order_id, o.amount, c.name, c.city
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val rows = spark.sql(
      """SELECT o.order_id, o.amount, c.name, c.city
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |ORDER BY o.order_id""".stripMargin).collect()

    assert(rows.length == 5)
    // order 1: Alice, new_york, 100.0
    val row1 = rows.find(_.getInt(0) == 1).get
    assert(row1.getDouble(1) == 100.0)
    assert(row1.getString(2) == "Alice")
  }

  // ══════════════════════════════════════════════════════════════
  //  JOIN WITH PREDICATE COMPENSATION
  // ══════════════════════════════════════════════════════════════

  test("Join rewrite: predicate compensation applies extra WHERE on MV scan") {
    // MV has no WHERE filter
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_oc_all AS
        |SELECT o.order_id, o.amount, o.region, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_oc_all").get

    // Query adds a WHERE filter
    val rows = spark.sql(
      """SELECT o.order_id, o.amount, o.region, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |WHERE o.region = 'east'""".stripMargin).collect()

    // Should return only east orders (orders 1, 3, 5)
    assert(rows.length == 3)
    assert(rows.forall(_.getString(2) == "east"))
  }

  // ══════════════════════════════════════════════════════════════
  //  JOIN WITH COLUMN SUBSET
  // ══════════════════════════════════════════════════════════════

  test("Join rewrite: column subset selects fewer columns from join MV") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_oc_full AS
        |SELECT o.order_id, o.amount, o.region, c.name, c.city
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_oc_full").get

    // Query selects only a subset of columns
    val result = spark.sql(
      """SELECT o.order_id, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val planStr = result.queryExecution.analyzed.toString()
    assert(planStr.contains(meta.backingTable),
      s"Expected column subset join rewrite to reference ${meta.backingTable}, got: $planStr")

    val rows = result.collect()
    assert(rows.length == 5)
    assert(rows(0).schema.fieldNames.length == 2)
  }

  // ══════════════════════════════════════════════════════════════
  //  JOIN + AGGREGATE MATCH
  // ══════════════════════════════════════════════════════════════

  test("Join + aggregate rewrite: aggregated join query uses MV") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_oc_agg AS
        |SELECT c.city, SUM(o.amount) AS total, COUNT(o.amount) AS cnt
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |GROUP BY c.city""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_oc_agg").get

    val result = spark.sql(
      """SELECT c.city, SUM(o.amount) AS total, COUNT(o.amount) AS cnt
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |GROUP BY c.city""".stripMargin)

    val planStr = result.queryExecution.analyzed.toString()
    assert(planStr.contains(meta.backingTable),
      s"Expected join+agg rewrite to reference ${meta.backingTable}, got: $planStr")
  }

  test("Join + aggregate rewrite: correct results") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_oc_agg2 AS
        |SELECT c.city, SUM(o.amount) AS total, COUNT(o.amount) AS cnt
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |GROUP BY c.city""".stripMargin)

    val rows = spark.sql(
      """SELECT c.city, SUM(o.amount) AS total, COUNT(o.amount) AS cnt
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |GROUP BY c.city
        |ORDER BY c.city""".stripMargin).collect()

    assert(rows.length == 3)
    // la: order 4 (300.0), count=1
    val laRow = rows.find(_.getString(0) == "la").get
    assert(laRow.getDouble(1) == 300.0)
    assert(laRow.getLong(2) == 1L)
    // new_york: orders 1 (100.0) + 3 (150.0) = 250.0, count=2
    val nyRow = rows.find(_.getString(0) == "new_york").get
    assert(nyRow.getDouble(1) == 250.0)
    assert(nyRow.getLong(2) == 2L)
    // sf: orders 2 (200.0) + 5 (250.0) = 450.0, count=2
    val sfRow = rows.find(_.getString(0) == "sf").get
    assert(sfRow.getDouble(1) == 450.0)
    assert(sfRow.getLong(2) == 2L)
  }

  // ══════════════════════════════════════════════════════════════
  //  NEGATIVE TESTS
  // ══════════════════════════════════════════════════════════════

  test("No rewrite: different join tables") {
    spark.sql("CREATE TABLE other_customers (id INT, name STRING) USING parquet")
    spark.sql("INSERT INTO other_customers VALUES (101, 'Zara')")

    spark.sql(
      """CREATE MATERIALIZED VIEW mv_oc_diff AS
        |SELECT o.order_id, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_oc_diff").get

    // Query joins with a different table
    val result = spark.sql(
      """SELECT o.order_id, c.name
        |FROM orders o
        |INNER JOIN other_customers c ON o.customer_id = c.id""".stripMargin)

    val planStr = result.queryExecution.analyzed.toString()
    assert(!planStr.contains(meta.backingTable),
      s"Should NOT reference ${meta.backingTable} for different join tables, got: $planStr")

    spark.sql("DROP TABLE IF EXISTS other_customers")
  }

  test("No rewrite: non-join query does not match join MV") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_join_only AS
        |SELECT o.order_id, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_join_only").get

    // Simple single-table query should NOT match the join MV
    val result = spark.sql("SELECT order_id, amount FROM orders")
    val planStr = result.queryExecution.analyzed.toString()
    assert(!planStr.contains(meta.backingTable),
      s"Should NOT reference join MV for single-table query, got: $planStr")
  }

  test("No rewrite: MV has restrictive WHERE that join query doesn't") {
    spark.sql(
      """CREATE MATERIALIZED VIEW mv_join_east AS
        |SELECT o.order_id, o.amount, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id
        |WHERE o.region = 'east'""".stripMargin)

    val meta = MaterializedViewCatalog.get("mv_join_east").get

    // Query without the WHERE filter should NOT use the MV
    val result = spark.sql(
      """SELECT o.order_id, o.amount, c.name
        |FROM orders o
        |INNER JOIN customers c ON o.customer_id = c.id""".stripMargin)

    val planStr = result.queryExecution.analyzed.toString()
    assert(!planStr.contains(meta.backingTable),
      s"Should NOT use join MV with more restrictive WHERE, got: $planStr")
  }
}
