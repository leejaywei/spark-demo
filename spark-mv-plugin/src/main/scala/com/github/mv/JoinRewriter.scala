package com.github.mv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Matches JOIN queries against materialized views and rewrites them
 * to read from the MV's backing table.  Inspired by StarRocks' MV
 * join rewrite capabilities, this supports:
 *
 *  1. '''Exact join match''' – the query's join tree (tables, join type,
 *     conditions) exactly matches the MV's defining query, so the entire
 *     plan is replaced by a scan of the backing table.
 *
 *  2. '''Join + aggregate match''' – a query that aggregates over a join
 *     can be served by an MV that pre-computes the same join and
 *     aggregation.
 *
 *  3. '''Join with predicate compensation''' – a query whose join matches
 *     the MV but has additional WHERE filters; the extra predicates are
 *     applied on top of the MV scan.
 *
 *  4. '''Join with column subset''' – the query selects fewer columns from
 *     the join result than the MV stores; a Project prunes them.
 */
object JoinRewriter {

  /**
   * Attempt to rewrite `queryPlan` using a join-based MV.
   * Returns `Some(rewrittenPlan)` on success, `None` if no MV applies.
   */
  def tryRewrite(
      session: SparkSession,
      queryPlan: LogicalPlan): Option[LogicalPlan] = {

    val mvs = MaterializedViewCatalog.listAll()
    if (mvs.isEmpty) return None

    mvs.foreach { meta =>
      try {
        val mvParsed = session.sessionState.sqlParser.parsePlan(meta.query)
        val mvAnalyzed = session.sessionState.analyzer.execute(mvParsed)

        val result = tryRewriteWithMV(session, queryPlan, mvAnalyzed, meta)
        if (result.isDefined) return result
      } catch {
        case _: Exception => // skip
      }
    }
    None
  }

  // ─────────────────────────────────────────────────────────────────
  //  Core matching
  // ─────────────────────────────────────────────────────────────────

  private def tryRewriteWithMV(
      session: SparkSession,
      queryPlan: LogicalPlan,
      mvPlan: LogicalPlan,
      meta: MaterializedViewMeta): Option[LogicalPlan] = {

    val queryJoinInfo = decomposeJoin(queryPlan)
    val mvJoinInfo = decomposeJoin(mvPlan)

    if (queryJoinInfo.isEmpty || mvJoinInfo.isEmpty) return None

    val qi = queryJoinInfo.get
    val mi = mvJoinInfo.get

    // Join trees must match structurally
    if (!sameJoinStructure(qi.joinTree, mi.joinTree)) return None

    // Check predicate compensation for non-join predicates
    val extraPreds = AggregateRewriter.predicateCompensation(qi.predicates, mi.predicates)
    if (extraPreds.isEmpty) return None

    // Build the backing-table scan
    val backingScan = session.sessionState.sqlParser
      .parsePlan(s"SELECT * FROM ${meta.backingTable}")
    val resolvedBacking = session.sessionState.analyzer.execute(backingScan)
    val backingAttrMap: Map[String, Attribute] =
      resolvedBacking.output.map(a => a.name.toLowerCase -> a).toMap

    // Check if this is a join + aggregate query
    if (qi.hasAggregate && mi.hasAggregate) {
      tryJoinAggregateRewrite(session, qi, mi, resolvedBacking, backingAttrMap, extraPreds.get)
    } else if (!qi.hasAggregate && !mi.hasAggregate) {
      // Simple join rewrite (no aggregation)
      trySimpleJoinRewrite(session, qi, mi, resolvedBacking, backingAttrMap, extraPreds.get)
    } else {
      None
    }
  }

  // ─────────────────────────────────────────────────────────────────
  //  Simple join rewrite (no aggregation)
  // ─────────────────────────────────────────────────────────────────

  private def trySimpleJoinRewrite(
      session: SparkSession,
      qi: JoinPlanInfo,
      mi: JoinPlanInfo,
      backingPlan: LogicalPlan,
      backingAttrMap: Map[String, Attribute],
      extraPreds: Seq[Expression]): Option[LogicalPlan] = {

    // Query output columns must be a subset of MV output columns
    if (!qi.outputNames.subsetOf(mi.outputNames)) return None

    var result: LogicalPlan = backingPlan

    // Apply extra predicates (predicate compensation)
    if (extraPreds.nonEmpty) {
      val compensated = AggregateRewriter.rewritePredicateRefs(extraPreds, backingAttrMap)
      if (compensated.isEmpty) return None
      result = Filter(compensated.get.reduceLeft(And), result)
    }

    // Project to match query output
    val projectExprs = qi.outputNames.toSeq.flatMap { name =>
      backingAttrMap.get(name).map(a => Alias(a, name)(exprId = NamedExpression.newExprId))
    }
    if (projectExprs.size != qi.outputNames.size) return None

    Some(Project(projectExprs, result))
  }

  // ─────────────────────────────────────────────────────────────────
  //  Join + aggregate rewrite
  // ─────────────────────────────────────────────────────────────────

  private def tryJoinAggregateRewrite(
      session: SparkSession,
      qi: JoinPlanInfo,
      mi: JoinPlanInfo,
      backingPlan: LogicalPlan,
      backingAttrMap: Map[String, Attribute],
      extraPreds: Seq[Expression]): Option[LogicalPlan] = {

    // Both must have aggregate info
    if (qi.aggregateInfo.isEmpty || mi.aggregateInfo.isEmpty) return None

    val qAgg = qi.aggregateInfo.get
    val mAgg = mi.aggregateInfo.get

    // Check GROUP BY compatibility
    val isExactGroupBy = qAgg.groupByNames == mAgg.groupByNames
    val isRollUp = qAgg.groupByNames.nonEmpty &&
                   qAgg.groupByNames.subsetOf(mAgg.groupByNames)

    if (!isExactGroupBy && !isRollUp) return None

    // Output columns must be available
    if (!qi.outputNames.subsetOf(mi.outputNames)) return None

    var result: LogicalPlan = backingPlan

    // Apply extra predicates
    if (extraPreds.nonEmpty) {
      val compensated = AggregateRewriter.rewritePredicateRefs(extraPreds, backingAttrMap)
      if (compensated.isEmpty) return None
      result = Filter(compensated.get.reduceLeft(And), result)
    }

    if (isExactGroupBy) {
      // Exact aggregate match - project from backing table
      val projectExprs = qi.outputNames.toSeq.flatMap { name =>
        backingAttrMap.get(name).map(a => Alias(a, name)(exprId = NamedExpression.newExprId))
      }
      if (projectExprs.size != qi.outputNames.size) return None
      Some(Project(projectExprs, result))
    } else {
      // Roll-up: re-aggregate from MV
      val mvAggMap = mAgg.aggregateDetails.map { case (alias, func, srcCol) =>
        alias -> (func, srcCol)
      }.toMap

      val newGroupExprs: Seq[Expression] = qAgg.groupByNames.toSeq.flatMap { name =>
        backingAttrMap.get(name)
      }
      if (newGroupExprs.size != qAgg.groupByNames.size) return None

      val newAggExprs = scala.collection.mutable.ArrayBuffer[NamedExpression]()

      // Add group-by columns
      qAgg.groupByNames.toSeq.foreach { name =>
        backingAttrMap.get(name).foreach { attr =>
          newAggExprs += Alias(attr, name)(exprId = NamedExpression.newExprId)
        }
      }

      // Map each query aggregate
      for ((queryAggName, queryAggFunc, querySourceCol) <- qAgg.aggregateDetails) {
        val mvColOpt = mvAggMap.collectFirst {
          case (mvColName, (mvFunc, mvSourceCol))
            if mvSourceCol == querySourceCol && canRollUp(mvFunc, queryAggFunc) =>
            mvColName
        }

        mvColOpt match {
          case Some(mvColName) =>
            val backingAttr = backingAttrMap.get(mvColName)
            if (backingAttr.isEmpty) return None
            val rollUpExpr = buildRollUpExpression(queryAggFunc, backingAttr.get)
            if (rollUpExpr.isEmpty) return None
            newAggExprs += Alias(rollUpExpr.get, queryAggName)(exprId = NamedExpression.newExprId)
          case None => return None
        }
      }

      Some(Aggregate(newGroupExprs, newAggExprs.toSeq, result))
    }
  }

  // ─────────────────────────────────────────────────────────────────
  //  Join structure comparison
  // ─────────────────────────────────────────────────────────────────

  /**
   * Compare two join trees structurally: same join type, same base tables
   * (by canonicalized form), and equivalent join conditions.
   */
  private def sameJoinStructure(a: JoinTree, b: JoinTree): Boolean = {
    (a, b) match {
      case (LeafTable(planA), LeafTable(planB)) =>
        AggregateRewriter.sameBaseTable(planA, planB)
      case (JoinNode(leftA, rightA, typeA, condA), JoinNode(leftB, rightB, typeB, condB)) =>
        typeA == typeB &&
          sameJoinCondition(condA, condB) &&
          sameJoinStructure(leftA, leftB) &&
          sameJoinStructure(rightA, rightB)
      case _ => false
    }
  }

  /**
   * Compare join conditions by their canonicalized form.
   */
  private def sameJoinCondition(a: Option[Expression], b: Option[Expression]): Boolean = {
    (a, b) match {
      case (None, None) => true
      case (Some(exprA), Some(exprB)) =>
        try {
          exprA.canonicalized == exprB.canonicalized
        } catch {
          case _: Exception => false
        }
      case _ => false
    }
  }

  // ─────────────────────────────────────────────────────────────────
  //  Plan decomposition for JOIN queries
  // ─────────────────────────────────────────────────────────────────

  /**
   * Decompose a logical plan into join structure, predicates, optional
   * aggregate info, and output columns.
   */
  def decomposeJoin(plan: LogicalPlan): Option[JoinPlanInfo] = {
    plan match {
      // Aggregate over join (with optional filter)
      case agg @ Aggregate(groupExprs, aggExprs, child) =>
        val (preds, base) = extractFilterAndJoinBase(child)
        extractJoinTree(base) match {
          case Some(joinTree) =>
            val groupByNames = groupExprs.flatMap(AggregateRewriter.exprColumnNames).toSet
            val aggDetails = extractAggregateDetails(aggExprs)
            val outputNames = aggExprs.flatMap {
              case a: Alias => Some(a.name.toLowerCase)
              case a: Attribute => Some(a.name.toLowerCase)
              case _ => None
            }.toSet
            Some(JoinPlanInfo(
              joinTree, preds, outputNames,
              hasAggregate = true,
              aggregateInfo = Some(AggregateInfo(groupByNames, aggDetails))))
          case None => None
        }

      // Project over Aggregate over join
      case Project(projectList, agg @ Aggregate(_, _, _)) =>
        decomposeJoin(agg).map { info =>
          val projNames = projectList.flatMap {
            case a: Alias => Some(a.name.toLowerCase)
            case a: Attribute => Some(a.name.toLowerCase)
            case _ => None
          }.toSet
          info.copy(outputNames = projNames)
        }

      // Project over join (no aggregation)
      case Project(projectList, child) =>
        val (preds, base) = extractFilterAndJoinBase(child)
        extractJoinTree(base) match {
          case Some(joinTree) =>
            val outputNames = projectList.flatMap {
              case a: Alias => Some(a.name.toLowerCase)
              case a: Attribute => Some(a.name.toLowerCase)
              case _ => None
            }.toSet
            Some(JoinPlanInfo(joinTree, preds, outputNames,
              hasAggregate = false, aggregateInfo = None))
          case None => None
        }

      // Raw join without project
      case join @ Join(_, _, _, _, _) =>
        extractJoinTree(join) match {
          case Some(joinTree) =>
            val outputNames = join.output.map(_.name.toLowerCase).toSet
            Some(JoinPlanInfo(joinTree, Seq.empty, outputNames,
              hasAggregate = false, aggregateInfo = None))
          case None => None
        }

      case _ => None
    }
  }

  /**
   * Extract predicates and the join base from a plan.
   */
  private def extractFilterAndJoinBase(plan: LogicalPlan): (Seq[Expression], LogicalPlan) = {
    plan match {
      case Filter(cond, child) =>
        val (childPreds, base) = extractFilterAndJoinBase(child)
        (AggregateRewriter.splitConjunction(cond) ++ childPreds, base)
      case Project(_, child) =>
        extractFilterAndJoinBase(child)
      case other =>
        (Seq.empty, other)
    }
  }

  /**
   * Extract a JoinTree from a logical plan. Returns None if the plan
   * does not contain a join.
   */
  private def extractJoinTree(plan: LogicalPlan): Option[JoinTree] = {
    plan match {
      case Join(left, right, joinType, condition, _) =>
        val leftTree = extractJoinTree(left).getOrElse(LeafTable(left))
        val rightTree = extractJoinTree(right).getOrElse(LeafTable(right))
        Some(JoinNode(leftTree, rightTree, joinType, condition))
      case SubqueryAlias(_, child) =>
        extractJoinTree(child)
      case Project(_, child) =>
        extractJoinTree(child)
      case Filter(_, child) =>
        extractJoinTree(child)
      case _ =>
        None // Leaf relation, not a join
    }
  }

  /**
   * Check if a plan contains a join.
   */
  def containsJoin(plan: LogicalPlan): Boolean = {
    plan match {
      case Join(_, _, _, _, _) => true
      case _ => plan.children.exists(containsJoin)
    }
  }

  private def extractAggregateDetails(
      exprs: Seq[NamedExpression]): Seq[(String, String, String)] = {
    exprs.flatMap {
      case Alias(AggregateExpression(fn, _, _, _, _), aliasName) =>
        fn match {
          case Sum(child, _)     => AggregateRewriter.exprColumnNames(child).headOption.map(c => (aliasName.toLowerCase, "sum", c))
          case Count(children)   => children.flatMap(AggregateRewriter.exprColumnNames).headOption.map(c => (aliasName.toLowerCase, "count", c))
          case Min(child)        => AggregateRewriter.exprColumnNames(child).headOption.map(c => (aliasName.toLowerCase, "min", c))
          case Max(child)        => AggregateRewriter.exprColumnNames(child).headOption.map(c => (aliasName.toLowerCase, "max", c))
          case Average(child, _) => AggregateRewriter.exprColumnNames(child).headOption.map(c => (aliasName.toLowerCase, "avg", c))
          case _                 => None
        }
      case _ => None
    }
  }

  private def canRollUp(mvFunc: String, queryFunc: String): Boolean = {
    (mvFunc.toLowerCase, queryFunc.toLowerCase) match {
      case ("sum", "sum")     => true
      case ("count", "count") => true
      case ("min", "min")     => true
      case ("max", "max")     => true
      case _                  => false
    }
  }

  private def buildRollUpExpression(
      queryFunc: String,
      mvColAttr: Attribute): Option[Expression] = {
    queryFunc.toLowerCase match {
      case "sum"   => Some(Sum(mvColAttr).toAggregateExpression())
      case "count" => Some(Sum(mvColAttr).toAggregateExpression())
      case "min"   => Some(Min(mvColAttr).toAggregateExpression())
      case "max"   => Some(Max(mvColAttr).toAggregateExpression())
      case _       => None
    }
  }

  // ─────────────────────────────────────────────────────────────────
  //  Data structures
  // ─────────────────────────────────────────────────────────────────

  /** Abstract representation of a join tree. */
  sealed trait JoinTree

  /** A leaf node representing a single table/relation. */
  case class LeafTable(plan: LogicalPlan) extends JoinTree

  /** A join node with left, right children, join type, and condition. */
  case class JoinNode(
      left: JoinTree,
      right: JoinTree,
      joinType: JoinType,
      condition: Option[Expression]) extends JoinTree

  /** Aggregate information extracted from a plan. */
  case class AggregateInfo(
      groupByNames: Set[String],
      aggregateDetails: Seq[(String, String, String)]) // (alias, funcName, sourceCol)

  /** Join plan information. */
  case class JoinPlanInfo(
      joinTree: JoinTree,
      predicates: Seq[Expression],
      outputNames: Set[String],
      hasAggregate: Boolean,
      aggregateInfo: Option[AggregateInfo])
}
