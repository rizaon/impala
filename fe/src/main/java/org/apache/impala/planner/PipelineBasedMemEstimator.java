package org.apache.impala.planner;

import com.google.common.base.Preconditions;

import org.apache.impala.thrift.TExecNodePhase;
import org.apache.impala.thrift.TQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineBasedMemEstimator {
  private final static Logger LOG = LoggerFactory.getLogger(Planner.class);

  private final List<PlanFragment> allFragments;
  private final TQueryOptions queryOptions;

  private final Set<PlanNodeId> unionPipelines = new HashSet<>();
  private final Map<PlanNodeId, Long> pipelineMemGrowth = new HashMap<>();
  private final Map<PlanNodeId, Long> totalMaxAdjacentOf = new HashMap<>();
  private long totalMinMemEstimate = 0;
  private long maxMemGrowthOfTree = 0;
  private long fixedResourcePerBackend = 0;

  public PipelineBasedMemEstimator(
      PlanFragment rootFragment, TQueryOptions queryOptions) {
    this.queryOptions = queryOptions;
    this.allFragments = rootFragment.getNodesPreOrder();
    Collections.reverse(this.allFragments);
    calculatePipelineBasedMemEstimate();
  }

  public long getEstimatedMemUsageBytes() {
    return totalMinMemEstimate + maxMemGrowthOfTree + fixedResourcePerBackend;
  }

  /**
   * Find maximum memory growth rooted at root plan's pipeline when considering
   * overlapping pipeline membership.
   *
   * Memory growth is the difference between minimum memory reservation and estimated peak
   * memory usage in bytes.
   *
   * We say pipeline X(GETNEXT) is adjacent to Y(OPEN) if a PlanNode has
   * PipelineMembership in both X(GETNEXT) and Y(OPEN).
   *
   * A memory growth estimate of a pipeline X is sum of memory estimate of all fragments
   * that have PipelineMembership X(GETNEXT) plus memory growth estimate of all fragment
   * that have PipelineMembership Y(OPEN) where X(GETNEXT) is adjacent to Y(OPEN) and both
   * X and Y originate from different fragment.
   *
   * Given pipeline X(GETNEXT) and all of its adjacent pipeline Y(OPEN), a maximum
   * memory growth estimate rooted at pipeline X is the maximum between:
   * 1). Memory growth estimate of a pipeline X.
   * 2). Sum of all maximum memory growth estimate rooted at pipelines adjacent to X.
   *
   * This method assume that all Y(OPEN) pipelines must complete first before X(GETNEXT)
   * pipeline can start streaming rows. This algorithm also include memory growth from
   * Fragment's per-backend resource profile (ie., memory allocated for runtime profile).
   */
  private void calculatePipelineBasedMemEstimate() {
    // Traverse build-side then probe-side.

    for (PlanFragment fragment : allFragments) {
      // Separate between backend resource profile (shared resource such as runtime filter
      // buffers) vs instance resource profile (resource needed by PlanNodes).
      fixedResourcePerBackend +=
          fragment.getPerBackendResourceProfile().getMemEstimateBytes();

      // Calculate min and peak memory estimate from
      // fragment.getPerInstanceResourceProfile() times number of instances in node.
      long numInstancePerHost = fragment.getNumInstancesPerHost(queryOptions);
      long peakMemEstimate = numInstancePerHost
          * fragment.getPerInstanceResourceProfile().getMemEstimateBytes();
      long minMemEstimate =
          numInstancePerHost * fragment.getPerIntstanceMinMemOverestimation();
      Preconditions.checkState(peakMemEstimate >= minMemEstimate);

      // Memory growth at this fragment is the difference between peak estimate vs
      // overestimated minimum memory reservation. Add minMemEstimate to
      // totalMinMemEstimate so that it will always be counted towards final estimate.
      totalMinMemEstimate += minMemEstimate;
      long currFragmentMemGrowth = peakMemEstimate - minMemEstimate;
      LOG.info(
          "tracing max resource at fragment {} with fragmentMemGrowth={} and minMemEstimate={}",
          fragment.getId(), currFragmentMemGrowth, minMemEstimate);

      // Find and note UNION node in the fragment if any.
      UnionNode union = fragment.getUnionNode();
      if (union != null) {
        union.getPipelines().forEach(p -> unionPipelines.add(p.getId()));
        LOG.info("unionPipelines={}", unionPipelines);
      }

      // Traverse PlanNode within a fragment bottom-up.
      // Within a PlanNode, we segregate between GETNEXT pipeline (also called as
      // leftPipe) and OPEN pipeline (also called as rightPipe). OPEN pipeline is
      // usually OPEN pipeline from build-hand side of Join node or OPEN
      // pipeline from child node to blocking parent node (ie., final Aggregation
      // node).
      List<PlanNode> planNodes = fragment.collectPlanNodes();
      Collections.reverse(planNodes);
      List<List<PipelineMembership>> perNodeNextPipelines = new ArrayList<>();
      List<List<PipelineMembership>> perNodeOpenPipelines = new ArrayList<>();
      Set<PlanNodeId> unionMemberPipes = new HashSet<>();
      Set<PlanNodeId> localPipes = new HashSet<>();
      for (PlanNode p : planNodes) {
        List<PipelineMembership> getNextPipelines = new ArrayList<>();
        List<PipelineMembership> openPipelines = new ArrayList<>();
        for (PipelineMembership pm : p.getPipelines()) {
          if (pm.getPhase() == TExecNodePhase.GETNEXT) {
            getNextPipelines.add(pm);
            unionMemberPipes.add(pm.getId());
            if (pm.getId().equals(p.getId())) { localPipes.add(pm.getId()); }
          } else {
            // pm is an OPEN pipeline.
            openPipelines.add(pm);
          }
        }
        perNodeNextPipelines.add(getNextPipelines);
        perNodeOpenPipelines.add(openPipelines);
      }

      // Count how many NEXT pipelines are collocated within a UNION node.
      unionMemberPipes.retainAll(unionPipelines);
      int unionMemberCount = unionMemberPipes.size();

      Map<PlanNodeId, Long> addedMemGrowth = new HashMap<>();
      Set<PlanNodeId> seenInThisFragment = new HashSet<>();
      for (int nodeIdx = 0; nodeIdx < perNodeNextPipelines.size(); nodeIdx++) {
        List<PipelineMembership> getNextPipelines = perNodeNextPipelines.get(nodeIdx);
        List<PipelineMembership> openPipelines = perNodeOpenPipelines.get(nodeIdx);

        // Initialize pipelineMemGrowth traversing the GETNEXT pipelines bottom-up
        // within this fragment.
        for (PipelineMembership getNextPipe : getNextPipelines) {
          PlanNodeId leftPipeId = getNextPipe.getId();
          if (seenInThisFragment.contains(leftPipeId)) continue;
          // Sum this fragment's memory growth to this pipeline once.
          long memGrowth = currFragmentMemGrowth;
          if (unionPipelines.contains(leftPipeId)) {
            LOG.info("leftPipeId {} is pass through UNION fragment, "
                    + "reduced memGrowth={}/{}",
                leftPipeId, currFragmentMemGrowth, unionMemberCount);
            memGrowth = currFragmentMemGrowth / unionMemberCount;
          }
          long totalMemGrowthAtPipe = pipelineMemGrowth.getOrDefault(leftPipeId, 0L);
          pipelineMemGrowth.put(leftPipeId, memGrowth + totalMemGrowthAtPipe);
          addedMemGrowth.put(leftPipeId, memGrowth);
          seenInThisFragment.add(leftPipeId);
        }

        // Sum all OPEN pipeline resources to its adjacent GETNEXT pipeline, EXCEPT
        // for OPEN pipeline that originate in this fragment. Also keep track
        // allMemGrowthAtOpens as the total of maximum resources rooted at all OPEN
        // pipeline.
        for (PipelineMembership getNextPipe : getNextPipelines) {
          PlanNodeId leftPipeId = getNextPipe.getId();
          Preconditions.checkState(pipelineMemGrowth.containsKey(leftPipeId));

          // Pipeline memory growth from leftPipeId + all openPipeId that are adjacent to
          // leftPipeId.
          long thisPipeMemGrowth = pipelineMemGrowth.get(leftPipeId);

          // Total maximum memory growth rooted at all adjacent OPEN pipelines.
          long allMemGrowthAtOpens = totalMaxAdjacentOf.getOrDefault(leftPipeId, 0L);

          for (PipelineMembership openPipe : openPipelines) {
            PlanNodeId openPipeId = openPipe.getId();
            Preconditions.checkState(pipelineMemGrowth.containsKey(openPipeId));
            if (!seenInThisFragment.contains(openPipeId)) {
              // openPipe is coming from different fragment, like JoinBuild.
              long openPipeMemGrowth = pipelineMemGrowth.get(openPipeId);
              long openMaxMemGrowth = getMaxMemGrowthAtPipeline(openPipeId);
              if (unionPipelines.contains(leftPipeId)) {
                // The GETNEXT pipeline is collocated within UNION node.
                // Divide it by unionMemberCount. There should be just 1 UNION node per
                // fragment.
                LOG.info(
                    "openPipe {} from different fragment added to UNION adjacentPipe {} "
                        + "with openPipeMemGrowth={}/{} openMaxMemGrowth={}/{}",
                    openPipeId, leftPipeId, openPipeMemGrowth, unionMemberCount,
                    openMaxMemGrowth, unionMemberCount);
                openPipeMemGrowth /= unionMemberCount;
                openMaxMemGrowth /= unionMemberCount;
              }
              thisPipeMemGrowth += openPipeMemGrowth;
              allMemGrowthAtOpens += openMaxMemGrowth;
            } else if (!localPipes.contains(openPipeId)) {
              // This OPEN pipeline is coming from leave PlanNodes within this fragment,
              // such as:
              // - GETNEXT pipeline from UNION node below this fragment.
              // - GETNEXT pipeline from multiple exchanges/scan within this fragment
              //   (ie, this fragment has UNION node).
              // Substract the addedMemGrowth from openPipeMemGrowth to avoid double
              // counting. Add the maximum between openPipeMemGrowth vs openMaxMemGrowth
              // towards allMemGrowthAtOpens.
              long toSubstract = addedMemGrowth.getOrDefault(openPipeId, 0L);
              long openPipeMemGrowth = pipelineMemGrowth.get(openPipeId) - toSubstract;
              long openMaxMemGrowth = totalMaxAdjacentOf.getOrDefault(openPipeId, 0L);
              allMemGrowthAtOpens += Math.max(openPipeMemGrowth, openMaxMemGrowth);
              LOG.info(
                  "Adding max(openPipeMemGrowth={}, openMaxMemGrowth={}) from collocated openPipe {} to "
                      + "leftPipe {}, toSubstract={}",
                  openPipeMemGrowth, openMaxMemGrowth, openPipeId, leftPipeId,
                  toSubstract);
            }
          }
          long maxMemGrowthAtPipe = getMaxMemGrowthAtPipeline(leftPipeId);
          pipelineMemGrowth.put(leftPipeId, thisPipeMemGrowth);
          totalMaxAdjacentOf.put(leftPipeId, allMemGrowthAtOpens);

          // Update Max resource rooted at this fragment and left pipelines.
          LOG.info(
              "At node {} pipeline[{}], compare current ({}) vs adjacent ({}) vs totalMaxAtOpen ({})",
              planNodes.get(nodeIdx).getId(), leftPipeId, maxMemGrowthAtPipe,
              thisPipeMemGrowth, allMemGrowthAtOpens);
          maxMemGrowthAtPipe = Math.max(thisPipeMemGrowth, allMemGrowthAtOpens);

          maxMemGrowthOfTree = Math.max(maxMemGrowthOfTree, maxMemGrowthAtPipe);
          LOG.info("maxMemGrowthAt pipeline[{}]={} maxMemGrowthOfTree={}", leftPipeId,
              maxMemGrowthAtPipe, maxMemGrowthOfTree);
        }
      }
    }

    LOG.info("pipelineMemGrowth={}", pipelineMemGrowth);
    LOG.info("totalMaxAdjacentOf={}", totalMaxAdjacentOf);
    LOG.info("totalMinMemEstimate={} maxMemGrowthOfTree={} fixedResourcePerBackend={}",
        totalMinMemEstimate, maxMemGrowthOfTree, fixedResourcePerBackend);
  }

  private long getMaxMemGrowthAtPipeline(PlanNodeId pipeId) {
    return Math.max(pipelineMemGrowth.getOrDefault(pipeId, 0L),
        totalMaxAdjacentOf.getOrDefault(pipeId, 0L));
  }
}
