package com.ctrip.hermes.monitor.job.partition.strategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaservice.queue.PartitionInfo;
import com.ctrip.hermes.metaservice.queue.TableContext;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.job.partition.context.MessageTableContext;
import com.ctrip.hermes.monitor.job.partition.finder.CreationStampFinder;
import com.ctrip.hermes.monitor.job.partition.finder.CreationStampFinder.CreationStamp;

public abstract class BasePartitionCheckerStrategy implements PartitionCheckerStrategy {
	private static final int SPEED_SAMPLE_COUNT = 3;

	private static final int MIN_PARTITION_COUNT = 5;

	abstract protected CreationStampFinder getCreationStampFinder();

	abstract protected MonitorConfig getConfig();

	public AnalysisResult analysisTable(TableContext ctx) {
		if (ctx == null) {
			throw new IllegalArgumentException("Wrong table context type for " + getClass().getName());
		}
		if (ctx.getPartitionInfos() == null) {
			throw new IllegalArgumentException("Partition infos illegal: " + ctx.getPartitionInfos());
		}

		if (ctx.getPartitionInfos().size() < MIN_PARTITION_COUNT) {
			return new AnalysisResult(calculateMinimalIncrementPartitions(ctx), new ArrayList<PartitionInfo>());
		}

		List<PartitionInfo> addList = new ArrayList<PartitionInfo>();
		List<PartitionInfo> dropList = new ArrayList<PartitionInfo>();

		excludeWrongPartition(ctx.getPartitionInfos(), dropList);

		CreationStamp oldest = getCreationStampFinder().findOldest(ctx);
		CreationStamp latest = getCreationStampFinder().findLatest(ctx);

		if (oldest != null && latest != null) {
			long leftCapacity = getLeftCapacity(ctx, latest);
			long dailyTotal = getLatestSpeedPerDay(ctx, oldest, latest);

			if (shouldAddPartition(leftCapacity, dailyTotal, ctx.getWatermarkInDay())) {
				addList.addAll(calculateIncrementPartitions(ctx, dailyTotal, getLatestCapacityPerPartition(ctx)));
			}
			dropList.addAll(calculateDecrementPartitions(ctx));
		}

		return new AnalysisResult(addList, dropList);
	}

	private int getPartitionIncrementStepByTableContext(TableContext ctx) {
		return ctx instanceof MessageTableContext ? getConfig().getPartitionSizeIncrementStep() : //
		      getConfig().getResendPartitionSizeIncrementStep();
	}

	private long getLatestCapacityPerPartition(TableContext ctx) {
		List<PartitionInfo> ps = ctx.getPartitionInfos();

		long partitionSize = getPartitionIncrementStepByTableContext(ctx);
		return ps.size() < 2 ? partitionSize : //
		      Math.max(partitionSize, ps.get(ps.size() - 1).getUpperbound() - ps.get(ps.size() - 2).getUpperbound());
	}

	private List<PartitionInfo> calculateDecrementPartitions(TableContext ctx) {
		List<PartitionInfo> list = new ArrayList<PartitionInfo>();
		List<PartitionInfo> ps = ctx.getPartitionInfos();
		for (int idx = 0; idx < ps.size() - 1; idx++) {
			if (ps.get(idx + 1).getRows() == 0) {
				break;
			}
			PartitionInfo p = ps.get(idx);
			CreationStamp stamp = getCreationStampFinder().findSpecific(ctx, p.getUpperbound() - 1);
			if (stamp.getDate().getTime() < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ctx.getRetainInDay())) {
				list.add(p);
			} else {
				break;
			}
		}
		return list;
	}

	private List<PartitionInfo> calculateIncrementPartitions(TableContext ctx, long speed, long partitionSize) {
		List<PartitionInfo> list = new ArrayList<PartitionInfo>();
		if (speed > 0) {
			long incrementPartitionCount = ctx.getIncrementInDay() * speed / partitionSize;
			if (incrementPartitionCount > getConfig().getPartitionIncrementMaxCount()) {
				Pair<Long, Long> pair = renewPartitionSizeAndCount(ctx, ctx.getIncrementInDay() * speed,
				      ctx.getIncrementInDay());
				partitionSize = pair.getKey();
				incrementPartitionCount = pair.getValue();
			}
			PartitionInfo latestPartitionInfo = ctx.getPartitionInfos().get(ctx.getPartitionInfos().size() - 1);
			for (int idx = 0; idx < incrementPartitionCount; idx++) {
				PartitionInfo nextPartitionInfo = new PartitionInfo();
				nextPartitionInfo.setUpperbound(latestPartitionInfo.getUpperbound() + partitionSize);
				nextPartitionInfo.setName(nextPartitionName(latestPartitionInfo));
				nextPartitionInfo.setTable(ctx.getTableName());
				list.add(nextPartitionInfo);

				latestPartitionInfo = nextPartitionInfo;
			}
		}
		return list;
	}

	private List<PartitionInfo> calculateMinimalIncrementPartitions(TableContext ctx) {
		List<PartitionInfo> list = new ArrayList<PartitionInfo>();
		PartitionInfo latestPartitionInfo = ctx.getPartitionInfos().get(ctx.getPartitionInfos().size() - 1);
		for (int idx = 0; idx < MIN_PARTITION_COUNT - ctx.getPartitionInfos().size(); idx++) {
			PartitionInfo nextPartitionInfo = new PartitionInfo();
			nextPartitionInfo.setUpperbound( //
			      latestPartitionInfo.getUpperbound() + getPartitionIncrementStepByTableContext(ctx));
			nextPartitionInfo.setName(nextPartitionName(latestPartitionInfo));
			nextPartitionInfo.setTable(ctx.getTableName());
			list.add(nextPartitionInfo);

			latestPartitionInfo = nextPartitionInfo;
		}
		return list;
	}

	private Pair<Long, Long> renewPartitionSizeAndCount(TableContext ctx, long capacityInCount, int capacityInDay) {
		long capacityPerDay = capacityInCount / capacityInDay;
		long step = getPartitionIncrementStepByTableContext(ctx);
		long size = step;
		while (size < getConfig().getPartitionMaxSize() && size < capacityPerDay) {
			size += step;
		}
		return new Pair<Long, Long>(size, (long) Math.ceil(capacityInCount / (double) size));
	}

	private String nextPartitionName(PartitionInfo current) {
		return String.format("p%s", Long.valueOf(current.getName().substring(1)) + 1);
	}

	private void excludeWrongPartition(List<PartitionInfo> partitions, List<PartitionInfo> delList) {
		PartitionInfo last = partitions.get(partitions.size() - 1);
		if (last.getUpperbound() == Long.MAX_VALUE) {
			delList.add(last);
			partitions.remove(partitions.size() - 1);
		}
	}

	private boolean shouldAddPartition(long capacity, long speed, int watermark) {
		if (speed <= 0) {
			return false;
		}
		return capacity / speed <= watermark;
	}

	private long getLeftCapacity(TableContext ctx, CreationStamp latest) {
		List<PartitionInfo> ps = ctx.getPartitionInfos();
		return ps.get(ps.size() - 1).getUpperbound() - latest.getId();
	}

	private long getLatestSpeedPerDay(TableContext ctx, CreationStamp oldest, CreationStamp latest) {
		List<PartitionInfo> ps = findSamplePartitions(ctx.getPartitionInfos());
		return ps.size() == 0 ? 0 : calculateAverageSpeed(ctx, ps, oldest, latest);
	}

	private long calculateAverageSpeed(//
	      TableContext ctx, List<PartitionInfo> ps, CreationStamp oldest, CreationStamp latest) {
		final long _1DayMillis = TimeUnit.DAYS.toMillis(1);
		oldest = ps.get(0).getOrdinal() == 1 ? oldest : //
		      getCreationStampFinder().findSpecific(ctx, ps.get(0).getUpperbound() - ps.get(0).getRows());
		if (oldest != null && latest != null) {
			long period = latest.getDate().getTime() - oldest.getDate().getTime();
			period = Math.max(1, period);
			if (period < _1DayMillis / 3) { // In case invalid speed
				return (latest.getId() - oldest.getId()) * 3;
			} else {
				return (long) ((latest.getId() - oldest.getId()) / (float) period * _1DayMillis);
			}
		}
		return -1L;
	}

	private List<PartitionInfo> findSamplePartitions(List<PartitionInfo> partitions) {
		if (SPEED_SAMPLE_COUNT > partitions.size()) {
			return removeEmptyPartition(new ArrayList<PartitionInfo>(partitions));
		}
		if (partitions.get(partitions.size() - 1).getRows() > 0) {
			return removeEmptyPartition(partitions.subList(partitions.size() - SPEED_SAMPLE_COUNT, partitions.size()));
		}
		for (int idx = SPEED_SAMPLE_COUNT; idx < partitions.size(); idx++) {
			if (partitions.get(idx).getRows() == 0) {
				return removeEmptyPartition(partitions.subList(idx - SPEED_SAMPLE_COUNT, idx));
			}
		}
		throw new RuntimeException("Impossiable status when find sample partitions!");
	}

	private List<PartitionInfo> removeEmptyPartition(List<PartitionInfo> ps) {
		Iterator<PartitionInfo> iter = ps.iterator();
		while (iter.hasNext()) {
			if (iter.next().getRows() == 0) {
				iter.remove();
			}
		}
		return ps;
	}
}