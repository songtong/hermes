package com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.TableContext;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.finder.CreationStampFinder;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.finder.CreationStampFinder.CreationStamp;
import com.ctrip.hermes.monitor.config.MonitorConfig;

public abstract class BasePartitionCheckerStrategy implements PartitionCheckerStrategy {
	private static final int SPEED_SAMPLE_COUNT = 3;

	private static final int MIN_PARTITION_COUNT = 5;

	abstract protected CreationStampFinder getCreationStampFinder();

	abstract protected MonitorConfig getConfig();

	public Pair<List<PartitionInfo>, List<PartitionInfo>> analysisTable(TableContext ctx) {
		if (ctx == null) {
			throw new IllegalArgumentException("Wrong table context type for " + getClass().getName());
		}
		if (ctx.getPartitionInfos() == null || ctx.getPartitionInfos().size() < MIN_PARTITION_COUNT) {
			throw new IllegalArgumentException("Partition infos illegal: " + ctx.getPartitionInfos());
		}

		List<PartitionInfo> addList = new ArrayList<PartitionInfo>();
		List<PartitionInfo> delList = new ArrayList<PartitionInfo>();

		excludeWrongPartition(ctx.getPartitionInfos(), delList);

		CreationStamp oldest = getCreationStampFinder().findOldest(ctx);
		CreationStamp latest = getCreationStampFinder().findLatest(ctx);

		if (oldest != null && latest != null) {
			long leftCapacity = getLeftCapacity(ctx, latest);
			long daySpeed = getLatestSpeedPerDay(ctx, oldest, latest);

			if (isUsageDanger(leftCapacity, daySpeed, ctx.getCordonInDay())) {
				long partitionCapacity = getLatestCapacityPerPartition(ctx);
				addList.addAll(caculateIncrementPartitions(ctx, daySpeed, partitionCapacity));
			}
			delList.addAll(caculateDecrementPartitions(ctx));
		}

		return new Pair<List<PartitionInfo>, List<PartitionInfo>>(addList, delList);
	}

	private long getLatestCapacityPerPartition(TableContext ctx) {
		List<PartitionInfo> ps = ctx.getPartitionInfos();
		return ps.get(ps.size() - 1).getBorder() - ps.get(ps.size() - 2).getBorder();
	}

	private List<PartitionInfo> caculateDecrementPartitions(TableContext ctx) {
		List<PartitionInfo> list = new ArrayList<PartitionInfo>();
		List<PartitionInfo> ps = ctx.getPartitionInfos();
		for (int idx = 0; idx < ps.size() - 1; idx++) {
			if (ps.get(idx + 1).getRows() == 0) {
				break;
			}
			PartitionInfo p = ps.get(idx);
			CreationStamp stamp = getCreationStampFinder().findSpecific(ctx, p.getBorder() - 1);
			if (stamp.getDate().getTime() < System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ctx.getRetainInDay())) {
				list.add(p);
			} else {
				break;
			}
		}
		return list;
	}

	private List<PartitionInfo> caculateIncrementPartitions(TableContext ctx, long speed, long partitionSize) {
		List<PartitionInfo> list = new ArrayList<PartitionInfo>();
		if (speed > 0) {
			long incrementPartitionCount = ctx.getIncrementInDay() * speed / partitionSize;
			if (incrementPartitionCount > getConfig().getPartitionIncrementMaxCount()) {
				Pair<Long, Long> pair = renewPartitionSizeAndCount(ctx.getIncrementInDay() * speed, ctx.getIncrementInDay());
				partitionSize = pair.getKey();
				incrementPartitionCount = pair.getValue();
			}
			PartitionInfo latestPartitionInfo = ctx.getPartitionInfos().get(ctx.getPartitionInfos().size() - 1);
			for (int idx = 0; idx < incrementPartitionCount; idx++) {
				PartitionInfo nextPartitionInfo = new PartitionInfo();
				nextPartitionInfo.setBorder(latestPartitionInfo.getBorder() + partitionSize);
				nextPartitionInfo.setName(nextPartitionName(latestPartitionInfo));
				nextPartitionInfo.setTable(ctx.getTableName());
				list.add(nextPartitionInfo);

				latestPartitionInfo = nextPartitionInfo;
			}
		}
		return list;
	}

	private Pair<Long, Long> renewPartitionSizeAndCount(long capacityInCount, int capacityInDay) {
		long capacityPerDay = capacityInCount / capacityInDay;
		long step = getConfig().getPartitionSizeIncrementStep();
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
		if (last.getBorder() == Long.MAX_VALUE) {
			delList.add(last);
			partitions.remove(partitions.size() - 1);
		}
	}

	private boolean isUsageDanger(long capacity, long speed, int cordon) {
		if (speed <= 0) {
			return false;
		}
		return capacity / speed <= cordon;
	}

	private long getLeftCapacity(TableContext ctx, CreationStamp latest) {
		List<PartitionInfo> ps = ctx.getPartitionInfos();
		return ps.get(ps.size() - 1).getBorder() - latest.getId();
	}

	private long getLatestSpeedPerDay(TableContext ctx, CreationStamp oldest, CreationStamp latest) {
		List<PartitionInfo> ps = findSamplePartitions(ctx.getPartitionInfos());
		return ps.size() == 0 ? 0 : calculateAverageSpeed(ctx, ps, oldest, latest);
	}

	private long calculateAverageSpeed(//
	      TableContext ctx, List<PartitionInfo> ps, CreationStamp oldest, CreationStamp latest) {
		oldest = ps.get(0).getOrdinal() == 1 ? oldest : //
		      getCreationStampFinder().findSpecific(ctx, ps.get(0).getBorder() - ps.get(0).getRows());
		if (oldest != null && latest != null) {
			long period = latest.getDate().getTime() - oldest.getDate().getTime();
			period = Math.max(1, period);
			return (long) ((latest.getId() - oldest.getId()) / (float) period * TimeUnit.DAYS.toMillis(1));
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
