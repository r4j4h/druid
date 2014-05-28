/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class BatchDataSegmentAnnouncer extends AbstractDataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchDataSegmentAnnouncer.class);
  private final BatchDataSegmentAnnouncerConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final String liveSegmentLocation;
  private final Object lock = new Object();
  private final AtomicLong counter = new AtomicLong(0);
  private final Set<SegmentZNode> availableZNodes = new ConcurrentSkipListSet<SegmentZNode>();
  private final Map<DataSegment, SegmentZNode> segmentLookup = Maps.newConcurrentMap();

  @Inject
  public BatchDataSegmentAnnouncer(
      DruidServerMetadata server,
      BatchDataSegmentAnnouncerConfig config,
      ZkPathsConfig zkPaths,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    super(server, zkPaths, announcer, jsonMapper);
    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;

    this.liveSegmentLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), server.getName());
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    int newBytesLen = jsonMapper.writeValueAsBytes(segment).length;
    if (newBytesLen > config.getMaxBytesPerNode()) {
      throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxBytesPerNode());
    }

    synchronized (lock) {
      // create new batch
      if (availableZNodes.isEmpty()) {
        SegmentZNode availableZNode = new SegmentZNode(makeServedSegmentPath(new DateTime().toString()));
        final byte[] bytes = availableZNode.addSegment(segment);

        log.info("Announcing segment[%s] at path[%s]", segment.getIdentifier(), availableZNode.getPath());
        announcer.announce(availableZNode.getPath(), bytes);
        segmentLookup.put(segment, availableZNode);
        availableZNodes.add(availableZNode);
      } else { // update existing batch
        Iterator<SegmentZNode> iter = availableZNodes.iterator();
        boolean done = false;
        while (iter.hasNext() && !done) {
          SegmentZNode availableZNode = iter.next();
          if (availableZNode.getBytesSize() + newBytesLen < config.getMaxBytesPerNode()) {
            final byte[] bytes = availableZNode.addSegment(segment);

            log.info("Announcing segment[%s] at path[%s]", segment.getIdentifier(), availableZNode.getPath());
            announcer.update(availableZNode.getPath(), bytes);
            segmentLookup.put(segment, availableZNode);

            if (availableZNode.getCount() >= config.getSegmentsPerNode()) {
              availableZNodes.remove(availableZNode);
            }

            done = true;
          }
        }
      }
    }
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    final SegmentZNode segmentZNode = segmentLookup.remove(segment);
    if (segmentZNode == null) {
      return;
    }

    synchronized (lock) {
      final byte[] bytes = segmentZNode.removeSegment(segment);

      log.info("Unannouncing segment[%s] at path[%s]", segment.getIdentifier(), segmentZNode.getPath());
      if (segmentZNode.getCount() == 0) {
        availableZNodes.remove(segmentZNode);
        announcer.unannounce(segmentZNode.getPath());
      } else {
        announcer.update(segmentZNode.getPath(), bytes);
        availableZNodes.add(segmentZNode);
      }
    }
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    SegmentZNode segmentZNode = new SegmentZNode(makeServedSegmentPath(new DateTime().toString()));
    Set<DataSegment> batch = Sets.newHashSet();
    int byteSize = 0;
    int count = 0;

    for (DataSegment segment : segments) {
      int newBytesLen = jsonMapper.writeValueAsBytes(segment).length;

      if (newBytesLen > config.getMaxBytesPerNode()) {
        throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxBytesPerNode());
      }

      if (count >= config.getSegmentsPerNode() || byteSize + newBytesLen > config.getMaxBytesPerNode()) {
        announcer.announce(segmentZNode.getPath(), segmentZNode.addSegments(batch));
        segmentZNode = new SegmentZNode(makeServedSegmentPath(new DateTime().toString()));
        batch = Sets.newHashSet();
        count = 0;
        byteSize = 0;
      }

      log.info("Announcing segment[%s] at path[%s]", segment.getIdentifier(), segmentZNode.getPath());
      segmentLookup.put(segment, segmentZNode);
      batch.add(segment);
      count++;
      byteSize += newBytesLen;
    }

    announcer.announce(segmentZNode.getPath(), segmentZNode.addSegments(batch));
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegment segment : segments) {
      unannounceSegment(segment);
    }
  }

  private String makeServedSegmentPath(String zNode)
  {
    return ZKPaths.makePath(liveSegmentLocation, String.format("%s%s", zNode, counter.getAndIncrement()));
  }

  private class SegmentZNode implements Comparable<SegmentZNode>
  {
    private final String path;
    private int count = 0;

    public SegmentZNode(String path)
    {
      this.path = path;
    }

    public String getPath()
    {
      return path;
    }

    public int getCount()
    {
      return count;
    }

    public long getBytesSize()
    {
      if (count == 0) {
        return 0;
      } else {
        return getBytes().length;
      }
    }

    private byte[] getBytes()
    {
      return announcer.getAnnouncedData(path);
    }

    public Set<DataSegment> getSegments()
    {
      if (count == 0) {
        return Sets.newHashSet();
      }
      try {
        return jsonMapper.readValue(
            getBytes(), new TypeReference<Set<DataSegment>>()
        {
        }
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public byte[] addSegment(DataSegment segment)
    {
      Set<DataSegment> zkSegments = getSegments();
      zkSegments.add(segment);

      try {
        byte[] bytes = jsonMapper.writeValueAsBytes(zkSegments);
        count++;
        return bytes;
      }
      catch (Exception e) {
        zkSegments.remove(segment);
        throw Throwables.propagate(e);
      }

    }

    public byte[] addSegments(Set<DataSegment> segments)
    {
      Set<DataSegment> zkSegments = getSegments();
      zkSegments.addAll(segments);

      try {
        byte[] bytes = jsonMapper.writeValueAsBytes(zkSegments);
        count += segments.size();
        return bytes;
      }
      catch (Exception e) {
        zkSegments.removeAll(segments);
        throw Throwables.propagate(e);
      }

    }

    public byte[] removeSegment(DataSegment segment)
    {
      Set<DataSegment> zkSegments = getSegments();
      zkSegments.remove(segment);
      try {
        byte[] bytes = jsonMapper.writeValueAsBytes(zkSegments);
        count--;
        return bytes;
      }
      catch (Exception e) {
        zkSegments.add(segment);
        throw Throwables.propagate(e);
      }

    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SegmentZNode that = (SegmentZNode) o;

      if (!path.equals(that.path)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return path.hashCode();
    }

    @Override
    public int compareTo(SegmentZNode segmentZNode)
    {
      return path.compareTo(segmentZNode.getPath());
    }
  }
}
