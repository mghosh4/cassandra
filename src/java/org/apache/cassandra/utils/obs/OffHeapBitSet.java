/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils.obs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.Memory;

/**
 * Off-heap bitset,
 * file compatible with OpeBitSet
 */
public class OffHeapBitSet implements IBitSet
{
    private final Memory bytes;

    public OffHeapBitSet(long numBits)
    {
        // OpenBitSet.bits2words calculation is there for backward compatibility.
        long wordCount = OpenBitSet.bits2words(numBits);
        if (wordCount > Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Bloom filter size is > 16GB, reduce the bloom_filter_fp_chance");
        try
        {
            long byteCount = wordCount * 8L;
            bytes = Memory.allocate(byteCount);
        }
        catch (OutOfMemoryError e)
        {
            throw new RuntimeException("Out of native memory occured, You can avoid it by increasing the system ram space or by increasing bloom_filter_fp_chance.");
        }
        // flush/clear the existing memory.
        clear();
    }

    private OffHeapBitSet(Memory bytes)
    {
        this.bytes = bytes;
    }

    public long capacity()
    {
        return bytes.size() * 8;
    }

    public boolean get(long index)
    {
        long i = index >> 3;
        long bit = index & 0x7;
        int bitmask = 0x1 << bit;
        return (bytes.getByte(i) & bitmask) != 0;
    }

    public void set(long index)
    {
        long i = index >> 3;
        long bit = index & 0x7;
        int bitmask = 0x1 << bit;
        bytes.setByte(i, (byte) (bitmask | bytes.getByte(i)));
    }

    public void set(long offset, byte b)
    {
        bytes.setByte(offset, b);
    }
      
    /** @return the number of set bits */
    public long cardinality()
    {
    	ByteBuffer bb[] = bytes.asByteBuffers();
    	byte[] bArr = new byte[bb[0].capacity()];
    	int count = 0;
    	for (ByteBuffer b : bb)
    	{
    		b.get(bArr);
    		count += ByteArrayHelpers.countBits(bArr);
    	}
    	return count;
    }
    
    public static class ByteArrayHelpers
    {
        private static Map<Integer,Integer> LookupTable = new HashMap<Integer, Integer>(); 

        static {
        	for (int value = 0; value <= 256; value++)
        	{
        		int count = 0;
        		for (int i=0; i < 8; i++)
        		{
        			count += (value >> i) & 1;
        		}
        		LookupTable.put(value, count);
        	}
        }

        public static int countBits(byte[] array)
        {
            int count = 0;
            for (byte b : array)
            {
                count += LookupTable.get(b);
            }
            return count;
        }
    }

    public void clear(long index)
    {
        long i = index >> 3;
        long bit = index & 0x7;
        int bitmask = 0x1 << bit;
        int nativeByte = (bytes.getByte(i) & 0xFF);
        nativeByte &= ~bitmask;
        bytes.setByte(i, (byte) nativeByte);
    }

    public void clear()
    {
        bytes.setMemory(0, bytes.size(), (byte) 0);
    }

    public void serialize(DataOutput out) throws IOException
    {
        out.writeInt((int) (bytes.size() / 8));
        for (long i = 0; i < bytes.size();)
        {
            long value = ((bytes.getByte(i++) & 0xff) << 0) 
                       + ((bytes.getByte(i++) & 0xff) << 8)
                       + ((bytes.getByte(i++) & 0xff) << 16)
                       + ((long) (bytes.getByte(i++) & 0xff) << 24)
                       + ((long) (bytes.getByte(i++) & 0xff) << 32)
                       + ((long) (bytes.getByte(i++) & 0xff) << 40)
                       + ((long) (bytes.getByte(i++) & 0xff) << 48)
                       + ((long) bytes.getByte(i++) << 56);
            out.writeLong(value);
        }
    }

    public long serializedSize(TypeSizes type)
    {
        return type.sizeof((int) bytes.size()) + bytes.size();
    }

    public static OffHeapBitSet deserialize(DataInput in) throws IOException
    {
        long byteCount = in.readInt() * 8L;
        Memory memory = Memory.allocate(byteCount);
        for (long i = 0; i < byteCount;)
        {
            long v = in.readLong();
            memory.setByte(i++, (byte) (v >>> 0));
            memory.setByte(i++, (byte) (v >>> 8));
            memory.setByte(i++, (byte) (v >>> 16));
            memory.setByte(i++, (byte) (v >>> 24));
            memory.setByte(i++, (byte) (v >>> 32));
            memory.setByte(i++, (byte) (v >>> 40));
            memory.setByte(i++, (byte) (v >>> 48));
            memory.setByte(i++, (byte) (v >>> 56));
        }
        return new OffHeapBitSet(memory);
    }

    public void close()
    {
        bytes.free();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof OffHeapBitSet))
            return false;
        OffHeapBitSet b = (OffHeapBitSet) o;
        return bytes.equals(b.bytes);
    }

    @Override
    public int hashCode()
    {
        // Similar to open bitset.
        long h = 0;
        for (long i = bytes.size(); --i >= 0;)
        {
            h ^= bytes.getByte(i);
            h = (h << 1) | (h >>> 63); // rotate left
        }
        return (int) ((h >> 32) ^ h) + 0x98761234;
    }
    
    public Memory getMemory()
    {
    	return bytes;
    }

	@Override
	public IBitSet union(IBitSet bitSet) {
		Memory outBuf = Memory.allocate(bytes.size());
		OffHeapBitSet secSet = (OffHeapBitSet) bitSet;
		
		for (int i = 0; i < bytes.size() / 8; i++)
		{
			long out = this.bytes.getLong(i * 8) | secSet.getMemory().getLong(i * 8);
			outBuf.setLong(i * 8, out);
		}
		return new OffHeapBitSet(outBuf);
	}

	@Override
	public void unionInPlace(IBitSet bitSet) {
		OffHeapBitSet secSet = (OffHeapBitSet) bitSet;
		
		for (int i = 0; i < bytes.size() / 8; i++)
		{
			long out = this.bytes.getLong(i * 8) | secSet.getMemory().getLong(i * 8);
			this.bytes.setLong(i * 8, out);
		}
	}
}
