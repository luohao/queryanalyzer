package com.twitter.query.offline;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.TypeSignature;

import static com.google.common.base.Preconditions.checkArgument;

public final class UnknownType
        extends AbstractFixedWidthType
{
    public static final UnknownType UNKNOWN = new UnknownType();
    public static final String NAME = "unknown";

    private UnknownType()
    {
        super(new TypeSignature(NAME), void.class, 0);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public long hash(Block block, int position)
    {
        // Check that the position is valid
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return 0;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // Check that the position is valid
        checkArgument(leftBlock.isNull(leftPosition), "Expected NULL value for UnknownType");
        checkArgument(rightBlock.isNull(rightPosition), "Expected NULL value for UnknownType");
        return true;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // Check that the position is valid
        checkArgument(leftBlock.isNull(leftPosition), "Expected NULL value for UnknownType");
        checkArgument(rightBlock.isNull(rightPosition), "Expected NULL value for UnknownType");
        return 0;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        // call is null in case position is out of bounds
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return null;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        blockBuilder.appendNull();
    }
}

