import java.nio._

def fromLittleToBig(i: Int): Int = {
    val bb = ByteBuffer.wrap(Array.ofDim[Byte](4))
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(i)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.rewind()
    bb.getInt()
}

def fromBigToLittle(i: Int): Int = {
    val bb = ByteBuffer.wrap(Array.ofDim[Byte](4))
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putInt(i)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.rewind()
    bb.getInt()
}

def fromLittleToBig(i: Long): Long = {
    val bb = ByteBuffer.wrap(Array.ofDim[Byte](8))
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.putLong(i)
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.rewind()
    bb.getLong()
}

def fromBigToLittle(i: Long): Long = {
    val bb = ByteBuffer.wrap(Array.ofDim[Byte](8))
    bb.order(ByteOrder.BIG_ENDIAN)
    bb.putLong(i)
    bb.order(ByteOrder.LITTLE_ENDIAN)
    bb.rewind()
    bb.getLong()
}

fromLittleToBig(330)
fromBigToLittle(330)
fromBigToLittle(330L)
