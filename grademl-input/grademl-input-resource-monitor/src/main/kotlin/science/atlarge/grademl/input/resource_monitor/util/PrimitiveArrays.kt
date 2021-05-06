package science.atlarge.grademl.input.resource_monitor.util

fun concatenateArrays(arrays: List<LongArray>, separator: LongArray = longArrayOf()): LongArray {
    // Skip if no arrays are provided
    if (arrays.isEmpty()) {
        return longArrayOf()
    }
    // Allocate an array of the correct dimensions
    val totalSize = arrays.sumOf { it.size } + separator.size * (arrays.size - 1)
    val outArray = LongArray(totalSize)
    // Iterate over arrays and copy their contents to the output array, inserting separators as appropriate
    var offset = 0
    for (i in arrays.indices) {
        // Insert separator, except before the first array
        if (i != 0) {
            separator.copyInto(outArray, destinationOffset = offset)
            offset += separator.size
        }
        // Copy next array
        arrays[i].copyInto(outArray, destinationOffset = offset)
        offset += outArray.size
    }

    return outArray
}

fun concatenateArrays(arrays: List<DoubleArray>, separator: DoubleArray = doubleArrayOf()): DoubleArray {
    // Skip if no arrays are provided
    if (arrays.isEmpty()) {
        return doubleArrayOf()
    }
    // Allocate an array of the correct dimensions
    val totalSize = arrays.sumOf { it.size } + separator.size * (arrays.size - 1)
    val outArray = DoubleArray(totalSize)
    // Iterate over arrays and copy their contents to the output array, inserting separators as appropriate
    var offset = 0
    for (i in arrays.indices) {
        // Insert separator, except before the first array
        if (i != 0) {
            separator.copyInto(outArray, destinationOffset = offset)
            offset += separator.size
        }
        // Copy next array
        arrays[i].copyInto(outArray, destinationOffset = offset)
        offset += outArray.size
    }

    return outArray
}