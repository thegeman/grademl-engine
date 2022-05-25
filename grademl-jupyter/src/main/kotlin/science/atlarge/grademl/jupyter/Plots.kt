package science.atlarge.grademl.jupyter

import jetbrains.letsPlot.facet.facetWrap
import jetbrains.letsPlot.geom.geomLine
import jetbrains.letsPlot.geom.geomRect
import jetbrains.letsPlot.geom.geomText
import jetbrains.letsPlot.intern.Plot
import jetbrains.letsPlot.letsPlot
import jetbrains.letsPlot.scale.scaleFillDiscrete
import jetbrains.letsPlot.scale.scaleXContinuous
import jetbrains.letsPlot.scale.scaleYContinuous
import jetbrains.letsPlot.themeClassic
import science.atlarge.grademl.core.models.Path

object Plots {

    private val runRegex = """\[run_id=mnist-sshfs-overlapping__\d*-\d*__(e\dr\d)]""".toRegex()
    private val sparkAppRegex = """\[app=app-[^\[\]]*, id=(\d*)]""".toRegex()
    private val sparkStageRegex = """\[attempt=(\d*), id=(\d*)]""".toRegex()

    private fun formatPhaseId(phaseId: String): String {
        val r1 = runRegex.replace(phaseId) { "[${it.groupValues[1]}]" }
        val r2 = sparkAppRegex.replace(r1) { "[${it.groupValues[1]}]" }
        val r3 = sparkStageRegex.replace(r2) { "[${it.groupValues[1]}:${it.groupValues[2]}]" }
        return r3
    }

    @Suppress("UNCHECKED_CAST")
    fun plotPhases(
        queryResult: QueryResult,
        phaseColumn: String,
        zoom: Pair<Double, Double>? = null
    ): Plot {
        val data = queryResult.toMap().let {
            if (zoom != null) zoom(it, zoom.first, zoom.second) else it
        }
        require(data.isNotEmpty())

        val startTime = (data["_start_time"]!! as List<Double>).minOrNull()!!
        val endTime = (data["_end_time"]!! as List<Double>).maxOrNull()!!
        val rowCount = (data["ROW"]!! as List<Int>).maxOrNull()!!
        val phaseName = (data[phaseColumn]!! as List<String>).map { formatPhaseId(it) }
        val depth = phaseName.map { Path.parse(it).pathComponents.size.toString() }

        return letsPlot(data) +
                geomRect(color = "black") {
                    xmin = "_start_time"
                    xmax = "_end_time"
                    ymin = data["ROW"]!!.map { -(it as Int) + 0.95 }
                    ymax = data["ROW"]!!.map { -(it as Int) + 0.05 }
                    fill = depth
                } +
                geomText(x = (endTime + startTime) / 2) {
                    y = data["ROW"]!!.map { -(it as Int) + 0.5 }
                    label = phaseName
                } +
                scaleFillDiscrete(guide = "none") +
                scaleXContinuous("Time [s]", limits = startTime to endTime, expand = listOf(0, 0)) +
                scaleYContinuous("Phases", limits = 0 to -rowCount, expand = listOf(0, 0), breaks = emptyList()) +
                themeClassic()
    }

    @Suppress("UNCHECKED_CAST")
    fun plotMetrics(
        queryResult: QueryResult,
        metricColumn: String,
        zoom: Pair<Double, Double>? = null
    ): Plot {
        val data = queryResult.toMap().let {
            if (zoom != null) zoom(it, zoom.first, zoom.second) else it
        }
        require(data.isNotEmpty())

        val steppedData = step(data, metricColumn)
        val startTime = (steppedData["_time"] as List<Double>).minOrNull()!!
        val endTime = (steppedData["_time"] as List<Double>).maxOrNull()!!

        return letsPlot(steppedData) +
                geomLine {
                    x = "_time"
                    y = "_val"
                } +
                scaleXContinuous("Time [s]", limits = startTime to endTime, expand = listOf(0, 0)) +
                themeClassic()
    }

    @Suppress("UNCHECKED_CAST")
    fun plotMetricsPerPhase(
        queryResult: QueryResult,
        metricColumn: String,
        phaseColumn: String,
        zoom: Pair<Double, Double>? = null
    ): Plot {
        val data = queryResult.toMap().let {
            if (zoom != null) zoom(it, zoom.first, zoom.second) else it
        }
        require(data.isNotEmpty())

        val steppedData = step(data, metricColumn, phaseColumn).toMutableMap()
        steppedData["_key"] = (steppedData["_key"]!! as List<String>).map { formatPhaseId(it) }
        val startTime = (steppedData["_time"] as List<Double>).minOrNull()!!
        val endTime = (steppedData["_time"] as List<Double>).maxOrNull()!!

        return letsPlot(steppedData) +
                geomLine {
                    x = "_time"
                    y = "_val"
                } +
                scaleXContinuous("Time [s]", limits = startTime to endTime, expand = listOf(0, 0)) +
                facetWrap("_key", ncol = 1) +
                themeClassic()
    }

    @Suppress("UNCHECKED_CAST")
    fun zoom(data: Map<String, List<*>>, startTime: Double, endTime: Double): Map<String, List<*>> {
        val startCol = (data["_start_time"]!! as List<Double>).toMutableList()
        val endCol = (data["_end_time"]!! as List<Double>).toMutableList()

        val isInRange = BooleanArray(startCol.size) { i ->
            endCol[i] > startTime && startCol[i] < endTime
        }

        for (i in startCol.indices) {
            if (isInRange[i] && startCol[i] < startTime) startCol[i] = startTime
            if (isInRange[i] && endCol[i] > endTime) endCol[i] = endTime
        }

        return data.map { (colName, colValues) ->
            colName to when (colName) {
                "ROW" -> (0 until isInRange.count { it }).toList()
                "_start_time" -> startCol.filterIndexed { index, _ -> isInRange[index] }
                "_end_time" -> endCol.filterIndexed { index, _ -> isInRange[index] }
                else -> colValues.filterIndexed { index, _ -> isInRange[index] }
            }
        }.toMap()
    }

    @Suppress("UNCHECKED_CAST")
    fun step(data: Map<String, List<*>>, valueColumn: String, keyColumn: String): Map<String, List<*>> {
        val startCol = data["_start_time"]!! as List<Double>
        val endCol = data["_end_time"]!! as List<Double>
        val valCol = data[valueColumn]!!.let { vs ->
            when (vs[0]) {
                is Double -> vs as List<Double>
                is Long -> (vs as List<Long>).map { it.toDouble() }
                is Int -> (vs as List<Int>).map { it.toDouble() }
                is Boolean -> (vs as List<Boolean>).map { if (it) 1.0 else 0.0 }
                else -> vs.map { it.toString().toDouble() }
            }
        }
        val keyCol = data[keyColumn]!!

        val rowGroups = startCol.indices
            .groupBy { keyCol[it] }
            .mapValues { (_, indices) -> indices.sortedBy { startCol[it] } }

        val outTimes = ArrayList<Double>()
        val outValues = ArrayList<Double>()
        val outKeys = ArrayList<Any?>()
        for (i in keyCol.indices) {
            if (i != 0 && keyCol[i] == keyCol[i - 1]) continue
            val key = keyCol[i]
            val indices = rowGroups[key]!!

            for (j in indices.indices) {
                val curr = indices[j]
                if (j != 0 && startCol[curr] != endCol[indices[j - 1]]) {
                    outTimes.add((startCol[curr] + endCol[indices[j - 1]]) / 2)
                    outValues.add(Double.NaN)
                    outKeys.add(key)
                }

                outTimes.add(startCol[curr])
                outTimes.add(endCol[curr])
                outValues.add(valCol[curr])
                outValues.add(valCol[curr])
                outKeys.add(key)
                outKeys.add(key)
            }
        }

        return mapOf(
            "_time" to outTimes,
            "_val" to outValues,
            "_key" to outKeys
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun step(data: Map<String, List<*>>, valueColumn: String): Map<String, List<*>> {
        val startCol = data["_start_time"]!! as List<Double>
        val endCol = data["_end_time"]!! as List<Double>
        val valCol = data[valueColumn]!!.let { vs ->
            when (vs[0]) {
                is Double -> vs as List<Double>
                is Long -> (vs as List<Long>).map { it.toDouble() }
                is Int -> (vs as List<Int>).map { it.toDouble() }
                is Boolean -> (vs as List<Boolean>).map { if (it) 1.0 else 0.0 }
                else -> vs.map { it.toString().toDouble() }
            }
        }

        val outTimes = ArrayList<Double>()
        val outValues = ArrayList<Double>()

        for (i in startCol.indices) {
            if (i != 0 && startCol[i] != endCol[i - 1]) {
                outTimes.add((startCol[i] + endCol[i - 1]) / 2)
                outValues.add(Double.NaN)
            }
            outTimes.add(startCol[i])
            outTimes.add(endCol[i])
            outValues.add(valCol[i])
            outValues.add(valCol[i])
        }
        return mapOf(
            "_time" to outTimes,
            "_val" to outValues
        )
    }

}