package science.atlarge.grademl.query.execution

import science.atlarge.grademl.query.language.FunctionDefinition
import science.atlarge.grademl.query.language.Type
import science.atlarge.grademl.query.model.BuiltinFunctions

object BuiltinFunctionImplementations {

    private val allImplementations = listOf(COUNT, COUNT_IF, MIN, MAX, SUM, AVG, WEIGHTED_AVG)
        .associateBy { it.definition }

    fun from(definition: FunctionDefinition): FunctionImplementation {
        return allImplementations[definition] ?: throw IllegalArgumentException(
            "No implementation defined for function ${definition.functionName}"
        )
    }

    object COUNT : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.COUNT
        override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
            private var count = 0
            override fun reset() {
                count = 0
            }

            override fun addRow(arguments: Array<TypedValue>) {
                count++
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.numericValue = count.toDouble()
            }
        }
    }

    object COUNT_IF : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.COUNT_IF
        override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
            private var count = 0
            override fun reset() {
                count = 0
            }

            override fun addRow(arguments: Array<TypedValue>) {
                if (arguments[0].booleanValue) count++
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.numericValue = count.toDouble()
            }
        }
    }

    object MIN : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.MIN
        override fun newAggregator(argumentTypes: List<Type>) = when (argumentTypes[0]) {
            Type.BOOLEAN -> object : Aggregator {
                private var allAreTrue = true
                override fun reset() {
                    allAreTrue = true
                }

                override fun addRow(arguments: Array<TypedValue>) {
                    if (!arguments[0].booleanValue) allAreTrue = false
                }

                override fun writeResultTo(outValue: TypedValue) {
                    outValue.booleanValue = allAreTrue
                }
            }
            Type.NUMERIC -> object : Aggregator {
                private var minValue: Double? = null
                override fun reset() {
                    minValue = null
                }

                override fun addRow(arguments: Array<TypedValue>) {
                    minValue = if (minValue == null) arguments[0].numericValue else
                        minOf(minValue!!, arguments[0].numericValue)
                }

                override fun writeResultTo(outValue: TypedValue) {
                    outValue.numericValue = minValue!!
                }
            }
            Type.STRING -> object : Aggregator {
                private var minValue: String? = null
                override fun reset() {
                    minValue = null
                }

                override fun addRow(arguments: Array<TypedValue>) {
                    minValue = if (minValue == null) arguments[0].stringValue else
                        minOf(minValue!!, arguments[0].stringValue)
                }

                override fun writeResultTo(outValue: TypedValue) {
                    outValue.stringValue = minValue!!
                }
            }
        }
    }

    object MAX : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.MAX
        override fun newAggregator(argumentTypes: List<Type>) = when (argumentTypes[0]) {
            Type.BOOLEAN -> object : Aggregator {
                private var anyAreTrue = false
                override fun reset() {
                    anyAreTrue = false
                }

                override fun addRow(arguments: Array<TypedValue>) {
                    if (arguments[0].booleanValue) anyAreTrue = true
                }

                override fun writeResultTo(outValue: TypedValue) {
                    outValue.booleanValue = anyAreTrue
                }
            }
            Type.NUMERIC -> object : Aggregator {
                private var maxValue: Double? = null
                override fun reset() {
                    maxValue = null
                }

                override fun addRow(arguments: Array<TypedValue>) {
                    maxValue = if (maxValue == null) arguments[0].numericValue else
                        maxOf(maxValue!!, arguments[0].numericValue)
                }

                override fun writeResultTo(outValue: TypedValue) {
                    outValue.numericValue = maxValue!!
                }
            }
            Type.STRING -> object : Aggregator {
                private var maxValue: String? = null
                override fun reset() {
                    maxValue = null
                }

                override fun addRow(arguments: Array<TypedValue>) {
                    maxValue = if (maxValue == null) arguments[0].stringValue else
                        maxOf(maxValue!!, arguments[0].stringValue)
                }

                override fun writeResultTo(outValue: TypedValue) {
                    outValue.stringValue = maxValue!!
                }
            }
        }
    }

    object SUM : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.SUM
        override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
            private var sum = 0.0
            override fun reset() {
                sum = 0.0
            }

            override fun addRow(arguments: Array<TypedValue>) {
                sum += arguments[0].numericValue
            }

            override fun writeResultTo(outValue: TypedValue) {
                outValue.numericValue = sum
            }
        }
    }

    object AVG : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.AVG
        override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
            private val sum = SUM.newAggregator(argumentTypes)
            private val count = COUNT.newAggregator(argumentTypes)
            private val sumValue = TypedValue()
            private val countValue = TypedValue()
            override fun reset() {
                sum.reset()
                count.reset()
            }

            override fun addRow(arguments: Array<TypedValue>) {
                sum.addRow(arguments)
                count.addRow(arguments)
            }

            override fun writeResultTo(outValue: TypedValue) {
                sum.writeResultTo(sumValue)
                count.writeResultTo(countValue)
                outValue.numericValue = sumValue.numericValue / countValue.numericValue
            }
        }
    }

    object WEIGHTED_AVG : AggregatingFunctionImplementation {
        override val definition: FunctionDefinition = BuiltinFunctions.WEIGHTED_AVG
        override fun newAggregator(argumentTypes: List<Type>) = object : Aggregator {
            private val sumWeightedValues = SUM.newAggregator(listOf(Type.NUMERIC))
            private val sumWeights = SUM.newAggregator(listOf(Type.NUMERIC))
            private val argumentArray = arrayOf(TypedValue())
            private val firstOutValue = TypedValue()
            private val secondOutValue = TypedValue()
            override fun reset() {
                sumWeightedValues.reset()
                sumWeights.reset()
            }

            override fun addRow(arguments: Array<TypedValue>) {
                argumentArray[0].numericValue = arguments[0].numericValue * arguments[1].numericValue
                sumWeightedValues.addRow(argumentArray)
                argumentArray[0].numericValue = arguments[1].numericValue
                sumWeights.addRow(argumentArray)
            }

            override fun writeResultTo(outValue: TypedValue) {
                sumWeightedValues.writeResultTo(firstOutValue)
                sumWeights.writeResultTo(secondOutValue)
                outValue.numericValue = firstOutValue.numericValue / secondOutValue.numericValue
            }
        }
    }

}